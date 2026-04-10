#![allow(clippy::must_use_candidate)]
//! In-memory registries for Team and Cron lifecycle management.
//!
//! Provides TeamCreate/Delete/Get/List and CronCreate/Delete/List runtime backing
//! to replace the stub implementations in the tools crate.

use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Maximum allowed length for a team name.
const MAX_TEAM_NAME_LEN: usize = 256;

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Team {
    pub team_id: String,
    pub name: String,
    pub task_ids: Vec<String>,
    pub status: TeamStatus,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamStatus {
    Created,
    Running,
    Completed,
    Deleted,
}

impl std::fmt::Display for TeamStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "created"),
            Self::Running => write!(f, "running"),
            Self::Completed => write!(f, "completed"),
            Self::Deleted => write!(f, "deleted"),
        }
    }
}

/// Typed error for team registry operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeamError {
    NotFound(String),
    AlreadyDeleted(String),
    EmptyName,
    NameTooLong(usize),
    EmptyTasks,
}

impl Display for TeamError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(id) => write!(f, "team not found: {id}"),
            Self::AlreadyDeleted(id) => write!(f, "team {id} is already deleted"),
            Self::EmptyName => write!(f, "team name must not be empty"),
            Self::NameTooLong(len) => {
                write!(
                    f,
                    "team name exceeds maximum length of {MAX_TEAM_NAME_LEN} (got {len})"
                )
            }
            Self::EmptyTasks => write!(f, "team must have at least one task"),
        }
    }
}

impl std::error::Error for TeamError {}

#[derive(Debug, Clone, Default)]
pub struct TeamRegistry {
    inner: Arc<Mutex<TeamInner>>,
}

#[derive(Debug, Default)]
struct TeamInner {
    teams: HashMap<String, Team>,
    counter: u64,
}

impl TeamRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create(&self, name: &str, task_ids: Vec<String>) -> Result<Team, TeamError> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err(TeamError::EmptyName);
        }
        if trimmed.len() > MAX_TEAM_NAME_LEN {
            return Err(TeamError::NameTooLong(trimmed.len()));
        }
        if task_ids.is_empty() {
            return Err(TeamError::EmptyTasks);
        }

        let mut inner = self.inner.lock().expect("team registry lock poisoned");
        inner.counter += 1;
        let ts = now_secs();
        let team_id = format!("team_{:08x}_{}", ts, inner.counter);
        let team = Team {
            team_id: team_id.clone(),
            name: trimmed.to_owned(),
            task_ids,
            status: TeamStatus::Created,
            created_at: ts,
            updated_at: ts,
        };
        inner.teams.insert(team_id, team.clone());
        Ok(team)
    }

    pub fn get(&self, team_id: &str) -> Option<Team> {
        let inner = self.inner.lock().expect("team registry lock poisoned");
        inner.teams.get(team_id).cloned()
    }

    pub fn list(&self) -> Vec<Team> {
        let inner = self.inner.lock().expect("team registry lock poisoned");
        inner.teams.values().cloned().collect()
    }

    pub fn delete(&self, team_id: &str) -> Result<Team, TeamError> {
        let mut inner = self.inner.lock().expect("team registry lock poisoned");
        let team = inner
            .teams
            .get_mut(team_id)
            .ok_or_else(|| TeamError::NotFound(team_id.to_owned()))?;
        if team.status == TeamStatus::Deleted {
            return Err(TeamError::AlreadyDeleted(team_id.to_owned()));
        }
        team.status = TeamStatus::Deleted;
        team.updated_at = now_secs();
        Ok(team.clone())
    }

    /// Transition team to a new status.
    pub fn set_status(&self, team_id: &str, status: TeamStatus) -> Result<(), TeamError> {
        let mut inner = self.inner.lock().expect("team registry lock poisoned");
        let team = inner
            .teams
            .get_mut(team_id)
            .ok_or_else(|| TeamError::NotFound(team_id.to_owned()))?;
        team.status = status;
        team.updated_at = now_secs();
        Ok(())
    }

    /// Remove a task_id from a team's task list (e.g. when the task is removed).
    pub fn remove_task_id(&self, team_id: &str, task_id: &str) -> Result<(), TeamError> {
        let mut inner = self.inner.lock().expect("team registry lock poisoned");
        let team = inner
            .teams
            .get_mut(team_id)
            .ok_or_else(|| TeamError::NotFound(team_id.to_owned()))?;
        team.task_ids.retain(|id| id != task_id);
        team.updated_at = now_secs();
        Ok(())
    }

    pub fn remove(&self, team_id: &str) -> Option<Team> {
        let mut inner = self.inner.lock().expect("team registry lock poisoned");
        inner.teams.remove(team_id)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        let inner = self.inner.lock().expect("team registry lock poisoned");
        inner.teams.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronEntry {
    pub cron_id: String,
    pub schedule: String,
    pub prompt: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub created_at: u64,
    pub updated_at: u64,
    pub last_run_at: Option<u64>,
    pub run_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct CronRegistry {
    inner: Arc<Mutex<CronInner>>,
}

#[derive(Debug, Default)]
struct CronInner {
    entries: HashMap<String, CronEntry>,
    counter: u64,
}

impl CronRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create(&self, schedule: &str, prompt: &str, description: Option<&str>) -> CronEntry {
        let mut inner = self.inner.lock().expect("cron registry lock poisoned");
        inner.counter += 1;
        let ts = now_secs();
        let cron_id = format!("cron_{:08x}_{}", ts, inner.counter);
        let entry = CronEntry {
            cron_id: cron_id.clone(),
            schedule: schedule.to_owned(),
            prompt: prompt.to_owned(),
            description: description.map(str::to_owned),
            enabled: true,
            created_at: ts,
            updated_at: ts,
            last_run_at: None,
            run_count: 0,
        };
        inner.entries.insert(cron_id, entry.clone());
        entry
    }

    pub fn get(&self, cron_id: &str) -> Option<CronEntry> {
        let inner = self.inner.lock().expect("cron registry lock poisoned");
        inner.entries.get(cron_id).cloned()
    }

    pub fn list(&self, enabled_only: bool) -> Vec<CronEntry> {
        let inner = self.inner.lock().expect("cron registry lock poisoned");
        inner
            .entries
            .values()
            .filter(|e| !enabled_only || e.enabled)
            .cloned()
            .collect()
    }

    pub fn delete(&self, cron_id: &str) -> Result<CronEntry, String> {
        let mut inner = self.inner.lock().expect("cron registry lock poisoned");
        inner
            .entries
            .remove(cron_id)
            .ok_or_else(|| format!("cron not found: {cron_id}"))
    }

    /// Disable a cron entry without removing it.
    pub fn disable(&self, cron_id: &str) -> Result<(), String> {
        let mut inner = self.inner.lock().expect("cron registry lock poisoned");
        let entry = inner
            .entries
            .get_mut(cron_id)
            .ok_or_else(|| format!("cron not found: {cron_id}"))?;
        entry.enabled = false;
        entry.updated_at = now_secs();
        Ok(())
    }

    /// Record a cron run.
    pub fn record_run(&self, cron_id: &str) -> Result<(), String> {
        let mut inner = self.inner.lock().expect("cron registry lock poisoned");
        let entry = inner
            .entries
            .get_mut(cron_id)
            .ok_or_else(|| format!("cron not found: {cron_id}"))?;
        entry.last_run_at = Some(now_secs());
        entry.run_count += 1;
        entry.updated_at = now_secs();
        Ok(())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        let inner = self.inner.lock().expect("cron registry lock poisoned");
        inner.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Team tests ──────────────────────────────────────

    #[test]
    fn creates_and_retrieves_team() {
        let registry = TeamRegistry::new();
        let team = registry
            .create("Alpha Squad", vec!["task_001".into(), "task_002".into()])
            .expect("create should succeed");
        assert_eq!(team.name, "Alpha Squad");
        assert_eq!(team.task_ids.len(), 2);
        assert_eq!(team.status, TeamStatus::Created);

        let fetched = registry.get(&team.team_id).expect("team should exist");
        assert_eq!(fetched.team_id, team.team_id);
    }

    #[test]
    fn lists_and_deletes_teams() {
        let registry = TeamRegistry::new();
        let t1 = registry
            .create("Team A", vec!["t1".into()])
            .expect("create should succeed");
        let t2 = registry
            .create("Team B", vec!["t2".into()])
            .expect("create should succeed");

        let all = registry.list();
        assert_eq!(all.len(), 2);

        let deleted = registry.delete(&t1.team_id).expect("delete should succeed");
        assert_eq!(deleted.status, TeamStatus::Deleted);

        // Team is still listable (soft delete)
        let still_there = registry.get(&t1.team_id).unwrap();
        assert_eq!(still_there.status, TeamStatus::Deleted);

        // Hard remove
        registry.remove(&t2.team_id);
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn rejects_missing_team_operations() {
        let registry = TeamRegistry::new();
        assert!(registry.delete("nonexistent").is_err());
        assert!(registry.get("nonexistent").is_none());
    }

    #[test]
    fn rejects_empty_name() {
        let registry = TeamRegistry::new();
        let err = registry
            .create("", vec!["t1".into()])
            .expect_err("empty name should be rejected");
        assert_eq!(err, TeamError::EmptyName);
    }

    #[test]
    fn rejects_whitespace_only_name() {
        let registry = TeamRegistry::new();
        let err = registry
            .create("   ", vec!["t1".into()])
            .expect_err("whitespace name should be rejected");
        assert_eq!(err, TeamError::EmptyName);
    }

    #[test]
    fn rejects_name_too_long() {
        let registry = TeamRegistry::new();
        let long_name = "x".repeat(300);
        let err = registry
            .create(&long_name, vec!["t1".into()])
            .expect_err("long name should be rejected");
        assert_eq!(err, TeamError::NameTooLong(300));
    }

    #[test]
    fn rejects_empty_tasks() {
        let registry = TeamRegistry::new();
        let err = registry
            .create("Valid Name", vec![])
            .expect_err("empty tasks should be rejected");
        assert_eq!(err, TeamError::EmptyTasks);
    }

    #[test]
    fn rejects_double_delete() {
        let registry = TeamRegistry::new();
        let team = registry
            .create("Zombie", vec!["t1".into()])
            .expect("create should succeed");
        registry
            .delete(&team.team_id)
            .expect("first delete should succeed");
        let err = registry
            .delete(&team.team_id)
            .expect_err("second delete should be rejected");
        assert_eq!(err, TeamError::AlreadyDeleted(team.team_id));
    }

    #[test]
    fn set_status_transitions_team() {
        let registry = TeamRegistry::new();
        let team = registry
            .create("Movers", vec!["t1".into()])
            .expect("create should succeed");
        registry
            .set_status(&team.team_id, TeamStatus::Running)
            .expect("set_status should succeed");
        let fetched = registry.get(&team.team_id).unwrap();
        assert_eq!(fetched.status, TeamStatus::Running);
    }

    #[test]
    fn set_status_rejects_missing_team() {
        let registry = TeamRegistry::new();
        let err = registry
            .set_status("ghost", TeamStatus::Running)
            .expect_err("missing team should be rejected");
        assert_eq!(err, TeamError::NotFound("ghost".to_string()));
    }

    #[test]
    fn remove_task_id_from_team() {
        let registry = TeamRegistry::new();
        let team = registry
            .create("Trim", vec!["t1".into(), "t2".into(), "t3".into()])
            .expect("create should succeed");

        registry
            .remove_task_id(&team.team_id, "t2")
            .expect("remove_task_id should succeed");

        let fetched = registry.get(&team.team_id).unwrap();
        assert_eq!(fetched.task_ids, vec!["t1", "t3"]);
    }

    #[test]
    fn remove_task_id_rejects_missing_team() {
        let registry = TeamRegistry::new();
        let err = registry
            .remove_task_id("ghost", "t1")
            .expect_err("missing team should be rejected");
        assert_eq!(err, TeamError::NotFound("ghost".to_string()));
    }

    #[test]
    fn trims_whitespace_from_name() {
        let registry = TeamRegistry::new();
        let team = registry
            .create("  Padded  ", vec!["t1".into()])
            .expect("create should succeed");
        assert_eq!(team.name, "Padded");
    }

    // ── Cron tests ──────────────────────────────────────

    #[test]
    fn creates_and_retrieves_cron() {
        let registry = CronRegistry::new();
        let entry = registry.create("0 * * * *", "Check status", Some("hourly check"));
        assert_eq!(entry.schedule, "0 * * * *");
        assert_eq!(entry.prompt, "Check status");
        assert!(entry.enabled);
        assert_eq!(entry.run_count, 0);
        assert!(entry.last_run_at.is_none());

        let fetched = registry.get(&entry.cron_id).expect("cron should exist");
        assert_eq!(fetched.cron_id, entry.cron_id);
    }

    #[test]
    fn lists_with_enabled_filter() {
        let registry = CronRegistry::new();
        let c1 = registry.create("* * * * *", "Task 1", None);
        let c2 = registry.create("0 * * * *", "Task 2", None);
        registry
            .disable(&c1.cron_id)
            .expect("disable should succeed");

        let all = registry.list(false);
        assert_eq!(all.len(), 2);

        let enabled_only = registry.list(true);
        assert_eq!(enabled_only.len(), 1);
        assert_eq!(enabled_only[0].cron_id, c2.cron_id);
    }

    #[test]
    fn deletes_cron_entry() {
        let registry = CronRegistry::new();
        let entry = registry.create("* * * * *", "To delete", None);
        let deleted = registry
            .delete(&entry.cron_id)
            .expect("delete should succeed");
        assert_eq!(deleted.cron_id, entry.cron_id);
        assert!(registry.get(&entry.cron_id).is_none());
        assert!(registry.is_empty());
    }

    #[test]
    fn records_cron_runs() {
        let registry = CronRegistry::new();
        let entry = registry.create("*/5 * * * *", "Recurring", None);
        registry.record_run(&entry.cron_id).unwrap();
        registry.record_run(&entry.cron_id).unwrap();

        let fetched = registry.get(&entry.cron_id).unwrap();
        assert_eq!(fetched.run_count, 2);
        assert!(fetched.last_run_at.is_some());
    }

    #[test]
    fn rejects_missing_cron_operations() {
        let registry = CronRegistry::new();
        assert!(registry.delete("nonexistent").is_err());
        assert!(registry.disable("nonexistent").is_err());
        assert!(registry.record_run("nonexistent").is_err());
        assert!(registry.get("nonexistent").is_none());
    }

    #[test]
    fn team_status_display_all_variants() {
        // given
        let cases = [
            (TeamStatus::Created, "created"),
            (TeamStatus::Running, "running"),
            (TeamStatus::Completed, "completed"),
            (TeamStatus::Deleted, "deleted"),
        ];

        // when
        let rendered: Vec<_> = cases
            .into_iter()
            .map(|(status, expected)| (status.to_string(), expected))
            .collect();

        // then
        assert_eq!(
            rendered,
            vec![
                ("created".to_string(), "created"),
                ("running".to_string(), "running"),
                ("completed".to_string(), "completed"),
                ("deleted".to_string(), "deleted"),
            ]
        );
    }

    #[test]
    fn new_team_registry_is_empty() {
        // given
        let registry = TeamRegistry::new();

        // when
        let teams = registry.list();

        // then
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(teams.is_empty());
    }

    #[test]
    fn team_remove_nonexistent_returns_none() {
        // given
        let registry = TeamRegistry::new();

        // when
        let removed = registry.remove("missing");

        // then
        assert!(removed.is_none());
    }

    #[test]
    fn team_len_transitions() {
        // given
        let registry = TeamRegistry::new();

        // when
        let alpha = registry
            .create("Alpha", vec!["t1".into()])
            .expect("create should succeed");
        let beta = registry
            .create("Beta", vec!["t2".into()])
            .expect("create should succeed");
        let after_create = registry.len();
        registry.remove(&alpha.team_id);
        let after_first_remove = registry.len();
        registry.remove(&beta.team_id);

        // then
        assert_eq!(after_create, 2);
        assert_eq!(after_first_remove, 1);
        assert_eq!(registry.len(), 0);
        assert!(registry.is_empty());
    }

    #[test]
    fn cron_list_all_disabled_returns_empty_for_enabled_only() {
        // given
        let registry = CronRegistry::new();
        let first = registry.create("* * * * *", "Task 1", None);
        let second = registry.create("0 * * * *", "Task 2", None);
        registry
            .disable(&first.cron_id)
            .expect("disable should succeed");
        registry
            .disable(&second.cron_id)
            .expect("disable should succeed");

        // when
        let enabled_only = registry.list(true);
        let all_entries = registry.list(false);

        // then
        assert!(enabled_only.is_empty());
        assert_eq!(all_entries.len(), 2);
    }

    #[test]
    fn cron_create_without_description() {
        // given
        let registry = CronRegistry::new();

        // when
        let entry = registry.create("*/15 * * * *", "Check health", None);

        // then
        assert!(entry.cron_id.starts_with("cron_"));
        assert_eq!(entry.description, None);
        assert!(entry.enabled);
        assert_eq!(entry.run_count, 0);
        assert_eq!(entry.last_run_at, None);
    }

    #[test]
    fn new_cron_registry_is_empty() {
        // given
        let registry = CronRegistry::new();

        // when
        let enabled_only = registry.list(true);
        let all_entries = registry.list(false);

        // then
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(enabled_only.is_empty());
        assert!(all_entries.is_empty());
    }

    #[test]
    fn cron_record_run_updates_timestamp_and_counter() {
        // given
        let registry = CronRegistry::new();
        let entry = registry.create("*/5 * * * *", "Recurring", None);

        // when
        registry
            .record_run(&entry.cron_id)
            .expect("first run should succeed");
        registry
            .record_run(&entry.cron_id)
            .expect("second run should succeed");
        let fetched = registry.get(&entry.cron_id).expect("entry should exist");

        // then
        assert_eq!(fetched.run_count, 2);
        assert!(fetched.last_run_at.is_some());
        assert!(fetched.updated_at >= entry.updated_at);
    }

    #[test]
    fn cron_disable_updates_timestamp() {
        // given
        let registry = CronRegistry::new();
        let entry = registry.create("0 0 * * *", "Nightly", None);

        // when
        registry
            .disable(&entry.cron_id)
            .expect("disable should succeed");
        let fetched = registry.get(&entry.cron_id).expect("entry should exist");

        // then
        assert!(!fetched.enabled);
        assert!(fetched.updated_at >= entry.updated_at);
    }

    // ── Stress / long-running stability tests ─────────

    #[test]
    fn stress_create_many_teams() {
        let registry = TeamRegistry::new();
        let count = 10_000;
        let mut ids = Vec::with_capacity(count);
        for i in 0..count {
            let task_id = format!("t_{i}");
            let team = registry
                .create(&format!("Team {i}"), vec![task_id])
                .expect("create should succeed");
            ids.push(team.team_id);
        }
        assert_eq!(registry.len(), count);
        // Verify every team is retrievable.
        for id in &ids {
            assert!(registry.get(id).is_some(), "team {id} should exist");
        }
    }

    #[test]
    fn stress_create_delete_cycle_repeated() {
        let registry = TeamRegistry::new();
        // Simulate 5000 create-then-delete cycles (10k total mutations).
        for cycle in 0..5_000 {
            let team = registry
                .create(&format!("Cycle {cycle}"), vec![format!("t_{cycle}")])
                .expect("create should succeed");
            registry
                .delete(&team.team_id)
                .expect("delete should succeed");
            registry.remove(&team.team_id);
        }
        assert!(registry.is_empty());
    }

    #[test]
    fn stress_rapid_status_transitions() {
        let registry = TeamRegistry::new();
        let team = registry
            .create("Flipper", vec!["t1".into()])
            .expect("create should succeed");
        let id = team.team_id.clone();

        // Cycle through statuses many times.
        for _ in 0..10_000 {
            registry
                .set_status(&id, TeamStatus::Running)
                .expect("→ Running");
            registry
                .set_status(&id, TeamStatus::Completed)
                .expect("→ Completed");
            registry
                .set_status(&id, TeamStatus::Created)
                .expect("→ Created");
        }
        let fetched = registry.get(&id).expect("should exist");
        assert_eq!(fetched.status, TeamStatus::Created);
    }

    #[test]
    fn stress_remove_task_ids_one_by_one() {
        let registry = TeamRegistry::new();
        let n = 1_000;
        let task_ids: Vec<String> = (0..n).map(|i| format!("t_{i}")).collect();
        let team = registry
            .create("Big Team", task_ids)
            .expect("create should succeed");
        let id = team.team_id.clone();

        // Remove every task_id one by one.
        for i in 0..n {
            registry
                .remove_task_id(&id, &format!("t_{i}"))
                .expect("remove_task_id should succeed");
        }
        let fetched = registry.get(&id).expect("should exist");
        assert!(fetched.task_ids.is_empty());
    }

    #[test]
    fn stress_large_team_with_many_tasks() {
        let registry = TeamRegistry::new();
        let n = 5_000;
        let task_ids: Vec<String> = (0..n).map(|i| format!("task_{i}")).collect();
        let team = registry
            .create("Mega Team", task_ids.clone())
            .expect("create should succeed");
        assert_eq!(team.task_ids.len(), n);

        let fetched = registry.get(&team.team_id).unwrap();
        assert_eq!(fetched.task_ids.len(), n);
    }

    #[test]
    fn stress_team_error_display_does_not_panic() {
        // Ensure all error variants render without panicking.
        let errors = vec![
            TeamError::NotFound("ghost".into()),
            TeamError::AlreadyDeleted("zombie".into()),
            TeamError::EmptyName,
            TeamError::NameTooLong(999),
            TeamError::EmptyTasks,
        ];
        for err in &errors {
            let _msg = err.to_string();
        }
    }
}
