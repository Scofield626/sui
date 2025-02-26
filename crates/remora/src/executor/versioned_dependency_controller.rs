// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use dashmap::DashMap;
use sui_types::base_types::{ObjectID, SequenceNumber};
use tokio::sync::Notify;

pub type TaskID = u64;
/// Notify is similar to a channel but without sending any data.
pub type TaskHandle = (TaskID, Arc<Notify>);
pub type TaskEntry = Option<TaskHandle>;
pub type ObjectTaskMap = DashMap<(ObjectID, SequenceNumber), TaskEntry>;

/// The dependency controller is responsible for dynamically maintaining
/// inter-task dependency graph due to overlapped resource accesses.
pub struct VersionedDependencyController {
    /// This map contains the tail task of all priors ones
    /// which access the given object.
    obj_task_map: ObjectTaskMap,
}

impl Default for VersionedDependencyController {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionedDependencyController {
    pub fn new() -> Self {
        let obj_task_map: ObjectTaskMap = DashMap::new();

        Self { obj_task_map }
    }

    /// Checks if a given `(ObjectID, SequenceNumber)` has an associated task.
    pub fn has_task_for_object(&self, obj_id: &ObjectID, seq_num: SequenceNumber) -> bool {
        self.obj_task_map.contains_key(&(*obj_id, seq_num))
    }
     /// **Creates or updates** an entry in the map for the given task and object versions.
    /// Returns the new `Notify` handles that the task should use.
    pub fn update_dependency(
        &self,
        task_id: TaskID,
        obj_versions: Vec<(ObjectID, SequenceNumber)>,
    ) -> Vec<Arc<Notify>> {
        let current_handles: Vec<_> = (0..obj_versions.len())
            .map(|_| Arc::new(Notify::new()))
            .collect();

        for ((obj_id, seq_num), notify) in obj_versions.iter().zip(current_handles.iter()) {
            self.obj_task_map.insert((*obj_id, *seq_num), Some((task_id, notify.clone())));
        }

        current_handles
    }

    /// **Reads existing dependencies** for the given object versions.
    /// Returns a list of prior `Notify` handles.
    pub fn read_dependency(
        &self,
        obj_versions: Vec<(ObjectID, SequenceNumber)>,
    ) -> Vec<Arc<Notify>> {
        let mut prior_handles = Vec::new();

        for (obj_id, seq_num) in obj_versions {
            if let Some(entry) = self.obj_task_map.get(&((*obj_id).into(), seq_num)) {
                if let Some((_, notify)) = entry.value() {
                    prior_handles.push(notify.clone());
                }
            }
        }

        prior_handles
    }

    /// **Removes dependencies** for the given object versions.
    pub fn remove_dependency(&self, obj_versions: Vec<(ObjectID, SequenceNumber)>) {
        for (obj_id, seq_num) in obj_versions {
            self.obj_task_map.remove(&((*obj_id).into(), seq_num));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_dependency() {
        let dependency_controller = VersionedDependencyController::new();
        let task_id = 1;
        let obj_versions = vec![
            (ObjectID::random(), SequenceNumber::from(1)),
            (ObjectID::random(), SequenceNumber::from(1)),
        ];

        let current_tasks = dependency_controller.update_dependency(task_id, obj_versions.clone());

        assert_eq!(
            current_tasks.len(),
            obj_versions.len(),
            "Should create a new notify handle for each object version."
        );

        for (obj_id, seq_num) in obj_versions {
            assert!(
                dependency_controller.obj_task_map.contains_key(&(obj_id, seq_num)),
                "The dependency controller should track this object version."
            );
        }
    }

    #[test]
    fn test_read_dependency() {
        let dependency_controller = VersionedDependencyController::new();
        let task_id = 1;
        let obj_versions = vec![
            (ObjectID::random(), SequenceNumber::from(1)),
            (ObjectID::random(), SequenceNumber::from(2)),
        ];

        let _ = dependency_controller.update_dependency(task_id, obj_versions.clone());

        let prior_tasks = dependency_controller.read_dependency(obj_versions.clone());

        assert_eq!(
            prior_tasks.len(),
            obj_versions.len(),
            "Should return the correct number of notify handles."
        );
    }

    #[test]
    fn test_remove_dependency() {
        let dependency_controller = VersionedDependencyController::new();
        let task_id = 1;
        let obj_versions = vec![
            (ObjectID::random(), SequenceNumber::from(1)),
            (ObjectID::random(), SequenceNumber::from(2)),
        ];

        let _ = dependency_controller.update_dependency(task_id, obj_versions.clone());

        dependency_controller.remove_dependency(obj_versions.clone());

        for (obj_id, seq_num) in obj_versions {
            assert!(
                !dependency_controller.obj_task_map.contains_key(&(obj_id, seq_num)),
                "The object version should be removed from the task map."
            );
        }
    }
}
