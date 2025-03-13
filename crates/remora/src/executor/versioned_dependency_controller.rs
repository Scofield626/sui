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
    /// The initial version.
    initial_version: SequenceNumber,
}

impl Default for VersionedDependencyController {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionedDependencyController {
    pub fn new() -> Self {
        let obj_task_map: ObjectTaskMap = DashMap::new();

        Self {
            obj_task_map,
            initial_version: SequenceNumber::from(2),
        }
    }

    /// Checks if a given `(ObjectID, SequenceNumber)` has an associated task.
    pub fn has_task_for_object(&self, obj_id: &ObjectID, seq_num: SequenceNumber) -> bool {
        self.obj_task_map.contains_key(&(*obj_id, seq_num))
    }

    /// A helper function to check the existing entry in the map, else create one and fill there
    fn entry_helper(
        &self,
        obj_id: ObjectID,
        seq_num: SequenceNumber,
        task_id: TaskID,
    ) -> Arc<Notify> {
        self.obj_task_map
            .entry((obj_id, seq_num))
            .or_insert_with(|| Some((task_id, Arc::new(Notify::new()))))
            .as_ref()
            .unwrap()
            .1
            .clone()
    }

    /// Get handles for both current version and the next version
    /// the current handles are those to await upon
    /// the next handles are those to signal once completed
    pub fn get_prior_dependency_and_update(
        &self,
        task_id: TaskID,
        obj_versions: Vec<(ObjectID, SequenceNumber)>,
    ) -> (Vec<Arc<Notify>>, Vec<Arc<Notify>>) {
        let mut current_handles = Vec::new();
        let mut next_handles = Vec::new();

        for (obj_id, seq_num) in obj_versions.iter() {
            if *seq_num > self.initial_version {
                current_handles.push(self.entry_helper(*obj_id, *seq_num, task_id));
            }
            next_handles.push(self.entry_helper(*obj_id, seq_num.next(), task_id));
        }

        (current_handles, next_handles)
    }

    /// Removes dependencies for the given object versions.
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
    fn test_no_prior_dependencies() {
        let dependency_controller = VersionedDependencyController::default();
        let task_id = 1;
        let obj_versions = vec![
            (ObjectID::random(), SequenceNumber::from(1)),
            (ObjectID::random(), SequenceNumber::from(1)),
        ];

        let (prior_tasks, current_tasks) =
            dependency_controller.get_prior_dependency_and_update(task_id, obj_versions.clone());

        // **Correction:** Prior tasks should now be the same as current_tasks since they are inserted
        assert_eq!(
            prior_tasks.len(),
            obj_versions.len(),
            "Prior tasks should exist since they are created on first access."
        );

        assert_eq!(
            current_tasks.len(),
            obj_versions.len(),
            "Should create a new notify for each versioned ObjectID."
        );

        // Ensure that each `(ObjectID, SequenceNumber)` now has a corresponding entry in the map
        for (obj_id, seq_num) in obj_versions {
            assert!(
                dependency_controller.has_task_for_object(&obj_id, seq_num),
                "The dependency controller should track this object version."
            );
        }
    }

    #[test]
    fn test_with_prior_dependencies_same_object_different_versions() {
        let dependency_controller = VersionedDependencyController::default();
        let task_id1 = 1;
        let task_id2 = 2;
        let obj_id = ObjectID::random();

        // First task accesses an object with version 1
        let (prior_tasks1, current_tasks1) = dependency_controller
            .get_prior_dependency_and_update(task_id1, vec![(obj_id, SequenceNumber::from(1))]);

        assert_eq!(
            prior_tasks1.len(),
            1,
            "Prior tasks should contain the same notify since it's created."
        );

        // Second task accesses the same object but version 2
        let (prior_tasks2, current_tasks2) = dependency_controller.get_prior_dependency_and_update(
            task_id2,
            vec![(obj_id, SequenceNumber::from(2))], // Newer version
        );

        assert_eq!(
            prior_tasks2.len(),
            1,
            "Prior tasks should contain the notify from the previous task."
        );

        assert!(
            Arc::ptr_eq(&prior_tasks2[0], &current_tasks1[0]),
            "The prior notify should match the first task’s notify for version 1."
        );

        // Ensure a new notify is created for version 2
        assert!(
            !Arc::ptr_eq(&prior_tasks2[0], &current_tasks2[0]),
            "New version should have a separate notify."
        );

        // Ensure both versions exist in the map
        assert!(
            dependency_controller.has_task_for_object(&obj_id, SequenceNumber::from(1)),
            "The dependency controller should track the first version."
        );
        assert!(
            dependency_controller.has_task_for_object(&obj_id, SequenceNumber::from(2)),
            "The dependency controller should track the second version."
        );
    }

    #[test]
    fn test_partial_prior_dependencies_with_versions() {
        let dependency_controller = VersionedDependencyController::default();
        let task_id1 = 1;
        let task_id2 = 2;
        let obj_id1 = ObjectID::random();
        let obj_id2 = ObjectID::random();
        let obj_versions1 = vec![
            (obj_id1, SequenceNumber::from(1)),
            (obj_id2, SequenceNumber::from(1)),
        ];
        let obj_versions2 = vec![
            (obj_id1, SequenceNumber::from(2)),
            (ObjectID::random(), SequenceNumber::from(2)),
        ];

        let (prior_tasks1, current_tasks1) =
            dependency_controller.get_prior_dependency_and_update(task_id1, obj_versions1.clone());

        let (prior_tasks2, current_tasks2) =
            dependency_controller.get_prior_dependency_and_update(task_id2, obj_versions2.clone());

        // Only the first object should have a prior dependency
        assert_eq!(
            prior_tasks2.len(),
            obj_versions2.len(),
            "Each accessed version should have an existing prior notify."
        );

        // The first object's prior notify should be the same as the first task's notify
        assert!(
            Arc::ptr_eq(&prior_tasks2[0], &current_tasks1[0]),
            "The prior notify should match the one for the overlapping ObjectID with the same version."
        );

        // Ensure the new object version got a new notify
        assert!(
            !Arc::ptr_eq(&prior_tasks2[1], &current_tasks2[1]),
            "New version should have a separate notify."
        );
    }
}
