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

    /// Get prior dependencies, generate new handles, and update the map.
    pub fn get_dependencies(
        &self,
        task_id: TaskID,
        obj_versions: Vec<(ObjectID, SequenceNumber)>,
    ) -> (Vec<Arc<Notify>>, Vec<Arc<Notify>>) {
        let mut prior_handles = Vec::new();

        let current_handles: Vec<_> = (0..obj_versions.len())
            .map(|_| Arc::new(Notify::new()))
            .collect();

        for ((obj_id, seq_num), notify) in obj_versions.iter().zip(current_handles.iter()) {
            if let Some(mut entry) = self.obj_task_map.get_mut(&(*obj_id, *seq_num)) {
                if let Some((_, existing_notify)) = entry.take() {
                    prior_handles.push(existing_notify);
                }

                *entry = Some((task_id, notify.clone()));
            } else {
                self.obj_task_map
                    .insert((*obj_id, *seq_num), Some((task_id, notify.clone())));
            }
        }

        (prior_handles, current_handles)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_prior_dependencies() {
        let dependency_controller = VersionedDependencyController::new();
        let task_id = 1;
        let obj_versions = vec![
            (ObjectID::random(), SequenceNumber::from(1)),
            (ObjectID::random(), SequenceNumber::from(1)),
        ];

        let (prior_tasks, current_tasks) =
            dependency_controller.get_dependencies(task_id, obj_versions.clone());

        assert!(
            prior_tasks.is_empty(),
            "There should be no prior dependencies."
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
        let dependency_controller = VersionedDependencyController::new();
        let task_id1 = 1;
        let task_id2 = 2;
        let obj_id = ObjectID::random();

        // First task accesses an object with version 1
        let (prior_tasks, current_tasks1) = dependency_controller
            .get_dependencies(task_id1, vec![(obj_id, SequenceNumber::from(1))]);
        assert!(
            prior_tasks.is_empty(),
            "There should be no prior dependencies for the first task."
        );

        // Second task accesses the same object but version 2
        let (prior_tasks, _current_tasks2) = dependency_controller.get_dependencies(
            task_id2,
            vec![(obj_id, SequenceNumber::from(2))], // Newer version
        );
        assert!(
            prior_tasks.is_empty(),
            "A newer object version should not depend on previous ones."
        );

        assert!(
            prior_tasks
                .iter()
                .zip(current_tasks1.iter())
                .all(|(a, b)| !Arc::ptr_eq(a, b)),
            "All prior notifies should be different from the current notifies."
        );
    }

    #[test]
    fn test_partial_prior_dependencies_with_versions() {
        let dependency_controller = VersionedDependencyController::new();
        let task_id1 = 1;
        let task_id2 = 2;
        let obj_id = ObjectID::random();
        let obj_versions1 = vec![
            (obj_id, SequenceNumber::from(1)),
            (ObjectID::random(), SequenceNumber::from(1)),
        ];
        let obj_versions2 = vec![
            (obj_id, SequenceNumber::from(1)),
            (ObjectID::random(), SequenceNumber::from(2)),
        ];

        let (prior_tasks1, current_tasks1) =
            dependency_controller.get_dependencies(task_id1, obj_versions1.clone());

        assert!(
            prior_tasks1.is_empty(),
            "There should be no prior dependencies for the first task."
        );

        let (prior_tasks2, _current_tasks2) =
            dependency_controller.get_dependencies(task_id2, obj_versions2.clone());

        assert_eq!(
            prior_tasks2.len(),
            1,
            "There should be one prior dependency for the overlapping ObjectID with the same version."
        );

        assert!(
            Arc::ptr_eq(&prior_tasks2[0], &current_tasks1[0]),
            "The prior notify should match the one for the overlapping ObjectID with the same version."
        );
    }
}
