// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use orx_concurrent_vec::ConcurrentVec;
use sui_types::base_types::{ObjectID, SequenceNumber};
use tokio::sync::Notify;

/// Maximum number of slots for 24-bit ObjectID indexing (2^24 = 16,777,216)
const MAX_OBJECT_SLOTS: usize = 16_777_216;

pub type TaskID = u64;
/// Notify is similar to a channel but without sending any data.
pub type TaskHandle = (TaskID, Arc<Notify>);
pub type TaskEntry = Option<TaskHandle>;

/// Version map for storing tasks per object version
type VersionMap = HashMap<SequenceNumber, TaskEntry>;

/// Concurrent object task map using 24-bit ObjectID indexing
pub struct ConcurrentObjectTaskMap {
    // Direct 24-bit indexed access to version maps
    object_slots: ConcurrentVec<Option<VersionMap>>,
}

impl Default for ConcurrentObjectTaskMap {
    fn default() -> Self {
        Self::new()
    }
}

impl ConcurrentObjectTaskMap {
    pub fn new() -> Self {
        let mut object_slots = ConcurrentVec::new();
        // Pre-allocate slots for 24-bit ObjectID indexing
        object_slots.extend((0..MAX_OBJECT_SLOTS).map(|_| None));

        Self { object_slots }
    }

    #[inline]
    fn object_id_24bit_index(object_id: &ObjectID) -> usize {
        let bytes = object_id.as_ref();
        let index = (bytes[0] as usize) | ((bytes[1] as usize) << 8) | ((bytes[2] as usize) << 16);
        index.saturating_sub(1)
    }

    /// Checks if a given `(ObjectID, SequenceNumber)` has an associated task.
    pub fn contains_key(&self, obj_id: &ObjectID, seq_num: SequenceNumber) -> bool {
        let idx = Self::object_id_24bit_index(obj_id);
        if let Some(version_map) = self.object_slots.get(idx) {
            if let Some(map) = version_map.cloned() {
                return map.contains_key(&seq_num);
            }
        }
        false
    }

    /// Get or create a task entry, similar to DashMap's entry API
    pub fn entry_helper(
        &self,
        obj_id: ObjectID,
        seq_num: SequenceNumber,
        task_id: TaskID,
    ) -> Arc<Notify> {
        let idx = Self::object_id_24bit_index(&obj_id);

        // First try to read existing entry
        if let Some(version_map) = self.object_slots.get(idx) {
            if let Some(map) = version_map.cloned() {
                if let Some(Some((_, notify))) = map.get(&seq_num) {
                    return notify.clone();
                }
            }
        }

        // Need to create new entry
        let notify = Arc::new(Notify::new());
        let notify_clone = notify.clone();

        // Use concurrent update to modify the slot
        if let Some(slot_elem) = self.object_slots.get(idx) {
            slot_elem.update(|slot| {
                let version_map = slot.get_or_insert_with(HashMap::new);
                version_map.insert(seq_num, Some((task_id, notify_clone.clone())));
            });
        }

        notify
    }

    /// Remove a dependency for the given object version.
    pub fn remove(&self, obj_id: &ObjectID, seq_num: SequenceNumber) {
        let idx = Self::object_id_24bit_index(obj_id);

        if let Some(slot_elem) = self.object_slots.get(idx) {
            slot_elem.update(|slot| {
                if let Some(version_map) = slot {
                    version_map.remove(&seq_num);
                    // Optional: clean up empty version maps
                    if version_map.is_empty() {
                        *slot = None;
                    }
                }
            });
        }
    }
}

// Legacy type alias for backward compatibility
pub type ObjectTaskMap = ConcurrentObjectTaskMap;

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
        let obj_task_map = ConcurrentObjectTaskMap::new();

        Self {
            obj_task_map,
            initial_version: SequenceNumber::from(2),
        }
    }

    /// Checks if a given `(ObjectID, SequenceNumber)` has an associated task.
    pub fn has_task_for_object(&self, obj_id: &ObjectID, seq_num: SequenceNumber) -> bool {
        self.obj_task_map.contains_key(obj_id, seq_num)
    }

    /// A helper function to check the existing entry in the map, else create one and fill there
    #[inline]
    fn entry_helper(
        &self,
        obj_id: ObjectID,
        seq_num: SequenceNumber,
        task_id: TaskID,
    ) -> Arc<Notify> {
        self.obj_task_map.entry_helper(obj_id, seq_num, task_id)
    }

    /// Compute the next version number from a list of object versions
    #[inline]
    fn calculate_next_version(obj_versions: &[(ObjectID, SequenceNumber)]) -> SequenceNumber {
        obj_versions
            .iter()
            .map(|(_, seq_num)| *seq_num)
            .max()
            .expect("No max key found, obj_versions is empty")
            .next()
    }

    /// Get handles for both current version and the next version
    /// the current handles are those to await upon
    /// the next handles are those to signal once completed
    pub fn get_prior_dependency_and_update(
        &self,
        task_id: TaskID,
        obj_versions: Vec<(ObjectID, SequenceNumber)>,
        ignore_prior: bool,
        ignore_next: bool,
    ) -> (Vec<Arc<Notify>>, Vec<Arc<Notify>>) {
        let mut current_handles = Vec::new();
        let mut next_handles = Vec::new();

        let next_v = Self::calculate_next_version(&obj_versions);

        for (obj_id, seq_num) in obj_versions.iter() {
            if *seq_num > self.initial_version && !ignore_prior {
                current_handles.push(self.entry_helper(*obj_id, *seq_num, task_id));
            }
            if !ignore_next {
                next_handles.push(self.entry_helper(*obj_id, next_v, task_id));
            }
        }

        (current_handles, next_handles)
    }

    /// Removes dependencies for the given object versions.
    #[inline]
    pub fn remove_dependency(&self, obj_versions: Vec<(ObjectID, SequenceNumber)>) {
        for (obj_id, seq_num) in obj_versions {
            self.obj_task_map.remove(&obj_id, seq_num);
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
            (ObjectID::random(), SequenceNumber::from(2)),
            (ObjectID::random(), SequenceNumber::from(2)),
        ];

        let (prior_tasks, current_tasks) = dependency_controller.get_prior_dependency_and_update(
            task_id,
            obj_versions.clone(),
            false,
            false,
        );

        // **Correction:** Prior tasks should now be the same as current_tasks since they are inserted
        assert_eq!(
            prior_tasks.len(),
            0,
            "Prior tasks should not exist since they are created on first access."
        );

        assert_eq!(
            current_tasks.len(),
            obj_versions.len(),
            "Should create a new notify for each versioned ObjectID."
        );
    }

    #[test]
    fn test_with_prior_dependencies_same_object_different_versions() {
        let dependency_controller = VersionedDependencyController::default();
        let task_id1 = 1;
        let task_id2 = 2;
        let obj_id = ObjectID::random();

        // First task accesses an object with version 2
        let (prior_tasks1, current_tasks1) = dependency_controller.get_prior_dependency_and_update(
            task_id1,
            vec![(obj_id, SequenceNumber::from(2))],
            false,
            false,
        );

        assert_eq!(
            prior_tasks1.len(),
            0,
            "Prior tasks should not exist since they are created on first access."
        );

        // Second task accesses the same object but version 3
        let (prior_tasks2, current_tasks2) = dependency_controller.get_prior_dependency_and_update(
            task_id2,
            vec![(obj_id, SequenceNumber::from(3))], // Newer version
            false,
            false,
        );

        assert_eq!(
            prior_tasks2.len(),
            1,
            "Prior tasks should contain the notify from the previous task."
        );

        assert!(
            Arc::ptr_eq(&prior_tasks2[0], &current_tasks1[0]),
            "The prior notify should match the first task's notify for version 1."
        );

        // Ensure a new notify is created for version 2
        assert!(
            !Arc::ptr_eq(&prior_tasks2[0], &current_tasks2[0]),
            "New version should have a separate notify."
        );

        assert!(
            dependency_controller.has_task_for_object(&obj_id, SequenceNumber::from(3)),
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
            (obj_id1, SequenceNumber::from(2)),
            (obj_id2, SequenceNumber::from(2)),
        ];
        let obj_versions2 = vec![
            (obj_id1, SequenceNumber::from(3)),
            (obj_id2, SequenceNumber::from(3)),
        ];

        let (_prior_tasks1, current_tasks1) = dependency_controller
            .get_prior_dependency_and_update(task_id1, obj_versions1.clone(), false, false);

        let (prior_tasks2, _current_tasks2) = dependency_controller
            .get_prior_dependency_and_update(task_id2, obj_versions2.clone(), false, false);

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
            Arc::ptr_eq(&prior_tasks2[1], &current_tasks1[1]),
            "The prior notify should match the one for the overlapping ObjectID with the same version."
        );
    }

    #[test]
    fn test_ignore_prior_flag() {
        let dependency_controller = VersionedDependencyController::default();
        let task_id = 1;
        let obj_id = ObjectID::random();
        let seq_num = SequenceNumber::from(3); // Greater than initial_version

        // First call with ignore_prior=false to set up dependencies
        dependency_controller.get_prior_dependency_and_update(
            task_id,
            vec![(obj_id, seq_num)],
            false,
            false,
        );

        // Second call with ignore_prior=true
        let (prior_tasks, next_tasks) = dependency_controller.get_prior_dependency_and_update(
            task_id + 1,
            vec![(obj_id, seq_num)],
            true, // ignore prior dependencies
            false,
        );

        assert_eq!(
            prior_tasks.len(),
            0,
            "Should have no prior tasks when ignore_prior is true"
        );

        assert_eq!(
            next_tasks.len(),
            1,
            "Should still create next tasks when ignore_prior is true"
        );
    }

    #[test]
    fn test_ignore_next_flag() {
        let dependency_controller = VersionedDependencyController::default();
        let task_id = 1;
        let obj_id = ObjectID::random();
        let seq_num = SequenceNumber::from(3);

        // Call with ignore_next=true
        let (prior_tasks, next_tasks) = dependency_controller.get_prior_dependency_and_update(
            task_id,
            vec![(obj_id, seq_num)],
            false,
            true, // ignore next dependencies
        );

        assert_eq!(
            prior_tasks.len(),
            1,
            "Should still create prior tasks when ignore_next is true"
        );

        assert_eq!(
            next_tasks.len(),
            0,
            "Should have no next tasks when ignore_next is true"
        );
    }

    #[test]
    fn test_both_flags_true() {
        let dependency_controller = VersionedDependencyController::default();
        let task_id = 1;
        let obj_id = ObjectID::random();
        let seq_num = SequenceNumber::from(3);

        // Call with both flags set to true
        let (prior_tasks, next_tasks) = dependency_controller.get_prior_dependency_and_update(
            task_id,
            vec![(obj_id, seq_num)],
            true, // ignore prior dependencies
            true, // ignore next dependencies
        );

        assert_eq!(
            prior_tasks.len(),
            0,
            "Should have no prior tasks when ignore_prior is true"
        );

        assert_eq!(
            next_tasks.len(),
            0,
            "Should have no next tasks when ignore_next is true"
        );

        // Verify the entry was still created in the map despite both flags being true
        assert!(
            !dependency_controller.has_task_for_object(&obj_id, seq_num),
            "Entry should not be created in the map"
        );
    }

    #[test]
    fn test_initial_version_behavior() {
        let dependency_controller = VersionedDependencyController::default();
        let task_id = 1;
        let obj_id = ObjectID::random();

        // Use sequence number equal to initial_version (2)
        let seq_num_initial = SequenceNumber::from(2);

        let (prior_tasks, next_tasks) = dependency_controller.get_prior_dependency_and_update(
            task_id,
            vec![(obj_id, seq_num_initial)],
            false,
            false,
        );

        assert_eq!(
            prior_tasks.len(),
            0,
            "Should have no prior tasks when sequence number equals initial_version"
        );

        assert_eq!(
            next_tasks.len(),
            1,
            "Should create next tasks regardless of sequence number"
        );

        // Now use sequence number less than initial_version
        let seq_num_less = SequenceNumber::from(1);

        let (prior_tasks, next_tasks) = dependency_controller.get_prior_dependency_and_update(
            task_id + 1,
            vec![(obj_id, seq_num_less)],
            false,
            false,
        );

        assert_eq!(
            prior_tasks.len(),
            0,
            "Should have no prior tasks when sequence number is less than initial_version"
        );

        assert_eq!(
            next_tasks.len(),
            1,
            "Should create next tasks regardless of sequence number"
        );
    }
}
