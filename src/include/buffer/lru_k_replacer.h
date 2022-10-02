//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * Version3: Optimization attempt
 * Use two separate priority for pre-mature and mature frames management
 * For frame less than k access, their priority will not change even if we access them
 * When a frame reach k access, remove from the pre-mature queue and add to mature queue
 * For frame with enough k access, they need to be removed and re-insert into queue after access update
 * And we switch to restore raw pointer instead of std::shared_ptr for performance enhancement
 */

#pragma once

#include <cstdint>
#include <iostream>
#include <limits>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <queue>
#include <set>
#include <unordered_map>
#include <vector>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/** since time is in uint64_t, use uint64_max to represent infinity */
#define ACCESSTIME_INFINITY UINT64_MAX

/**
 * LRUKFrameRecord implements a frame's access statistics in the LRUKReplacer
 */
class LRUKFrameRecord {
 public:
  /**
   * @brief a new LRUKFrameRecord
   * @param frame_id the frame id it corresponds to
   * @param k the last k access time to be kept
   */
  explicit LRUKFrameRecord(size_t frame_id, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKFrameRecord);

  /**
   * @brief check if this frame is allowed to be evicted
   * @return true if it's OK to evict, or false if currently pinned
   */
  auto IsEvictable() const -> bool;

  /**
   * @brief Set the bool flag for this frame if it's evictable or not
   * @param is_evictable flag
   */
  auto SetEvictable(bool is_evictable) -> void;

  /**
   * @brief Access this page and update the queue of k access time history
   * @param time the access time
   */
  auto Access(uint64_t time) -> void;

  /**
   * @brief Get the k-th last access time, or Infinity if not enough access time so far
   * @return k-th last access time or infinity
   */
  auto LastKAccessTime() const -> uint64_t;

  /**
   * @brief Get the EarliestAccessTime access time, regardless of whether there is enough k access time
   * @precondition this record must not have been accessed more than k times
   * @return the earliest timestamp
   */
  auto EarliestAccessTime() const -> uint64_t;

  /**
   * @brief Get the frame id for this frame record
   * @return the id
   */
  auto GetFrameId() const -> size_t;

  /**
   * @brief Get the size of access record queue
   * @return size of access record queue
   */
  auto AccessSize() const -> size_t;

 private:
  bool is_evictable_{false};
  size_t frame_id_;
  size_t k_;
  std::queue<uint64_t> access_records_;
};

struct PrematureFrameComp {
  auto operator()(const LRUKFrameRecord *lhs, const LRUKFrameRecord *rhs) const -> bool {
    // compare two premature frame, only depends on their first entry time
    return lhs->EarliestAccessTime() < rhs->EarliestAccessTime();
  }
};

struct MatureFrameComp {
  auto operator()(const LRUKFrameRecord *lhs, const LRUKFrameRecord *rhs) const -> bool {
    // compare two mature frame, only depends on their last k access timestamp
    return lhs->LastKAccessTime() < rhs->LastKAccessTime();
  }
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
  using container_iterator = std::set<LRUKFrameRecord *>::iterator;

 public:
  /**
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * @brief Destroys the LRUReplacer. Free all the frames dynamically allocated on heap
   */
  ~LRUKReplacer() {
    for (auto &frame : frames_) {
      delete frame;
    }
  }

  /**
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

  /**
   * @brief Return the current timestamp
   * @precondition need to be protected by mutex
   * @return current timestamp
   */
  auto CurrTime() -> uint64_t { return curr_time_++; }

 private:
  /**
   * @brief allocate a new frame at index and increase the size
   * @param frame_id the index to be allocated
   */
  auto AllocateFrameRecord(size_t frame_id) -> void;

  /**
   * @brief remove a frame by id and reduce the size
   * @param frame_id the index to be deallocated
   * @param is_premature if this iterator belongs to premature set or mature set
   */
  auto DeallocateFrameRecord(size_t frame_id, bool is_premature) -> void;

  /**
   * @brief remove a frame by iterator and reduce the size
   * @param frame_id the index to be deallocated
   * @param is_premature if this iterator belongs to premature set or mature set
   */
  auto DeallocateFrameRecord(container_iterator it, bool is_premature) -> void;

  uint64_t curr_time_{0};    // curr timestamp
  size_t curr_size_{0};      // how many frames in the replacer
  size_t replacer_size_{0};  // how many evictable frames in the replacer
  size_t k_;                 // the k as in LRU-K
  std::mutex latch_;
  std::vector<LRUKFrameRecord *> frames_;
  // two special data structures containing all evictable frames in sorted order
  // by custom comparator
  std::set<LRUKFrameRecord *, PrematureFrameComp> lru_premature_;
  std::set<LRUKFrameRecord *, MatureFrameComp> lru_mature_;
};

}  // namespace bustub
