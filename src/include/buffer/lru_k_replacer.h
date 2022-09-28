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
 * Version2: Optimization attempt
 * In original version, we keep an array of frame records
 * everytime we want to search, we iterate over and try to find a victim
 * and we can update frame's status directly through the array indexing
 * which gives: search O(n) and write O(1) time complexity
 *
 * We are not sure if the system is read-heavy or write-heavy. We will try an alternative design here
 * we will maintain two separate ordered container (std::set) for record with enough k access, and not enough k access
 * This will involve a bit more engineering effort,
 * since we will need to move thing around back and forth between the two containers
 * which gives: search O(1) and write O(logn) time complexity
 */

#pragma once

#include <cstdint>
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

  /**
   * @brief Get the K parameter in LRU-K
   * @return k
   */
  auto GetK() const -> size_t;

 private:
  bool is_evictable_{false};
  size_t frame_id_;
  size_t k_;
  std::queue<uint64_t> access_records_;
};

struct FrameComp {
  auto operator()(const std::shared_ptr<LRUKFrameRecord> &lhs, const std::shared_ptr<LRUKFrameRecord> &rhs) const
      -> bool {
    // comparator for comparing 2 frame records
    size_t k = lhs->GetK();
    size_t lhs_size = lhs->AccessSize();
    size_t rhs_size = rhs->AccessSize();
    if (lhs_size < k && rhs_size < k) {
      // two premature frames
      return lhs->EarliestAccessTime() < rhs->EarliestAccessTime();
    }
    if (lhs_size == k && rhs_size < k) {
      return false;
    }
    if (lhs_size < k && rhs_size == k) {
      return true;
    }
    // two mature frames
    return lhs->LastKAccessTime() < rhs->LastKAccessTime();
  };
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
 public:
  /**
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

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
   * @brief allocate a new frame at index and increase the size
   * @param frame_id the index to be deallocated
   */
  auto DeallocateFrameRecord(size_t frame_id) -> void;

  uint64_t curr_time_{0};    // curr timestamp
  size_t curr_size_{0};      // how many frames in the replacer
  size_t replacer_size_{0};  // how many evictable frames in the replacer
  size_t k_;                 // the k as in LRU-K
  std::mutex latch_;
  std::vector<std::shared_ptr<LRUKFrameRecord>> frames_;
  // a special data structure containing all evictable frames in sorted order
  // by custom comparator
  std::set<std::shared_ptr<LRUKFrameRecord>, FrameComp> lru_set_;
};

}  // namespace bustub
