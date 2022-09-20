//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

// ------------- LRUKFrameRecord ----------------- //
// ----------------------------------------------- //
LRUKFrameRecord::LRUKFrameRecord(size_t frame_id, size_t k) : frame_id_(frame_id), k_(k) {}

auto LRUKFrameRecord::IsEvictable() -> bool { return is_evictable_; }

auto LRUKFrameRecord::SetEvictable(bool is_evictable) -> void { is_evictable_ = is_evictable; }

auto LRUKFrameRecord::Access(uint64_t time) -> void {
  // only maintain last k access time
  // by first-in-first-out queue
  while (access_records_.size() >= k_) {
    access_records_.pop();
  }
  access_records_.push(time);
}

auto LRUKFrameRecord::LastKAccessTime() -> uint64_t {
  // if not enough k records, return INFINITY
  if (access_records_.size() < k_) {
    return ACCESSTIME_INFINITY;
  }
  return access_records_.front();
}

auto LRUKFrameRecord::EarliestAccessTime() -> uint64_t { return access_records_.front(); }

auto LRUKFrameRecord::GetFrameId() -> size_t { return frame_id_; }

// ---------------- LRUKReplacer ----------------- //
// ----------------------------------------------- //

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : num_frames_(num_frames), k_(k) {
  // fixed size of frames, initially all set to be null frame
  frames_.resize(num_frames, nullptr);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // whenever there is evictable frame with less than k access, they get evicted first
  std::scoped_lock<std::mutex> lock(latch_);
  uint64_t less_than_k_access_time = ACCESSTIME_INFINITY;
  size_t less_than_k_access_frame = num_frames_;
  uint64_t earliest_k_access_time = ACCESSTIME_INFINITY;
  size_t earliest_k_access_frame = num_frames_;
  for (size_t i = 0; i < frames_.size(); i++) {
    if (frames_[i] != nullptr && frames_[i]->IsEvictable()) {
      uint64_t access_time_k = frames_[i]->LastKAccessTime();
      if (access_time_k == ACCESSTIME_INFINITY) {
        // not enough k access, check the earliest one
        uint64_t access_time_earliest = frames_[i]->EarliestAccessTime();
        if (access_time_earliest < less_than_k_access_time) {
          less_than_k_access_time = access_time_earliest;
          less_than_k_access_frame = i;
        }
      } else {
        // enough k access
        if (access_time_k < earliest_k_access_time) {
          earliest_k_access_time = access_time_k;
          earliest_k_access_frame = i;
        }
      }
    }
  }
  if (less_than_k_access_frame < num_frames_) {
    // evict a frame with less than k access first
    *frame_id = less_than_k_access_frame;
    DeallocateFrameRecord(less_than_k_access_frame);
    return true;
  }
  if (earliest_k_access_frame < num_frames_) {
    // evict the farthest k access frame
    *frame_id = earliest_k_access_frame;
    DeallocateFrameRecord(earliest_k_access_frame);
    return true;
  }
  return false;  // no evictable frame found
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frames_[frame_id] == nullptr) {
    AllocateFrameRecord(frame_id);
  }
  frames_[frame_id]->Access(CurrTime());
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frames_[frame_id] == nullptr) {
    // no page allocated for this
    return;
  }
  if (set_evictable && !frames_[frame_id]->IsEvictable()) {
    // transit from not evictable to evictable
    replacer_size_++;
  }
  if (!set_evictable && frames_[frame_id]->IsEvictable()) {
    // transit from evictable to non evictable
    replacer_size_--;
  }
  frames_[frame_id]->SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frames_[frame_id] == nullptr) {
    // not found, directly return
    return;
  }
  DeallocateFrameRecord(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return replacer_size_; }

auto LRUKReplacer::AllocateFrameRecord(size_t frame_id) -> void {
  frames_[frame_id] = std::make_shared<LRUKFrameRecord>(frame_id, k_);
  curr_size_++;
}

auto LRUKReplacer::DeallocateFrameRecord(size_t frame_id) -> void {
  frames_[frame_id] = nullptr;
  curr_size_--;
  replacer_size_--;
}

}  // namespace bustub
