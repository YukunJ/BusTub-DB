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

auto LRUKFrameRecord::IsEvictable() const -> bool { return is_evictable_; }

auto LRUKFrameRecord::SetEvictable(bool is_evictable) -> void { is_evictable_ = is_evictable; }

auto LRUKFrameRecord::Access(uint64_t time) -> void {
  // only maintain last k access time
  // by first-in-first-out queue
  while (access_records_.size() >= k_) {
    access_records_.pop();
  }
  access_records_.push(time);
}

auto LRUKFrameRecord::LastKAccessTime() const -> uint64_t { return access_records_.front(); }

auto LRUKFrameRecord::EarliestAccessTime() const -> uint64_t { return access_records_.front(); }

auto LRUKFrameRecord::GetFrameId() const -> size_t { return frame_id_; }

auto LRUKFrameRecord::AccessSize() const -> size_t { return access_records_.size(); }

// ---------------- LRUKReplacer ----------------- //
// ----------------------------------------------- //

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : k_(k) {
  // fixed size of frames, initially all set to be null frame
  frames_.resize(num_frames, nullptr);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // whenever there is evictable frame with less than k access, they get evicted first
  std::scoped_lock<std::mutex> lock(latch_);
  if (lru_mature_.empty() && lru_premature_.empty()) {
    return false;
  }
  auto has_premature = !lru_premature_.empty();
  auto first_iter = has_premature ? lru_premature_.begin() : lru_mature_.begin();
  *frame_id = (*first_iter)->GetFrameId();
  DeallocateFrameRecord(first_iter, has_premature);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frames_[frame_id] == nullptr) {
    AllocateFrameRecord(frame_id);
  }
  // careful here that, if you made change to frame
  // the set might contain duplicate, because "old version" and "new version" deemed different
  // therefore, first remove, make changes, and then add it back
  auto is_premature = frames_[frame_id]->AccessSize() < k_;
  auto is_evictable = frames_[frame_id]->IsEvictable();
  if (is_evictable && is_premature && frames_[frame_id]->AccessSize() == (k_ - 1)) {
    // about to move from premature to mature
    lru_premature_.erase(frames_[frame_id]);
  }
  if (is_evictable && (!is_premature)) {
    lru_mature_.erase(frames_[frame_id]);
  }

  frames_[frame_id]->Access(CurrTime());

  if (is_evictable && is_premature && frames_[frame_id]->AccessSize() == k_) {
    // move from premature to mature
    lru_mature_.insert(frames_[frame_id]);
  }
  if (is_evictable && (!is_premature)) {
    lru_mature_.insert(frames_[frame_id]);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frames_[frame_id] == nullptr) {
    // no page allocated for this
    return;
  }
  auto is_premature = frames_[frame_id]->AccessSize() < k_;
  if (set_evictable && !frames_[frame_id]->IsEvictable()) {
    // transit from not evictable to evictable
    replacer_size_++;
    if (is_premature) {
      lru_premature_.insert(frames_[frame_id]);
    } else {
      lru_mature_.insert(frames_[frame_id]);
    }
  }
  if (!set_evictable && frames_[frame_id]->IsEvictable()) {
    // transit from evictable to non evictable
    replacer_size_--;
    if (is_premature) {
      lru_premature_.erase(frames_[frame_id]);
    } else {
      lru_mature_.erase(frames_[frame_id]);
    }
  }
  frames_[frame_id]->SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frames_[frame_id] == nullptr) {
    // not found, directly return
    return;
  }
  DeallocateFrameRecord(frame_id, frames_[frame_id]->AccessSize() < k_);
}

auto LRUKReplacer::Size() -> size_t { return replacer_size_; }

auto LRUKReplacer::AllocateFrameRecord(size_t frame_id) -> void {
  frames_[frame_id] = new LRUKFrameRecord(frame_id, k_);
  curr_size_++;
}

auto LRUKReplacer::DeallocateFrameRecord(size_t frame_id, bool is_premature) -> void {
  if (is_premature) {
    lru_premature_.erase(frames_[frame_id]);
  } else {
    lru_mature_.erase(frames_[frame_id]);
  }
  delete frames_[frame_id];
  frames_[frame_id] = nullptr;
  curr_size_--;
  replacer_size_--;
}

auto LRUKReplacer::DeallocateFrameRecord(LRUKReplacer::container_iterator it, bool is_premature) -> void {
  frame_id_t frame_to_delete = (*it)->GetFrameId();
  if (is_premature) {
    lru_premature_.erase(it);
  } else {
    lru_mature_.erase(it);
  }
  delete frames_[frame_to_delete];
  frames_[frame_to_delete] = nullptr;
  curr_size_--;
  replacer_size_--;
}

}  // namespace bustub
