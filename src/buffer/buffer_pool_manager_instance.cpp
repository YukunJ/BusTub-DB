//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  free_list_.resize(pool_size);
  std::iota(free_list_.begin(), free_list_.end(), 0);
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // first check if there is any available frame
  // don't call allocate before we are sure there is availability
  // otherwise the page id is incremented and messed up
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t available_frame_id = -1;
  if (!FindVictim(&available_frame_id)) {
    // no evictable frame found
    return nullptr;
  }
  // at this point, we know that there is indeed an available frame id to be populated
  // we can call allocate() to get a page_id
  page_id_t new_page_id = AllocatePage();
  // reset memory and meta data for new page
  pages_[available_frame_id].ResetMemory();
  pages_[available_frame_id].page_id_ = new_page_id;
  pages_[available_frame_id].pin_count_ = 1;  // has the first pin access right now
  pages_[available_frame_id].is_dirty_ = false;
  // let replacer track this new page
  replacer_->RecordAccess(available_frame_id);
  replacer_->SetEvictable(available_frame_id, false);
  // output the new page's id to outside
  *page_id = new_page_id;
  //  insert such a mapping page_id -> frame_id in the page table
  page_table_->Insert(new_page_id, available_frame_id);
  return &pages_[available_frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  // first search the page_id in buffer pool
  frame_id_t existing_frame_id = -1;
  if (page_table_->Find(page_id, existing_frame_id)) {
    // this page exists in buffer pool
    replacer_->RecordAccess(existing_frame_id);
    replacer_->SetEvictable(existing_frame_id, false);
    pages_[existing_frame_id].pin_count_++;  // one more thread accessing this page
    return &pages_[existing_frame_id];
  }
  // not currently in buffer pool, need to try evict a frame out
  frame_id_t available_frame_id = -1;
  if (!FindVictim(&available_frame_id)) {
    // no evictable frame found
    return nullptr;
  }
  // reset the memory and meta data for this new frame
  pages_[available_frame_id].ResetMemory();
  pages_[available_frame_id].page_id_ = page_id;
  pages_[available_frame_id].pin_count_ = 1;  // has the first pin access right now
  pages_[available_frame_id].is_dirty_ = false;
  // fetch the required page's actual data from disk
  disk_manager_->ReadPage(page_id, pages_[available_frame_id].GetData());
  //  let replacer track this new page
  replacer_->RecordAccess(available_frame_id);
  replacer_->SetEvictable(available_frame_id, false);
  // insert such a mapping page_id -> frame_id in the page table
  page_table_->Insert(page_id, available_frame_id);
  return &pages_[available_frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    // page id is not present in the buffer pool
    return false;
  }
  if (pages_[frame_id].GetPinCount() == 0) {
    // pin count already 0
    return false;
  }
  if (--pages_[frame_id].pin_count_ == 0) {
    // pin count reaches 0, become evictable
    replacer_->SetEvictable(frame_id, true);
  }
  // don't overwrite other threads' is_dirty bit set on this page
  // only turn on the flag, never turn it off here
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    // page id is not present in the buffer pool
    return false;
  }
  // flush regardless of dirty or not
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;  // reset dirty flag regardless

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      // there is a valid physical page resides, regardless of dirty or not
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    // page id is not present in the buffer pool
    return true;
  }
  if (pages_[frame_id].pin_count_ != 0) {
    // currently pinned and cannot be deleted
    return false;
  }
  // if dirty, output to disk
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;  // reset dirty flag regardless
  }
  // delete from page table
  page_table_->Remove(page_id);
  // stop track in the replacer
  replacer_->Remove(frame_id);
  // add this frame back to freelist
  free_list_.push_back(frame_id);
  // reset metadata
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  // imitate free the page on disk
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::FindVictim(frame_id_t *available_frame_id) -> bool {
  if (!free_list_.empty()) {
    // pick the first free frame
    *available_frame_id = free_list_.back();
    free_list_.pop_back();
    return true;
  }
  // try evict a page
  if (replacer_->Evict(available_frame_id)) {
    // do find a frame that's evictable, check if it needs flush out
    if (pages_[*available_frame_id].IsDirty()) {
      // the page to be evicted is dirty, flush out
      // don't call FlushPgImp, otherwise lead to deadlock
      disk_manager_->WritePage(pages_[*available_frame_id].page_id_, pages_[*available_frame_id].GetData());
      pages_[*available_frame_id].is_dirty_ = false;  // reset dirty flag regardless
    }
    // remove the mapping from page table
    page_table_->Remove(pages_[*available_frame_id].page_id_);
    return true;
  }
  // no victim found
  return false;
}

}  // namespace bustub
