//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/extendible_hash_table.h"
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  // initially only 1 bucket with depth 0 of required size
  dir_.push_back(std::make_shared<Bucket>(bucket_size_, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  while (!dir_[IndexOf(key)]->Insert(key, value)) {
    // bucket overflow happens
    auto index = IndexOf(key);
    auto bucket_ptr = dir_[index];
    auto global_extend = false;
    if (bucket_ptr->GetDepth() == global_depth_) {
      // extend the hashtable by twice, redistribute pointers to buckets
      global_depth_++;
      dir_.resize(std::pow(2, global_depth_), nullptr);
      global_extend = true;
    }

    // split the overflow bucket into 2, and remap directory pointers
    RedistributeBucket(bucket_ptr, index);

    // re-distribute directory pointers if extension happens
    if (global_extend) {
      auto curr_dir_size = dir_.size();
      auto old_dir_size = curr_dir_size >> 1;
      for (auto i = old_dir_size; i < curr_dir_size; i++) {
        if (dir_[i] == nullptr) {
          dir_[i] = dir_[i - old_dir_size];
        }
      }
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket, int64_t index) -> void {
  auto old_depth = bucket->GetDepth();
  int64_t old_gap = std::pow(2, old_depth);
  bucket->IncrementDepth();

  // this overflow bucket will be split into two
  // one with 0 prefix, the other with 1 prefix
  auto even_new_bucket = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
  auto odd_new_bucket = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
  num_buckets_++;

  // redistribute the directory pointers for this bucket's split
  for (uint64_t i = index % old_gap; i < dir_.size(); i += old_gap) {
    if (dir_[i] == nullptr || dir_[i] == bucket) {
      // fetch the new prefix bit and decide pair with even or odd split-bucket
      auto prefix_bit = (i >> old_depth) & 1;
      dir_[i] = (prefix_bit == 0) ? even_new_bucket : odd_new_bucket;
    }
  }

  for (auto it = bucket->list_.rbegin(); it != bucket->list_.rend(); it++) {
    // each will have a new hash index, re-distribute items in old bucket
    auto item_index = IndexOf(it->first);
    dir_[item_index]->Insert(it->first, it->second);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &[k, v] : list_) {
    if (key == k) {
      // find match key, set value associated for output
      value = v;
      return true;
    }
  }
  return false;  // not found
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.cbegin(); it != list_.cend(); it++) {
    if (key == it->first) {
      // find match key, remove inside iterate only once
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &[k, v] : list_) {
    if (key == k) {
      // update value if key already exists
      v = value;
      return true;
    }
  }
  if (IsFull()) {
    return false;
  }
  list_.emplace_front(key, value);  // locality
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
