//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_insert_test.cpp
//
// Identification: test/storage/b_plus_tree_insert_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

/*
 * A few cases to be tested:
 * 1. normal begin() to end()
 * 2. begin(key) with exact match key
 * 3. begin(key) with a bigger key within that page
 * 4. begin(key) with a bigger key in next page
 * 5. begin(key) with a leaf page but no bigger key in there
 */
TEST(BPlusTreeTests, IteratorTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(100, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  ASSERT_EQ(page_id, HEADER_PAGE_ID);
  (void)header_page;

  int key_size = 50;
  int gap = 3;
  std::vector<int64_t> keys(key_size);
  for (int i = 0; i < key_size; i++) {
    // 0, 3, 6, 9, 12, ..., 147
    keys.push_back(gap * i);
  }

  std::cout << "Insert " << key_size << " keys with gap " << gap << " into B+ Tree and verify their existences"
            << std::endl;
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::cout << "test case 1: normal begin()" << std::endl;
  /* test case 1 */
  int64_t start_key = 0;
  int64_t current_key = start_key;
  for (auto it = tree.Begin(); it != tree.End(); ++it) {
    auto location = (*it).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + gap;
  }
  EXPECT_EQ(current_key, gap * key_size);

  std::cout << "test case 2: begin(key) with exact key match" << std::endl;
  /* test case 2 */
  start_key = 30;
  current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto it = tree.Begin(index_key); it != tree.End(); ++it) {
    auto location = (*it).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + gap;
  }
  EXPECT_EQ(current_key, gap * key_size);

  std::cout << "test case 3: begin(key) with bigger key in that leaf page" << std::endl;
  /* test case 3 */
  start_key = 31;
  current_key = 33;
  index_key.SetFromInteger(start_key);
  for (auto it = tree.Begin(index_key); it != tree.End(); ++it) {
    auto location = (*it).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + gap;
  }
  EXPECT_EQ(current_key, gap * key_size);

  std::cout << "test case 4: begin(key) with bigger key in next leaf page" << std::endl;
  /* test case 4 */
  start_key = 34;
  current_key = 36;
  index_key.SetFromInteger(start_key);
  for (auto it = tree.Begin(index_key); it != tree.End(); ++it) {
    auto location = (*it).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + gap;
  }
  EXPECT_EQ(current_key, gap * key_size);

  std::cout << "test case 5: begin(key) with no bigger key anymore" << std::endl;
  /* test case 5 */
  start_key = 148;
  index_key.SetFromInteger(start_key);
  auto it = tree.Begin(index_key);
  EXPECT_EQ(it, tree.End());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
}  // namespace bustub
