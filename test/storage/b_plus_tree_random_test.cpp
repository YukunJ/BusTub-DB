//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_delete_test.cpp
//
// Identification: test/storage/b_plus_tree_delete_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>  // NOLINT
#include <cstdio>
#include <iterator>  // NOLINT
#include <random>    // NOLINT

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

TEST(BPlusTreeTests, ScaleRandomTest) {
  // create KeyComparator and index schema
  std::vector<int> key_generations{50, 100, 1000, 3000, 5000};
  std::vector<float> remove_ratios{0.1, 0.3, 0.5, 0.7, 0.9};
  for (auto key_generation : key_generations) {
    for (auto remove_ratio : remove_ratios) {
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
      (void)header_page;

      std::unordered_set<int64_t> random_keys;
      for (int i = 0; i < key_generation; i++) {
        random_keys.insert(rand() % INT64_MAX);  // NOLINT
      }

      for (auto key : random_keys) {
        int64_t value = key & 0xFFFFFFFF;
        rid.Set(static_cast<int32_t>(key >> 32), value);
        index_key.SetFromInteger(key);
        tree.Insert(index_key, rid, transaction);
      }

      std::vector<RID> rids;
      for (auto key : random_keys) {
        rids.clear();
        index_key.SetFromInteger(key);
        tree.GetValue(index_key, &rids);
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].GetSlotNum(), value);
      }

      std::vector<int64_t> remove_keys;
      int remove_number = static_cast<int>(random_keys.size() * remove_ratio);
      auto seed = std::random_device{}();  // NOLINT
      std::sample(random_keys.begin(), random_keys.end(), std::back_inserter(remove_keys), remove_number,
                  std::mt19937{seed});  // NOLINT
      for (auto i : remove_keys) {
        index_key.SetFromInteger(i);
        tree.Remove(index_key, transaction);
      }

      int64_t size = 0;
      bool is_present;

      for (auto key : random_keys) {
        rids.clear();
        index_key.SetFromInteger(key);
        is_present = tree.GetValue(index_key, &rids);

        if (!is_present) {
          EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
        } else {
          EXPECT_EQ(rids.size(), 1);
          EXPECT_EQ(rids[0].GetPageId(), 0);
          EXPECT_EQ(rids[0].GetSlotNum(), key);
          size = size + 1;
        }
      }

      EXPECT_EQ(size, random_keys.size() - remove_keys.size());
      std::cout << "=====================================================" << std::endl;
      std::cout << "We insert " << random_keys.size() << " elements into the B+ Tree" << std::endl;
      std::cout << "randomly remove " << remove_keys.size() << " keys" << std::endl;
      std::cout << "Then in the B+ Tree we found " << size << " keys remaining" << std::endl;
      std::cout << "We expect size(" << size << ") equals key-remove(" << random_keys.size() - remove_keys.size() << ")"
                << std::endl;
      std::cout << "=====================================================" << std::endl;
      bpm->UnpinPage(HEADER_PAGE_ID, true);
      delete transaction;
      delete disk_manager;
      delete bpm;
      remove("test.db");
      remove("test.log");
    }
  }
}
}  // namespace bustub
