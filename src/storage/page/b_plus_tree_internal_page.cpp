//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
  SetMaxSize(max_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get/set the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::SetValueAt(int index, const ValueType &value) {
  array_[index].second = value;
}

/*
 * Flow through the internal page based on the key and comparator
 * and return next level page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::SearchPage(const KeyType &key, KeyComparator& comparator) -> ValueType {
  // TODO(YukunJ): Support binary search
  // find smallest i s.t. key <= curr_page[i].key
  auto bigger_or_equal_key_idx = -1;
  auto exists_bigger = false;
  for (auto i = 1; i < GetSize(); i++) {
    // 1st key is INVALID, start from the 2nd key
    if (comparator(key, KeyAt(i)) <= 0) {
      bigger_or_equal_key_idx = i;
      exists_bigger = true;
      break;
    }
  }
  auto jump_idx = -1;
  if (!exists_bigger) {
    jump_idx = GetSize() - 1;
  } else {
    if (comparator(key, KeyAt(bigger_or_equal_key_idx)) == 0) {
      // equal, go to right pointer
      jump_idx = bigger_or_equal_key_idx;
    } else {
      // strict smaller, go to left pointer
      jump_idx = bigger_or_equal_key_idx - 1;
    }
  }
  return ValueAt(jump_idx);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
