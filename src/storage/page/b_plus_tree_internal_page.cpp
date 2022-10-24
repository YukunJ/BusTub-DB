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
  SetSize(1);  // the first entry with invalid key and negative infinity pointer
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
 * Given a key,find the jump index for which the traversal could proceed
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::SearchJumpIdx(const KeyType &key,
                                                                             KeyComparator &comparator) -> int {
  // find smallest i s.t. key <= curr_page[i].key
  auto bigger_or_equal_key_idx = -1;
  auto left = 1;
  auto right = GetSize() - 1;
  while (left <= right) {
    // bindary search
    auto mid = left + (right - left) / 2;
    if (comparator(key, KeyAt(mid)) <= 0) {
      bigger_or_equal_key_idx = mid;
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  auto jump_idx = -1;
  if (bigger_or_equal_key_idx == -1) {
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

  return jump_idx;
}

/*
 * Flow through the internal page based on the key and comparator
 * and return next level page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::SearchPage(const KeyType &key, KeyComparator &comparator)
    -> ValueType {
  return ValueAt(SearchJumpIdx(key, comparator));
}

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::Insert(const KeyType &key, const ValueType &value,
                                                                      KeyComparator &comparator) -> bool {
  // need to maintain sorted order
  auto insert_idx = GetSize();  // initial assume at right-hand most
  auto left = 1;
  auto right = GetSize() - 1;
  while (left <= right) {
    // bindary search
    auto mid = left + (right - left) / 2;
    auto comp_res = comparator(key, KeyAt(mid));
    if (comp_res == 0) {
      return false;  // duplicate key
    }
    if (comp_res < 0) {
      // mid might be the place
      insert_idx = mid;
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  ExcavateIndex(insert_idx);
  SetKeyAt(insert_idx, key);
  SetValueAt(insert_idx, value);
  IncreaseSize(1);
  return true;
}

/*
 * Locate a key's index within this leaf page
 * return -1 if not found
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::FindKeyPosition(const KeyType &key,
                                                                               KeyComparator &comparator) -> int {
  auto left = 1;
  auto right = GetSize() - 1;
  while (left <= right) {
    auto mid = left + (right - left) / 2;
    auto comp_res = comparator(key, KeyAt(mid));
    if (comp_res == 0) {
      return mid;
    }
    if (comp_res < 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  return -1;
}

/*
 * Delete a pair with the specified key from this leaf page
 * return false if such key doesn't exist, true if successful deletion
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::RemoveKey(const KeyType &key, KeyComparator &comparator)
    -> bool {
  auto to_delete_index = FindKeyPosition(key, comparator);
  if (to_delete_index == -1) {
    return false;
  }
  FillIndex(to_delete_index + 1);  // shift left by 1 starting from to_delete_index + 1
  DecreaseSize(1);
  return true;
}

/*
 * Move All elements in this node to another recipient
 * and adjust size of both sides accordingly
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::MoveAllTo(BPlusTreeInternalPage *recipient) {
  auto recipient_size = recipient->GetSize();
  auto size = GetSize();
  std::copy(&array_[0], &array_[size], &recipient->array_[recipient_size]);
  SetSize(0);
  recipient->IncreaseSize(size);
}

/*
 * Internal Page's split is different from that of leaf page
 * We split internal page before an insertion if before insertion the interal page is full
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::MoveLatterHalfWithOneExtraTo(
    BPlusTreeInternalPage *recipient, KeyType extra_key, ValueType extra_value, KeyComparator &comparator) {
  BUSTUB_ASSERT(GetSize() == GetMaxSize(), "MoveLatterHalfWithOneExtraTo(): Assert GetSize() == GetMaxSize()");
  // cannot overflow the page by inserting into it directly then re-distribute
  //  auto total_size = GetSize() + 1;
  //  MappingType tmp_space[total_size];
  //  std::copy(&array_[0], &array_[GetSize()], tmp_space);
  //  int insert_position = GetSize();
  //  auto left = 0;
  //  auto right = GetSize() - 1;
  //  while (left <= right) {
  //    auto mid = left + (right - left) / 2;
  //    if (comparator(extra_key, KeyAt(mid)) < 0) {
  //      insert_position = mid;
  //      right = mid - 1;
  //    } else {
  //      left = mid + 1;
  //    }
  //  }
  //  // shift everything starting from insertion_position to right by 1 position
  //  std::copy_backward(tmp_space + insert_position, tmp_space + GetSize(), tmp_space + GetSize() + 1);
  //  tmp_space[insert_position].first = extra_key;
  //  tmp_space[insert_position].second = extra_value;
  //  auto size_retain = total_size / 2 + (total_size % 2 != 0);  // round up
  //  auto size_move = total_size - size_retain;
  //  std::copy(&tmp_space[0], &tmp_space[size_retain], array_);
  //  std::copy(&tmp_space[size_retain], &tmp_space[total_size], recipient->array_);
  //  SetSize(size_retain);
  //  recipient->SetSize(size_move);
  auto total_size = GetSize() + 1;
  Insert(extra_key, extra_value, comparator);
  auto size_retain = total_size / 2 + (total_size % 2 != 0);  // round up
  auto size_move = total_size - size_retain;
  std::copy(&array_[size_retain], &array_[total_size], recipient->array_);
  SetSize(size_retain);
  recipient->SetSize(size_move);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::GetMappingSize() -> size_t {
  return sizeof(MappingType);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::GetArray() -> char * {
  return reinterpret_cast<char *>(&array_[0]);
}

INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::ExcavateIndex(int index) {
  std::copy_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::FillIndex(int index) {
  std::copy(array_ + index, array_ + GetSize(), array_ + index - 1);
}

/*
 * Remove my first element to the end of recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::MoveFirstToEndOf(BPlusTreeInternalPage *recipient) {
  auto recipient_size = recipient->GetSize();
  recipient->SetKeyAt(recipient_size, KeyAt(0));
  recipient->SetValueAt(recipient_size, ValueAt(0));
  recipient->IncreaseSize(1);
  FillIndex(1);
  DecreaseSize(1);
}

/*
 * Remove my last element to the front of recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::MoveLastToFrontOf(BPlusTreeInternalPage *recipient) {
  auto size = GetSize();
  recipient->ExcavateIndex(0);
  recipient->SetKeyAt(0, KeyAt(size - 1));
  recipient->SetValueAt(0, ValueAt(size - 1));
  recipient->IncreaseSize(1);
  DecreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
