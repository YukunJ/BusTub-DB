/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * default ctor makes the End iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

/*
 * Assume the caller has fetched the leaf page for us, unpin it later on when we are done with it
 */
INDEX_TEMPLATE_ARGUMENTS
IndexIterator<KeyType, ValueType, KeyComparator>::IndexIterator(page_id_t page_id, int index, LEAF_TYPE leaf_page,
                                                                BufferPoolManager *buffer_pool_manager)
    : page_id_(page_id), idx_(index), leaf_page_(leaf_page), buffer_pool_manager_(buffer_pool_manager) {
  BUSTUB_ASSERT(page_id == leaf_page->GetPageId(), "page_id == leaf_page->GetPageId()");  // should match
  BUSTUB_ASSERT(leaf_page->GetSize() > index, "leaf_page->GetSize() > index");
  BUSTUB_ASSERT(buffer_pool_manager != nullptr, "buffer_pool_manager != nullptr");
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  // if currently holding a pinned page, unpin it
  if (page_id_ != INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(page_id_, false);
  }
}

/*
 * When slide forward to nowhere, it's the end
 */
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  BUSTUB_ASSERT(page_id_ != INVALID_PAGE_ID, "page_id_ != INVALID_PAGE_ID");
  BUSTUB_ASSERT(page_id_ == leaf_page_->GetPageId(), "page_id_ == leaf_page_->GetPageId()");  // should match
  BUSTUB_ASSERT(leaf_page_->GetSize() > idx_, "leaf_page_->GetSize() > idx_");
  return leaf_page_->PairAt(idx_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    // not able to move forward anymore
    return *this;
  }
  idx_++;  // move forward the cursor
  if (idx_ == leaf_page_->GetSize()) {
    // beyond current page
    // Try fetch next page
    page_id_ = leaf_page_->GetNextPageId();
    if (page_id_ == INVALID_PAGE_ID) {
      // reach end
      buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
      leaf_page_ = nullptr;
      idx_ = -1;
    } else {
      buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
      leaf_page_ = FetchLeafPage(page_id_);
      idx_ = 0;
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto IndexIterator<KeyType, ValueType, KeyComparator>::operator==(const IndexIterator &itr) const -> bool {
  return page_id_ == itr.page_id_ && idx_ == itr.idx_;
}

INDEX_TEMPLATE_ARGUMENTS
auto IndexIterator<KeyType, ValueType, KeyComparator>::operator!=(const IndexIterator &itr) const -> bool {
  return !operator==(itr);
}

/*
 * Fetch a leaf page based on the given page id
 * This increase the pin count of this page by 1
 * Later on when move pass this page or in destructor, need to unpin the page
 */
INDEX_TEMPLATE_ARGUMENTS
auto IndexIterator<KeyType, ValueType, KeyComparator>::FetchLeafPage(page_id_t page_id) -> LEAF_TYPE {
  if (page_id == INVALID_PAGE_ID) {
    // when reaching the end of iterator, no need to fetch any more page
    return nullptr;
  }
  return reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
      buffer_pool_manager_->FetchPage(page_id)->GetData());
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
