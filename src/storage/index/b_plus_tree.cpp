#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }
  bool found = false;
  auto leaf_page = FindLeafPage(key);
  for (auto i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(key, leaf_page->KeyAt(i)) == 0) {
      result->push_back(leaf_page->ValueAt(i));
      found = true;
    }
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    InitBPlusTree(key, value);
    return true;
  }
  auto leaf_page = FindLeafPage(key);
  BUSTUB_ASSERT(leaf_page->GetPageId() != INVALID_PAGE_ID, "leaf_page->GetPageId() != INVALID_PAGE_ID");
  bool no_duplicate = leaf_page->Insert(key, value, comparator_);
  if (!no_duplicate) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);  // no modification made
    return false;
  }
  if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
    // overflow, need split
    // TODO(YukunJ): abstract the copying functionality into member functions of B+ Tree Page
    //               instead of raw manipulation
    auto mapping_size = leaf_page->GetMappingSize();
    auto cpy_size = leaf_page->GetMaxSize() * mapping_size;
    char temp[cpy_size];
    memcpy(temp, static_cast<const char *>(leaf_page->GetArray()), cpy_size);
    auto leaf_page_prime = CreateLeafPage();
    // leaf -> leaf_prime -> leaf's origin next
    leaf_page_prime->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page_prime->SetParentPageId(leaf_page->GetParentPageId());  // same parent
    leaf_page->SetNextPageId(leaf_page_prime->GetPageId());
    auto size_retain_in_leaf = leaf_page->GetMaxSize() / 2 + (leaf_page->GetMaxSize() % 2 != 0);  // round up
    auto size_retain_in_prime = leaf_page->GetMaxSize() - size_retain_in_leaf;
    leaf_page->SetSize(size_retain_in_leaf);
    leaf_page_prime->SetSize(size_retain_in_prime);
    memcpy(leaf_page->GetArray(), static_cast<const char *>(temp), size_retain_in_leaf * mapping_size);
    memcpy(leaf_page_prime->GetArray(), static_cast<const char *>(&temp[size_retain_in_leaf * mapping_size]),
           size_retain_in_prime * mapping_size);
    const auto key_upward = leaf_page_prime->KeyAt(0);
    InsertInParent(leaf_page, leaf_page_prime, key_upward);
    buffer_pool_manager_->UnpinPage(leaf_page_prime->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/**
 * Create a new internal page from buffer pool manager
 * Caller should unpin this page after usage
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @return pointer to newly created internal page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::CreateInternalPage()
    -> BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> * {
  page_id_t p_id = INVALID_PAGE_ID;
  auto new_page = buffer_pool_manager_->NewPage(&p_id);
  BUSTUB_ASSERT(p_id != INVALID_PAGE_ID, "p_id != INVALID_PAGE_ID");
  auto internal_page =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(new_page->GetData());
  internal_page->Init(p_id, INVALID_PAGE_ID, internal_max_size_);
  return internal_page;
}

/**
 * Create a new leaf page from buffer pool manager
 * Caller should unpin this page after usage
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @return pointer to newly created leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::CreateLeafPage()
    -> BPlusTreeLeafPage<KeyType, RID, KeyComparator> * {
  page_id_t p_id = INVALID_PAGE_ID;
  auto new_page = buffer_pool_manager_->NewPage(&p_id);
  BUSTUB_ASSERT(p_id != INVALID_PAGE_ID, "p_id != INVALID_PAGE_ID");
  auto leaf_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(new_page->GetData());
  leaf_page->Init(p_id, INVALID_PAGE_ID, leaf_max_size_);
  return leaf_page;
}

/**
 * Initialize a B+ Tree from empty state, update root_page accordingly
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param key
 * @param value
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::InitBPlusTree(const KeyType &key, const ValueType &value) -> void {
  /*
   * Ask for a new page from bpm (which will have pin count = 1)
   * and set this page to be B+ Leaf Page and insert first key-value pair into it
   * update the header page root_idx and unpin this page with dirty flag
   */
  auto root_leaf_page = CreateLeafPage();
  root_page_id_ = root_leaf_page->GetPageId();
  BUSTUB_ASSERT(root_leaf_page != nullptr, "root_leaf_page != nullptr");
  BUSTUB_ASSERT(root_page_id_ != INVALID_PAGE_ID, "root_page_id_ != INVALID_PAGE_ID");
  UpdateRootPageId(root_page_id_);
  auto r = root_leaf_page->Insert(key, value, comparator_);
  BUSTUB_ASSERT(r, "BPlusTree Init Insert should be True");
  buffer_pool_manager_->UnpinPage(root_page_id_, true);  // modification made
}

/**
 * Iterate through the B+ Tree to fetch a leaf page
 * the caller should unpin the leaf page after usage
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param key
 * @return pointer to a leaf page if found, nullptr otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::FindLeafPage(const KeyType &key)
    -> BPlusTreeLeafPage<KeyType, RID, KeyComparator> * {
  BUSTUB_ASSERT(root_page_id_ != INVALID_PAGE_ID, "root_page_id_ != INVALID_PAGE_ID");
  auto curr_page = FetchBPlusTreePage(root_page_id_);
  decltype(curr_page) next_page = nullptr;
  while (!curr_page->IsLeafPage()) {
    auto curr_page_internal = ReinterpretAsInternalPage(curr_page);
    page_id_t jump_pid = curr_page_internal->SearchPage(key, comparator_);
    BUSTUB_ASSERT(jump_pid != INVALID_PAGE_ID, "jump_pid != INVALID_PAGE_ID");
    next_page = FetchBPlusTreePage(jump_pid);
    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    curr_page = next_page;
  }
  BUSTUB_ASSERT(curr_page->IsLeafPage(), "curr_page->IsLeafPage()");
  return ReinterpretAsLeafPage(curr_page);
}

/*
 * Recursively Insert Into the parent node
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTree<KeyType, ValueType, KeyComparator>::InsertInParent(BPlusTreePage *left_page, BPlusTreePage *right_page,
                                                                  const KeyType &upward_key) {
  if (left_page->IsRootPage()) {
    auto new_root_page = CreateInternalPage();
    root_page_id_ = new_root_page->GetPageId();
    UpdateRootPageId(root_page_id_);
    new_root_page->SetValueAt(0, left_page->GetPageId());
    new_root_page->SetKeyAt(1, upward_key);
    new_root_page->SetValueAt(1, right_page->GetPageId());
    new_root_page->IncreaseSize(1);
    BUSTUB_ASSERT(new_root_page->GetSize() == 2, "new_root_page->GetSize() == 2");
    left_page->SetParentPageId(root_page_id_);
    right_page->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }
  // upon entry, both left and right's parent point to this parent_page, may need change
  auto parent_page = ReinterpretAsInternalPage(FetchBPlusTreePage(left_page->GetParentPageId()));
  parent_page->Insert(upward_key, right_page->GetPageId(), comparator_);
  if (parent_page->GetSize() == parent_page->GetMaxSize()) {
    // TODO(YukunJ): duplicate code with Insert main procedure, only differ in Leaf/Interal Type
    //               try factor it out later
    // overflow in parent page, split parent page
    // parent page is definitely internal page, be careful of the 0-index invalid key
    auto mapping_size = parent_page->GetMappingSize();
    auto cpy_size = parent_page->GetMaxSize() * mapping_size;
    char temp[cpy_size];
    memcpy(temp, static_cast<const char *>(parent_page->GetArray()), cpy_size);
    auto parent_page_prime = CreateInternalPage();
    parent_page_prime->SetParentPageId(parent_page->GetParentPageId());                                 // same parent
    auto size_retain_in_parent = parent_page->GetMaxSize() / 2 + (parent_page->GetMaxSize() % 2 != 0);  // round up
    auto size_retain_in_prime = parent_page->GetMaxSize() - size_retain_in_parent;
    parent_page->SetSize(size_retain_in_parent);
    parent_page_prime->SetSize(size_retain_in_prime);
    memcpy(parent_page->GetArray(), static_cast<const char *>(temp), size_retain_in_parent * mapping_size);
    memcpy(parent_page_prime->GetArray(), static_cast<const char *>(&temp[size_retain_in_parent * mapping_size]),
           size_retain_in_prime * mapping_size);
    const auto further_upward_key = parent_page->KeyAt(size_retain_in_parent);
    if (comparator_(upward_key, further_upward_key) >= 0) {
      right_page->SetParentPageId(parent_page_prime->GetPageId());
    } else {
      right_page->SetParentPageId(parent_page->GetPageId());
    }
    InsertInParent(parent_page, parent_page_prime, further_upward_key);
    buffer_pool_manager_->UnpinPage(parent_page_prime->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/**
 * Fetch a page using bufferPoolManager
 * and return it in the form of Base Class BPlusTreePage
 * User could further reinterpret_cast the page based on page type
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page_id the page id to be fetched from buffer pool manager
 * @return pointer to BPlusTreePage, to be further reinterpreted by caller function
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::FetchBPlusTreePage(page_id_t page_id) -> BPlusTreePage * {
  return reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
}

/** Cast a Base BPlusTree Page to LeafPage */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::ReinterpretAsLeafPage(BPlusTreePage *page)
    -> BPlusTreeLeafPage<KeyType, RID, KeyComparator> * {
  return reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(page);
}

/** Cast a Base BPlusTree Page to InternalPage */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::ReinterpretAsInternalPage(BPlusTreePage *page)
    -> BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> * {
  return reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
