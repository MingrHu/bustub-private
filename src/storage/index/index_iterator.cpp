//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t target_leafpgid, int index,
                                  IndexPageType page_type) {
  bpm_ = bpm;
  leaf_pgid_ = target_leafpgid;
  index_ = index;
  page_type_ = page_type;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_type_ == IndexPageType::INVALID_INDEX_PAGE; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  auto leaf_page_guard = bpm_->ReadPage(leaf_pgid_);
  auto leaf_page = leaf_page_guard.template As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  kv_store_ = {leaf_page->KeyAt(index_), leaf_page->ValueAt(index_)};
  return kv_store_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (page_type_ != IndexPageType::INVALID_INDEX_PAGE) {
    auto cur_leaf_guard = bpm_->ReadPage(leaf_pgid_);
    auto cur_leaf_page = cur_leaf_guard.template As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    page_id_t next_leaf_pgid = cur_leaf_page->GetNextPageId();
    if (index_ + 1 != cur_leaf_page->GetSize()) {
      index_ += 1;
    } else if (next_leaf_pgid == INVALID_PAGE_ID) {
      index_ = -1;
      leaf_pgid_ = INVALID_PAGE_ID;
      page_type_ = IndexPageType::INVALID_INDEX_PAGE;
    } else {
      leaf_pgid_ = next_leaf_pgid;
      index_ = 0;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
