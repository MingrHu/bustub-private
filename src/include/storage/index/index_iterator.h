//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(BufferPoolManager* bpm,page_id_t target_leafpgid,int index,IndexPageType page_type);
  IndexIterator();
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType &, const ValueType &>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { 
    return itr.index_ == index_ && itr.leaf_pgid_ == leaf_pgid_ && page_type_ == itr.page_type_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { 
    return page_type_!= itr.page_type_ || itr.index_ != index_ || itr.leaf_pgid_ != leaf_pgid_;
  }

 private:
  // add your own private member variables here
  BufferPoolManager *bpm_;
  page_id_t leaf_pgid_;
  int index_;
  IndexPageType page_type_;
};

}  // namespace bustub
