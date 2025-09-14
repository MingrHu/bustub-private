//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include <algorithm>
#include <optional>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.template AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  // 获取B+树根对应的页面
  bool res = false;
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.template As<BPlusTreeHeaderPage>();
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    res = true;
  }
  return res;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // 保存目标节点的页面
  Context ctx;

  auto header_page_guard = bpm_->ReadPage(header_page_id_);
  auto root_pgid = header_page_guard.template As<BPlusTreeHeaderPage>()->root_page_id_;
  if (root_pgid == INVALID_PAGE_ID) {
    return false;
  }
  // 先拿到根节点
  ctx.read_set_.push_back(bpm_->ReadPage(root_pgid));
  header_page_guard.Drop();

  int index = -1;

  while (!ctx.read_set_.back().template As<BPlusTreePage>()->IsLeafPage()) {
    // 获取页面 并转为B+树页面
    auto cur_page_guard = std::move(ctx.read_set_.back());
    ctx.read_set_.pop_back();
    // 待操作页面
    auto inner_page = cur_page_guard.template As<InternalPage>();
    // inner_page 查找的要特殊一些
    index = KeyBinarySearch(key, inner_page, false);
    page_id_t pgid = inner_page->ValueAt(index);
    // 放入下一个值的页面
    ctx.read_set_.push_back(bpm_->ReadPage(pgid));
  }

  ReadPageGuard leaf_page_guard = std::move(ctx.read_set_.back());
  ctx.read_set_.pop_back();
  auto leaf_page = leaf_page_guard.template As<LeafPage>();
  index = KeyBinarySearch(key, leaf_page, true);
  if (index == -1) {
    return false;
  }
  result->push_back(leaf_page->ValueAt(index));
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // 获取节点位置顺序：头页面--根节点页面/内部节点页面--叶子节点页面
  // 锁原则：获取当前节点读锁 读取保存键值内容 释放锁 使用键值内容（读锁或写锁）
  // Declaration of context instance. Using the Context is not necessary but advised.

  // 判断树是否为空 线程安全
  {
    auto header_page_guard = bpm_->ReadPage(header_page_id_);
    auto header_page = header_page_guard.template As<BPlusTreeHeaderPage>();
    if (header_page->root_page_id_ == INVALID_PAGE_ID) {
      header_page_guard.Drop();
      auto header_write_page_guard = bpm_->WritePage(header_page_id_);
      auto header_write_page = header_write_page_guard.template AsMut<BPlusTreeHeaderPage>();
      if (header_write_page->root_page_id_ == INVALID_PAGE_ID) {
        // 获取新的页 并设置该页的叶子节点值
        auto new_pgid = bpm_->NewPage();
        auto write_guard = bpm_->WritePage(new_pgid);
        auto leaf_page = write_guard.template AsMut<LeafPage>();
        // 初始化根节点
        leaf_page->Init(leaf_max_size_);
        InsertLeafPageNode(0, key, value, leaf_page);
        // 更新根节点的页ID值
        header_write_page->root_page_id_ = new_pgid;
        return true;
      }
    }
  }

  Context ctx;
  // 乐观锁策略 一路读锁
  // 为了避免锁升级的问题 read_set_必须持有两个节点
  auto h_page_guard = bpm_->ReadPage(header_page_id_);
  ctx.root_page_id_ = h_page_guard.template As<BPlusTreeHeaderPage>()->root_page_id_;
  ctx.read_set_.emplace_back(bpm_->ReadPage(ctx.root_page_id_));
  auto leaf_pgid = ctx.root_page_id_;

  // 尝试读锁去寻找叶子节点
  while (!ctx.read_set_.back().template As<BPlusTreePage>()->IsLeafPage()) {
    h_page_guard.Drop();
    auto cur_page = ctx.read_set_.back().template As<BPlusTreePage>();
    auto parent_inner_page = static_cast<const InternalPage *>(cur_page);
    int index = KeyBinarySearch(key, parent_inner_page, false);
    // 拿到子节点
    leaf_pgid = parent_inner_page->ValueAt(index);

    ctx.read_set_.emplace_back(bpm_->ReadPage(leaf_pgid));
    // 保证可以拿到父亲保护节点
    if (ctx.read_set_.back().template As<BPlusTreePage>()->IsLeafPage()) {
      break;
    }
    ctx.read_set_.pop_front();
  }

  // read_set_一定有叶子节点 先弹出
  ctx.read_set_.pop_back();
  ctx.write_set_.emplace_back(bpm_->WritePage(leaf_pgid));
  auto waited_leaf_page = ctx.write_set_.back().template AsMut<LeafPage>();
  if (ParentIsSafe(waited_leaf_page, true)) {
    int insert_pos = LowerBound(key, waited_leaf_page, true);
    // 不能重复
    if (insert_pos < waited_leaf_page->GetSize() && comparator_(waited_leaf_page->KeyAt(insert_pos), key) == 0) {
      return false;
    }
    InsertLeafPageNode(insert_pos, key, value, waited_leaf_page);
    return true;
  }

  // 乐观和悲观的策略上 只要你采用的是悲观策略 就必须全程持有锁
  // 直到当前节点的父节点安全时才能释放之前的锁 否则不能释放
  ctx.read_set_.clear();
  ctx.write_set_.clear();
  h_page_guard.Drop();
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  ctx.root_page_id_ = ctx.header_page_->template As<BPlusTreeHeaderPage>()->root_page_id_;
  // BUSTUB_ENSURE(ctx.root_page_id_!=INVALID_PAGE_ID, "You read the deleted tree but you do not want it empty!\n");
  ctx.write_set_.emplace_back(bpm_->WritePage(ctx.root_page_id_));

  // 现在write里面有根节点和页面头节点
  // 执行悲观查找
  while (!ctx.write_set_.back().template As<BPlusTreePage>()->IsLeafPage()) {
    // 当前节点先拿出来操作
    auto cur_page_guard = std::move(ctx.write_set_.back());
    auto cur_page = cur_page_guard.template As<InternalPage>();
    ctx.write_set_.pop_back();

    // 获取子节点信息
    int index = KeyBinarySearch(key, cur_page, false);
    // 记录内部节点的索引信息和相关信息
    ctx.indexs_store_.push(index);
    page_id_t next_pgid = cur_page->ValueAt(index);

    // 判断当前节点是否可能发生分裂 不分裂则释放所有父节点
    if (ParentIsSafe(cur_page, true)) {
      ctx.header_page_ = std::nullopt;
      ctx.write_set_.clear();
    }
    // 当前节点再放回去 这里默认所有叶节点的父节点加锁
    ctx.write_set_.emplace_back(std::move(cur_page_guard));
    ctx.write_set_.emplace_back(bpm_->WritePage(next_pgid));
  }

  // 检查当前叶子节点的父亲是否安全
  if (ParentIsSafe(ctx.write_set_.back().template As<LeafPage>(), true)) {
    auto leaf_page_guard = std::move(ctx.write_set_.back());
    auto leaf_page = leaf_page_guard.template AsMut<LeafPage>();
    int insert_pos = LowerBound(key, leaf_page, true);
    if (insert_pos < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(insert_pos), key) == 0) {
      return false;
    }
    InsertLeafPageNode(insert_pos, key, value, leaf_page);
    return true;
  }

  //********************** 否则执行分裂
  // 如果叶子节点需要分裂
  // 分裂的流程是：当前节点加锁 当前节点分裂 保存向上传递的信息 当前节点解锁 寻找父节点/创建父节点 加锁完成插入
  // 如果又发生分裂 则重复上述流程
  // 向上传递分两种情况：
  // 1.没有父节点了 得自己手动创建一个父节点 所以需要分裂的两部分都传递上去
  // 2.存在父节点 那么左半部分和父节点关系不改变 右半部分和父节点重新建立关系
  // 且需要分割点传递到父节点中 父节点直接将分割点放在原来的索引位置后一位
  // (leaf_max_size_ + 1)>> 1: 至少保留一位 (internal_max_size_ >> 1) + 1 保证原节点哨兵位置不改变

  // 1 拿到待分裂节点和插入位置信息
  auto o_leaf_page = ctx.write_set_.back().template AsMut<LeafPage>();
  int insert_pos = LowerBound(key, o_leaf_page, true);

  if (insert_pos < o_leaf_page->GetSize() && comparator_(o_leaf_page->KeyAt(insert_pos), key) == 0) {
    return false;
  }

  // 2 先获取一个存放Key和Value的块
  std::vector<KeyType> leafkey_block;
  std::vector<ValueType> leafval_block;
  leafkey_block.reserve(leaf_max_size_ + 1);
  leafkey_block.reserve(leaf_max_size_ + 1);

  for (int i = 0; i < insert_pos; i++) {
    leafkey_block.push_back(o_leaf_page->KeyAt(i));
    leafval_block.push_back(o_leaf_page->ValueAt(i));
  }
  leafkey_block.push_back(key);
  leafval_block.push_back(value);
  for (int i = insert_pos; i < o_leaf_page->GetSize(); i++) {
    leafkey_block.push_back(o_leaf_page->KeyAt(i));
    leafval_block.push_back(o_leaf_page->ValueAt(i));
  }

  // 3 保存需要往上传递的分裂信息
  int split_pos = (leaf_max_size_ + 1) >> 1;
  // printf("leaf_key_size = %zu\n",leafkey_block.size());
  KeyType split_key = leafkey_block[split_pos];
  page_id_t left_pgid = ctx.write_set_.back().GetPageId();
  page_id_t right_pgid = bpm_->NewPage();

  // 4 获取新的节点以把当前节点的一半移动过去
  auto new_leaf_guard = bpm_->WritePage(right_pgid);
  auto new_leaf_page = new_leaf_guard.template AsMut<LeafPage>();

  // 5 执行分裂操作
  LeafSplitPage(split_pos, o_leaf_page, new_leaf_page, right_pgid, leafkey_block, leafval_block);

  // 每次都可以先释放当前锁 叶子节点的父节点一定有锁
  new_leaf_guard.Drop();
  ctx.write_set_.pop_back();

  // 孩子节点已经分裂了 目前处理的是父节点的问题
  // 只要不空 就说明当前父节点都会受影响
  // 但不一定父节点也会分裂 需要判断
  while (!ctx.write_set_.empty()) {
    // 1.先拿到当前节点信息
    auto parent_page = ctx.write_set_.back().template AsMut<InternalPage>();
    left_pgid = ctx.write_set_.back().GetPageId();
    insert_pos = ctx.indexs_store_.top() + 1;
    ctx.indexs_store_.pop();

    // 插如果入节点后小于最大值数量  则不需要分裂直接放
    if (parent_page->GetSize() < internal_max_size_) {
      InsertInnerPageNode(insert_pos, split_key, right_pgid, parent_page);
      ctx.header_page_ = std::nullopt;
      break;
    }
    // 否则需要进行划分
    // 先获取一个存放Key和Value的块
    std::vector<KeyType> key_block;
    std::vector<page_id_t> pageid_block;
    key_block.reserve(internal_max_size_ + 1);
    pageid_block.reserve(internal_max_size_ + 1);

    for (int i = 0; i < insert_pos; i++) {
      if (i > 0) {
        key_block.push_back(parent_page->KeyAt(i));
      }
      pageid_block.push_back(parent_page->ValueAt(i));
    }
    key_block.push_back(split_key);
    pageid_block.push_back(right_pgid);
    for (int i = insert_pos; i < internal_max_size_; i++) {
      if (i > 0) {
        key_block.push_back(parent_page->KeyAt(i));
      }
      pageid_block.push_back(parent_page->ValueAt(i));
    }

    // 划分分裂点
    // 保存需要往上传递的分裂信息
    split_pos = (internal_max_size_ >> 1) + 1;
    split_key = key_block[split_pos - 1];
    right_pgid = bpm_->NewPage();

    auto new_page_guard = bpm_->WritePage(right_pgid);
    auto new_inner_page = new_page_guard.template AsMut<InternalPage>();

    InnerSplitPage(split_pos, parent_page, new_inner_page, key_block, pageid_block);
    ctx.write_set_.pop_back();
  }

  // 如果会影响到根节点
  if (ctx.header_page_ != std::nullopt) {
    // 开始新建根节点并设置值
    auto new_root_pgid = bpm_->NewPage();
    auto new_root_page_guard = bpm_->WritePage(new_root_pgid);
    auto root_page = new_root_page_guard.template AsMut<InternalPage>();
    root_page->Init(internal_max_size_);
    root_page->SetSize(2);
    // 这里必须手动设置值 因为不需要插入任何键
    root_page->SetValueAt(0, left_pgid);
    root_page->SetKeyAt(1, split_key);
    root_page->SetValueAt(1, right_pgid);

    ctx.write_set_.clear();
    auto header_page = ctx.header_page_->template AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = new_root_pgid;
  }
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // 判断树是否为空
  {
    auto header_page_guard = bpm_->ReadPage(header_page_id_);
    auto header_page = header_page_guard.template As<BPlusTreeHeaderPage>();
    if (header_page->root_page_id_ == INVALID_PAGE_ID) {
      return;
    }
  }

  // Declaration of context instance.
  Context ctx;
  // 乐观锁 一路读锁
  auto h_page_guard = bpm_->ReadPage(header_page_id_);
  ctx.root_page_id_ = h_page_guard.template As<BPlusTreeHeaderPage>()->root_page_id_;
  ctx.read_set_.emplace_back(bpm_->ReadPage(ctx.root_page_id_));
  auto leaf_pgid = ctx.root_page_id_;

  // 尝试读锁去寻找叶子节点
  while (!ctx.read_set_.back().template As<BPlusTreePage>()->IsLeafPage()) {
    h_page_guard.Drop();
    auto cur_page = ctx.read_set_.back().template As<BPlusTreePage>();
    auto parent_inner_page = static_cast<const InternalPage *>(cur_page);
    int index = KeyBinarySearch(key, parent_inner_page, false);
    // 拿到子节点
    leaf_pgid = parent_inner_page->ValueAt(index);
    ctx.read_set_.emplace_back(bpm_->ReadPage(leaf_pgid));
    if (ctx.read_set_.back().template As<BPlusTreePage>()->IsLeafPage()) {
      break;
    }
    ctx.read_set_.pop_front();
  }

  // 这里升级叶子读锁为写锁的时候一定是有父节点保护的
  ctx.read_set_.pop_back();
  ctx.write_set_.emplace_back(bpm_->WritePage(leaf_pgid));
  auto waited_leaf_page = ctx.write_set_.front().template AsMut<LeafPage>();
  if (ParentIsSafe(waited_leaf_page, false)) {
    int index = KeyBinarySearch(key, waited_leaf_page, true);
    if (index == -1) {
      return;
    }
    // 删除这个键值
    DeleteSpeciKeyVal(index, waited_leaf_page, true);
    return;
  }

  // 不安全则采用悲观锁的方式
  ctx.read_set_.clear();
  ctx.write_set_.clear();
  h_page_guard.Drop();
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  ctx.root_page_id_ = ctx.header_page_->template As<BPlusTreeHeaderPage>()->root_page_id_;
  ctx.write_set_.emplace_back(bpm_->WritePage(ctx.root_page_id_));

  // 目前包含根节点和头页面节点
  while (!ctx.write_set_.back().template AsMut<BPlusTreePage>()->IsLeafPage()) {
    auto cur_page_guard = std::move(ctx.write_set_.back());
    auto cur_page = cur_page_guard.template As<InternalPage>();
    ctx.write_set_.pop_back();

    int index = KeyBinarySearch(key, cur_page, false);
    ctx.indexs_store_.push(index);
    page_id_t next_pgid = cur_page->ValueAt(index);

    if (ParentIsSafe(cur_page, false)) {
      ctx.header_page_ = std::nullopt;
      ctx.write_set_.clear();
    }
    ctx.write_set_.emplace_back(std::move(cur_page_guard));
    ctx.write_set_.emplace_back(bpm_->WritePage(next_pgid));
  }

  // 现在最后一个write_set是叶子节点
  int dl_pos = KeyBinarySearch(key, ctx.write_set_.back().template AsMut<LeafPage>(), true);
  if (dl_pos == -1) {
    return;
  }

  if (ParentIsSafe(ctx.write_set_.back().template AsMut<LeafPage>(), false)) {
    auto leaf_page_guard = std::move(ctx.write_set_.back());
    auto leaf_page = leaf_page_guard.template AsMut<LeafPage>();
    DeleteSpeciKeyVal(dl_pos, leaf_page, true);
    return;
  }

  // 1 对叶子节点先操作 记录必要的相关信息
  auto o_leaf_page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto o_leaf_page = o_leaf_page_guard.template AsMut<LeafPage>();
  page_id_t dl_pgid = o_leaf_page_guard.GetPageId();

  // 叶子节点会发生借用或者合并
  // 说明前叶子节点一定会被删空
  // 2 执行叶子节点的删除
  // 先进行删除操作 这个删除操作一定会导致叶节点为空
  DeleteSpeciKeyVal(dl_pos, o_leaf_page, true);

  // 又是根又是叶节点
  // 3 删除后检查这个节点是否为根节点 如果为根节点就不存在后面的操作
  if (leaf_pgid == ctx.root_page_id_) {
    AdjustRoot(o_leaf_page, ctx);
    o_leaf_page_guard.Drop();
    bpm_->DeletePage(dl_pgid);
    return;
  }

  int leaf_page_index = ctx.indexs_store_.top();
  ctx.indexs_store_.pop();
  // 更新当前叶节点的next_pgid信息
  // 4 如果不是根节点 尝试借用或合并
  auto leaf_parent_page = ctx.write_set_.back().template AsMut<InternalPage>();

  // 4.1先尝试借用兄弟节点
  bool redis_out = Redistribute(leaf_parent_page, leaf_page_index, true, o_leaf_page);
  if (redis_out) {
    return;
  }
  // 4.2再尝试合并节点 合并节点会涉及到原来页面的删除
  dl_pgid = MergeNode(leaf_parent_page, leaf_page_index, true, o_leaf_page);
  if (dl_pgid == INVALID_PAGE_ID) {
    // 还原待删除pgid
    dl_pgid = o_leaf_page_guard.GetPageId();
  }

  o_leaf_page_guard.Drop();
  bpm_->DeletePage(dl_pgid);

  // 开始检查父节点情况
  // 重复执行叶子节点的相关操作
  // 与叶子节点不同的是当前操作节点都是已经被删除相关键值后的
  // 而且不存在页面被删空的情况 除非是根节点页面
  while (!ctx.write_set_.empty()) {
    // 和插入有小区别 删除的时候保证当前节点不会是header 最多是根
    auto op_page_guard = std::move(ctx.write_set_.back());
    auto op_page = op_page_guard.template AsMut<InternalPage>();
    ctx.write_set_.pop_back();

    // 1 检查当前节点是否需要进行合并或者借用
    if (op_page->GetSize() >= op_page->GetMinSize()) {
      return;
    }

    // 2 检查是否为根
    if (op_page_guard.GetPageId() == ctx.root_page_id_) {
      AdjustRoot(op_page, ctx);
      auto op_pgid = op_page_guard.GetPageId();
      op_page_guard.Drop();
      bpm_->DeletePage(op_pgid);
      return;
    }

    // 3 先尝试借用兄弟节点
    auto parent_page = ctx.write_set_.back().template AsMut<InternalPage>();
    int index = ctx.indexs_store_.top();
    ctx.indexs_store_.pop();

    bool borrow_res = Redistribute(parent_page, index, false, op_page);
    if (borrow_res) {
      return;
    }

    // 4 否则尝试合并
    dl_pgid = MergeNode(parent_page, index, false, op_page);
    if (dl_pgid == INVALID_PAGE_ID) {
      dl_pgid = op_page_guard.GetPageId();
    }
    op_page_guard.Drop();
    bpm_->DeletePage(dl_pgid);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto header_page_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_page_guard.template As<BPlusTreeHeaderPage>();
  page_id_t cur_pgid = header_page->root_page_id_;
  if (cur_pgid == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE{bpm_, INVALID_PAGE_ID, -1, IndexPageType::INVALID_INDEX_PAGE};
  }
  header_page_guard.Drop();
  Context ctx;
  ctx.read_set_.push_back(bpm_->ReadPage(cur_pgid));
  while (!ctx.read_set_.back().template As<BPlusTreePage>()->IsLeafPage()) {
    auto cur_page_guard = std::move(ctx.read_set_.back());
    ctx.read_set_.pop_back();
    auto cur_page = cur_page_guard.template As<InternalPage>();
    cur_pgid = cur_page->ValueAt(0);
    ctx.read_set_.push_back(bpm_->ReadPage(cur_pgid));
  }
  return INDEXITERATOR_TYPE{bpm_, cur_pgid, 0, IndexPageType::LEAF_PAGE};
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto header_page_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_page_guard.template As<BPlusTreeHeaderPage>();
  page_id_t cur_pgid = header_page->root_page_id_;
  if (cur_pgid == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE{bpm_, INVALID_PAGE_ID, -1, IndexPageType::INVALID_INDEX_PAGE};
  }
  header_page_guard.Drop();
  Context ctx;
  ctx.read_set_.push_back(bpm_->ReadPage(cur_pgid));
  int index = -1;
  while (!ctx.read_set_.back().template As<BPlusTreePage>()->IsLeafPage()) {
    auto cur_page_guard = std::move(ctx.read_set_.back());
    ctx.read_set_.pop_back();
    auto cur_page = cur_page_guard.template As<InternalPage>();
    index = KeyBinarySearch(key, cur_page, false);
    cur_pgid = cur_page->ValueAt(index);
    ctx.read_set_.push_back(bpm_->ReadPage(cur_pgid));
  }
  auto leaf_page = ctx.read_set_.back().template As<LeafPage>();
  index = KeyBinarySearch(key, leaf_page, true);
  if (index == -1) {
    return INDEXITERATOR_TYPE{bpm_, INVALID_PAGE_ID, -1, IndexPageType::INVALID_INDEX_PAGE};
  }
  return INDEXITERATOR_TYPE{bpm_, cur_pgid, index, IndexPageType::LEAF_PAGE};
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1, IndexPageType::INVALID_INDEX_PAGE);
}

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.template As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

/**
 * @brief Other helper function!
 * @return Page id of the root of this tree
 *
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::KeyBinarySearch(const KeyType &key, const BPlusTreePage *page, bool is_leaf) -> int {
  int l = 0;
  int r = page->GetSize() - 1;
  auto mid = l + (r - l) / 2;

  if (is_leaf) {
    auto leaf_page = static_cast<const LeafPage *>(page);
    while (l <= r) {
      // mid < target
      if (comparator_(leaf_page->KeyAt(mid), key) == -1) {
        l = mid + 1;
      }
      // mid > target
      else if (comparator_(leaf_page->KeyAt(mid), key) == 1) {
        r = mid - 1;
      } else {
        return mid;
      }
      mid = l + (r - l) / 2;
    }
    return -1;
  }

  // 查找内部节点
  l = 1, r = page->GetSize() - 1, mid = l + (r - l) / 2;
  auto inner_page = static_cast<const InternalPage *>(page);

  // 比第一个有效键值小
  if (comparator_(inner_page->KeyAt(l), key) == 1) {
    return 0;
  }

  // 找到第一个大于等于key的节点
  while (l <= r) {
    if (comparator_(inner_page->KeyAt(mid), key) == -1) {
      l = mid + 1;
    } else {
      r = mid - 1;
    }
    mid = l + (r - l) / 2;
  }
  if (l != inner_page->GetSize() && comparator_(key, inner_page->KeyAt(l)) == 0) {
    return l;
  }
  return l - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LowerBound(const KeyType &key, const BPlusTreePage *target_page, bool is_leaf) -> int {
  int res = -1;
  if (is_leaf) {
    auto leaf_page = static_cast<const LeafPage *>(target_page);
    int l = 0;
    int r = leaf_page->GetSize() - 1;
    auto mid = l + (r - l) / 2;
    while (l <= r) {
      // mid < target
      if (comparator_(leaf_page->KeyAt(mid), key) == -1) {
        l = mid + 1;
      }
      // mid >= target
      else {
        r = mid - 1;
      }
      mid = l + (r - l) / 2;
    }
    res = l;
  } else {
    auto inner_page = static_cast<const InternalPage *>(target_page);
    int l = 1;
    int r = inner_page->GetSize() - 1;
    auto mid = l + (r - l) / 2;
    while (l <= r) {
      // mid < target
      if (comparator_(inner_page->KeyAt(mid), key) == -1) {
        l = mid + 1;
      }
      // mid >= target
      else {
        r = mid - 1;
      }
      mid = l + (r - l) / 2;
    }
    res = l;
  }
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertLeafPageNode(int insert_pos, const KeyType &key, const ValueType &val, LeafPage *leaf_page) {
  int org_size = leaf_page->GetSize();
  leaf_page->ChangeSizeBy(1);
  for (int i = org_size; i > insert_pos; i--) {
    // 键和值全部往后移动一位
    leaf_page->SetKeyAt(i, leaf_page->KeyAt(i - 1));
    leaf_page->SetValueAt(i, leaf_page->ValueAt(i - 1));
  }
  leaf_page->SetKeyAt(insert_pos, key);
  leaf_page->SetValueAt(insert_pos, val);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInnerPageNode(int insert_pos, const KeyType &key, const page_id_t &val,
                                         InternalPage *inner_page) {
  int org_size = inner_page->GetSize();
  inner_page->ChangeSizeBy(1);
  for (int i = org_size; i > insert_pos; i--) {
    // 哨兵键不能直接移动
    if (i - 1 > 0) {
      inner_page->SetKeyAt(i, inner_page->KeyAt(i - 1));
    }
    inner_page->SetValueAt(i, inner_page->ValueAt(i - 1));
  }
  if (insert_pos > 0) {
    inner_page->SetKeyAt(insert_pos, key);
  }
  inner_page->SetValueAt(insert_pos, val);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LeafSplitPage(int split_pos, LeafPage *origional_page, LeafPage *new_page, page_id_t right_pgid,
                                   std::vector<KeyType> &key_block, std::vector<ValueType> &val_block) {
  new_page->Init(leaf_max_size_);
  new_page->SetSize(leaf_max_size_ + 1 - split_pos);
  origional_page->SetSize(split_pos);

  new_page->SetNextPageId(origional_page->GetNextPageId());
  origional_page->SetNextPageId(right_pgid);

  for (int i = 0; i < leaf_max_size_ + 1; i++) {
    if (i < split_pos) {
      origional_page->SetKeyAt(i, key_block[i]);
      origional_page->SetValueAt(i, val_block[i]);
    } else {
      new_page->SetKeyAt(i - split_pos, key_block[i]);
      new_page->SetValueAt(i - split_pos, val_block[i]);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InnerSplitPage(int split_pos, InternalPage *origional_page, InternalPage *new_page,
                                    std::vector<KeyType> &key_block, std::vector<page_id_t> &val_block) {
  // 初始化新页面
  origional_page->SetSize(split_pos);
  new_page->Init(internal_max_size_);
  new_page->SetSize(internal_max_size_ + 1 - split_pos);
  // 将当前节点的分割点及以后键值复制到新的节点里面
  // inner比较特殊 必须从1开始 0是哨兵节点
  for (int i = 0; i < internal_max_size_ + 1; i++) {
    if (i < split_pos) {
      if (i > 0) {
        origional_page->SetKeyAt(i, key_block[i - 1]);
      }
      origional_page->SetValueAt(i, val_block[i]);
    } else {
      if (i - split_pos > 0) {
        new_page->SetKeyAt(i - split_pos, key_block[i - 1]);
      }
      new_page->SetValueAt(i - split_pos, val_block[i]);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteSpeciKeyVal(int delete_pos, BPlusTreePage *op_page, bool is_leaf) {
  // 如果删除的是叶子节点的键值
  if (is_leaf) {
    auto leaf_page = static_cast<LeafPage *>(op_page);
    for (int i = delete_pos; i < leaf_page->GetSize() - 1; i++) {
      leaf_page->SetKeyAt(i, leaf_page->KeyAt(i + 1));
      leaf_page->SetValueAt(i, leaf_page->ValueAt(i + 1));
    }
  }
  // 否则删除的内部节点的键值
  else {
    auto inner_page = static_cast<InternalPage *>(op_page);
    for (int i = delete_pos; i < inner_page->GetSize() - 1; i++) {
      if (i > 0) {
        inner_page->SetKeyAt(i, inner_page->KeyAt(i + 1));
      }
      inner_page->SetValueAt(i, inner_page->ValueAt(i + 1));
    }
  }
  op_page->ChangeSizeBy(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ParentIsSafe(const BPlusTreePage *cur_page, bool is_insert) -> bool {
  if (is_insert) {
    return (cur_page->GetSize() < cur_page->GetMaxSize());
  }
  return cur_page->GetSize() > cur_page->GetMinSize();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_page, Context &ctx) {
  if (!old_root_page->IsLeafPage() && old_root_page->GetSize() == 1) {
    auto root_page = static_cast<InternalPage *>(old_root_page);
    page_id_t only_child = root_page->ValueAt(0);
    auto header_page = ctx.header_page_->template AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = only_child;
  } else if (old_root_page->IsLeafPage() && old_root_page->GetSize() == 0) {
    auto header_page = ctx.header_page_->template AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = INVALID_PAGE_ID;
  }
  ctx.header_page_ = std::nullopt;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowSibLeft(BPlusTreePage *sib_page, BPlusTreePage *poor_page, bool is_leaf, const KeyType &key)
    -> KeyType {
  int sib_size = sib_page->GetSize();
  KeyType call_back_key;
  if (is_leaf) {
    auto sib_leaf_page = static_cast<LeafPage *>(sib_page);
    auto poor_leaf_page = static_cast<LeafPage *>(poor_page);
    // 拿走左兄弟最大的键值
    call_back_key = sib_leaf_page->KeyAt(sib_size - 1);
    auto val = sib_leaf_page->ValueAt(sib_size - 1);
    InsertLeafPageNode(0, call_back_key, val, poor_leaf_page);
    DeleteSpeciKeyVal(sib_size - 1, sib_leaf_page, true);
  } else {
    auto sib_inner_page = static_cast<InternalPage *>(sib_page);
    auto poor_inner_page = static_cast<InternalPage *>(poor_page);
    // 保证inner_page 的第一个哨兵键不能被访问
    // 拿走左兄弟最大的键值
    call_back_key = sib_inner_page->KeyAt(sib_size - 1);
    auto val = sib_inner_page->ValueAt(sib_size - 1);
    InsertInnerPageNode(0, key, val, poor_inner_page);
    // 手动填入第一个有效键
    poor_inner_page->SetKeyAt(1, key);
    DeleteSpeciKeyVal(sib_size - 1, sib_inner_page, false);
  }
  return call_back_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowSibRight(BPlusTreePage *sib_page, BPlusTreePage *poor_page, bool is_leaf, const KeyType &key)
    -> KeyType {
  int poor_size = poor_page->GetSize();
  KeyType call_back_key;
  if (is_leaf) {
    auto sib_leaf_page = static_cast<LeafPage *>(sib_page);
    auto poor_leaf_page = static_cast<LeafPage *>(poor_page);
    // 拿走右兄弟最小的键值
    auto val = sib_leaf_page->ValueAt(0);
    // 返回右兄弟之后的第一个键
    call_back_key = sib_leaf_page->KeyAt(1);
    InsertLeafPageNode(poor_size, sib_leaf_page->KeyAt(0), val, poor_leaf_page);
    DeleteSpeciKeyVal(0, sib_leaf_page, true);
  } else {
    auto sib_inner_page = static_cast<InternalPage *>(sib_page);
    auto poor_inner_page = static_cast<InternalPage *>(poor_page);
    // 保证inner_page 的第一个哨兵键不能被访问
    // 拿走右兄弟最小的值
    auto val = sib_inner_page->ValueAt(0);
    // 拿走右兄弟的第一个有效键
    call_back_key = sib_inner_page->KeyAt(1);
    InsertInnerPageNode(poor_size, key, val, poor_inner_page);
    DeleteSpeciKeyVal(0, sib_inner_page, false);
  }
  return call_back_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Redistribute(InternalPage *parent, int child_index, bool is_leaf, BPlusTreePage *op_page) -> bool {
  // 向左借简而言之就是 把左边兄弟的最大节点值借走放到当前节点值的第一个位置
  // 左兄弟的父节点键不变 当前节点的父节点键就必须更新为左节点借走的键
  // 对于当前节点的第一个键 如果是内部节点则为之前父节点对应当前节点的键
  // 否则第一个键为借走的左兄弟最大的键
  auto redistribute_left = [&, this](bool is_leaf) -> bool {
    // 判断是否有左兄弟
    if (child_index - 1 >= 0) {
      page_id_t lsib_pgid = parent->ValueAt(child_index - 1);
      auto left_sib_guard = bpm_->WritePage(lsib_pgid);
      auto left_sib_page = left_sib_guard.template AsMut<BPlusTreePage>();
      // 确认左兄弟满足要求
      if (left_sib_page->GetSize() >= left_sib_page->GetMinSize() + 1) {
        KeyType parent_key = parent->KeyAt(child_index);
        KeyType borrow_key = BorrowSibLeft(left_sib_page, op_page, is_leaf, parent_key);
        parent->SetKeyAt(child_index, borrow_key);
        return true;
      }
    }
    return false;
  };

  // 向右借简而言之就是 把右边兄弟的最小节点值借走放到当前节点值的最后一个位置
  // 那么对应的右兄弟的父节点键变为右兄弟的第一个有效键
  // 当前节点如果是内部节点 则最后一个键就必须更新为之前的右兄弟父节点对应键
  // 如果当前节点是叶子节点 则最后一个键为借走的右兄弟的键
  auto redistribute_right = [&, this](bool is_leaf) -> bool {
    // 判断是否有右兄弟
    if (child_index + 1 < parent->GetSize()) {
      // 先找到右兄弟
      page_id_t rsib_pgid = parent->ValueAt(child_index + 1);
      auto right_sib_guard = bpm_->WritePage(rsib_pgid);
      auto right_sib_page = right_sib_guard.template AsMut<BPlusTreePage>();
      // 判断右兄弟是否满足要求
      if (right_sib_page->GetSize() >= right_sib_page->GetMinSize() + 1) {
        KeyType parent_key = parent->KeyAt(child_index + 1);
        KeyType borrow_key = BorrowSibRight(right_sib_page, op_page, is_leaf, parent_key);
        parent->SetKeyAt(child_index + 1, borrow_key);
        return true;
      }
    }
    return false;
  };
  return (redistribute_left(is_leaf) || redistribute_right(is_leaf));
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MoveToLeft(BPlusTreePage *sib_page, BPlusTreePage *cur_page, bool is_leaf, KeyType &key) {
  int cur_page_size = cur_page->GetSize();
  int sib_page_size = sib_page->GetSize();
  // 根据是否为叶子区分
  // 把当前节点键值全部移动到左节点
  if (is_leaf) {
    auto leaf_sib_page = static_cast<LeafPage *>(sib_page);
    auto org_page = static_cast<LeafPage *>(cur_page);
    for (int i = 0; i < cur_page_size; i++) {
      InsertLeafPageNode(sib_page_size + i, org_page->KeyAt(i), org_page->ValueAt(i), leaf_sib_page);
    }
    // 左节点接收当前节点的右指针
    leaf_sib_page->SetNextPageId(org_page->GetNextPageId());
  } else {
    auto inner_sib_page = static_cast<InternalPage *>(sib_page);
    auto org_page = static_cast<InternalPage *>(cur_page);
    for (int i = 0; i < cur_page_size; i++) {
      KeyType insert_key = (i == 0 ? key : org_page->KeyAt(i));
      InsertInnerPageNode(sib_page_size + i, insert_key, org_page->ValueAt(i), inner_sib_page);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeNode(InternalPage *parent, int child_index, bool is_leaf, BPlusTreePage *op_page)
    -> page_id_t {
  // 向左合并过程：把当前节点的所有值移动到左兄弟
  // 父节点删除当前节点的对应键
  // 如果是内部节点左移 则需要把父亲节点对应当前的键移动到左兄弟
  // 如果是叶子节点 则直接把当前所有键也移动过去
  int cur_size = op_page->GetSize();
  page_id_t res_pgid = INVALID_PAGE_ID;
  auto merge_left = [&, this](bool is_leaf) -> bool {
    if (child_index - 1 >= 0) {
      // 先找到左兄弟
      page_id_t lsib_pgid = parent->ValueAt(child_index - 1);
      auto left_sib_guard = bpm_->WritePage(lsib_pgid);
      auto left_sib_page = left_sib_guard.template AsMut<BPlusTreePage>();

      if (left_sib_page->GetSize() + cur_size <= op_page->GetMaxSize()) {
        KeyType parent_key = parent->KeyAt(child_index);
        MoveToLeft(left_sib_page, op_page, is_leaf, parent_key);
        // 删除父节点的相关键值 父节点一定不为leaf
        DeleteSpeciKeyVal(child_index, parent, false);
        return true;
      }
    }
    return false;
  };

  // 和上述的当前节点左合并一致
  // 向右合并的方法就是把右兄弟所有值移动到当前节点
  // 移动完成后父节点先删除右兄弟在父节点的对应键
  // 如果是内部节点则需要把右兄弟对应父节点的键移动到当前节点尾
  // 叶子节点直接移动就行
  auto merge_right = [&, this](bool is_leaf) -> bool {
    if (child_index + 1 < parent->GetSize()) {
      page_id_t rsib_pgid = parent->ValueAt(child_index + 1);
      auto right_sib_guard = bpm_->WritePage(rsib_pgid);
      auto right_page = right_sib_guard.template AsMut<BPlusTreePage>();

      if (right_page->GetSize() + cur_size <= op_page->GetMaxSize()) {
        KeyType parent_key = parent->KeyAt(child_index + 1);
        // 直接反着来就行
        MoveToLeft(op_page, right_page, is_leaf, parent_key);
        DeleteSpeciKeyVal(child_index + 1, parent, false);
        res_pgid = right_sib_guard.GetPageId();
        return true;
      }
    }
    return false;
  };

  if (merge_left(is_leaf)) {
  } else {
    merge_right(is_leaf);
  }

  return res_pgid;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
