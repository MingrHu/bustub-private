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
#include "common/config.h"
#include "storage/index/b_plus_tree_debug.h"
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
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
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
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  if(root_page->root_page_id_ == INVALID_PAGE_ID){
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

  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_pgid = guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  if(root_pgid == INVALID_PAGE_ID){
    return false;
  }
  ctx.root_page_id_ = root_pgid;
  // 先拿到根节点
  ctx.read_set_.push_back(bpm_->ReadPage(ctx.root_page_id_));
  guard.Drop();
  int index = -1;

  while(!ctx.read_set_.empty()){
    // 获取页面 并转为B+树页面
    auto guard = std::move(ctx.read_set_.front());
    // 待操作页面
    auto op_page = guard.As<BPlusTreePage>();

    if(op_page->IsLeafPage()){
      const auto leaf_page = static_cast<const LeafPage*>(op_page);
      index = KeyBinarySearch(key, leaf_page,true);
      // 没找到
      if(index == -1){
        ctx.read_set_.pop_front();
      }
      break;
    }
    
    const auto inner_page = static_cast<const InternalPage*>(op_page);
    // inner_page 查找的要特殊一些 
    index = KeyBinarySearch(key, inner_page,false);
    auto pgid = inner_page->ValueIndex(index);
    // 放入下一个值的页面
    ctx.read_set_.emplace_back(bpm_->ReadPage(pgid));
    ctx.read_set_.pop_front();
  }
  // 没找到
  if(ctx.read_set_.empty()){
    return false;
  }

  ReadPageGuard res_guard = std::move(ctx.read_set_.front());
  auto target_page = res_guard.As<LeafPage>();
  result->push_back(target_page->ValueAt(index));
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
    auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
    if(header_page->root_page_id_ == INVALID_PAGE_ID){
      header_page_guard.Drop();
      auto header_write_page_guard = bpm_->WritePage(header_page_id_);
      auto header_write_page = header_write_page_guard.AsMut<BPlusTreeHeaderPage>();
      if(header_write_page->root_page_id_ == INVALID_PAGE_ID){
        // 获取新的页 并设置该页的叶子节点值
        auto new_pgid = bpm_->NewPage();
        // 初始化根节点
        auto write_guard = bpm_->WritePage(new_pgid);
        auto leaf_page = write_guard.AsMut<LeafPage>();
        // 更新根节点
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
  // 拿到子节点的锁
  auto header_page_guard = bpm_->ReadPage(header_page_id_);
  ctx.root_page_id_ = header_page_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  ctx.read_set_.push_back(bpm_->ReadPage(ctx.root_page_id_));
  header_page_guard.Drop();

  // 尝试读锁去寻找叶子节点
  while(!ctx.read_set_.front().As<BPlusTreePage>()->IsLeafPage()){
    auto cur_page = ctx.read_set_.front().As<BPlusTreePage>();
    auto parent_inner_page = static_cast<const InternalPage*>(cur_page);
    int index = KeyBinarySearch(key, parent_inner_page,false);
    // 拿到子节点
    ctx.read_set_.push_back(bpm_->ReadPage(parent_inner_page->ValueAt(index)));
    // 释放刚刚的锁
    ctx.read_set_.pop_front();
  }

  // 现在只有叶子节点了
  auto leaf_pgid = ctx.read_set_.front().GetPageId();
  ctx.read_set_.pop_front();
  // 对待操作的叶子节点加写锁
  ctx.write_set_.push_back(bpm_->WritePage(leaf_pgid));
  auto waited_leaf_page = ctx.write_set_.front().AsMut<LeafPage>();
  if(ParentIsSafe(waited_leaf_page, true)){
    int insert_pos = LowerBound(key, waited_leaf_page,true);
    // 不能重复
    if(comparator_(waited_leaf_page->KeyAt(insert_pos),key) == 0){
      return false;
    }
    InsertLeafPageNode(insert_pos, key, value, waited_leaf_page);
    return true;
  }

  // 乐观和悲观的策略上 只要你采用的是悲观策略 就必须全程持有锁 
  // 直到当前节点的父节点安全时才能释放之前的锁 否则不能释放
  ctx.write_set_.clear();
  ctx.root_page_id_ = bpm_->ReadPage(header_page_id_).As<BPlusTreeHeaderPage>()->root_page_id_;
  ctx.write_set_.push_back(bpm_->WritePage(ctx.root_page_id_));

  // 现在write里面有header和root
  // 执行悲观查找
  while(!ctx.write_set_.back().As<BPlusTreePage>()->IsLeafPage()){
    // 当前节点就是父节点
    auto cur_page = ctx.write_set_.back().As<InternalPage>();
    // 获取子节点信息
    int index = KeyBinarySearch(key, cur_page, false);
    page_id_t next_pgid = cur_page->ValueAt(index);
    auto next_page_guard = bpm_->WritePage(next_pgid);
    auto next_page = next_page_guard.As<BPlusTreePage>();
    
    // 判断子节点是否可能发生分裂 
    if(ParentIsSafe(next_page, true)){
      ctx.write_set_.clear();
    }

    // 记录内部节点的索引信息
    ctx.indexs_store_.push(index);
    ctx.write_set_.emplace_back(std::move(next_page_guard));
  }

  // 检查当前叶子节点的父亲是否安全
  if(ParentIsSafe(ctx.write_set_.back().As<LeafPage>(), true)){
    auto leaf_page_guard = std::move(ctx.write_set_.back());
    auto leaf_page = leaf_page_guard.AsMut<LeafPage>();
    int insert_pos = LowerBound(key, leaf_page,true);
    if(comparator_(leaf_page->KeyAt(insert_pos),key) == 0){
      return false;
    }
    InsertLeafPageNode(insert_pos, key, value,leaf_page);
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
  // leaf_page->GetSize() >> 1: 至少保留一位 (inner_page->GetSize() >> 1) + 1 保证原节点哨兵位置不改变

  // 1 拿到待分裂节点和插入位置信息
  auto o_leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
  int insert_pos = LowerBound(key,o_leaf_page,true);
  if(comparator_(o_leaf_page->KeyAt(insert_pos),key) == 0){
    return false;
  }
  // 2 先执行插入
  InsertLeafPageNode(insert_pos, key, value,ctx.write_set_.back().AsMut<LeafPage>());
  // 3 保存需要往上传递的分裂信息
  int split_pos = o_leaf_page->GetSize() >> 1;
  KeyType split_key = o_leaf_page->KeyAt(split_pos);
  page_id_t left_pgid = ctx.write_set_.back().GetPageId();
  page_id_t right_pgid = bpm_->NewPage();

  // 4 获取新的节点以把当前节点的一半移动过去
  auto new_leaf_guard = bpm_->WritePage(right_pgid);
  auto new_leaf_page = new_leaf_guard.AsMut<LeafPage>();

  // 5 执行分裂操作并释放锁
  SplitPage(split_pos, right_pgid, o_leaf_page, new_leaf_page,true);   
  new_leaf_guard.Drop(); 
  ctx.write_set_.pop_back();
  bool new_root_page = true;

  // 孩子节点已经分裂了 目前处理的是父节点的问题
  // 只要不空 就说明当前父节点都会受影响
  // 但不一定父节点也会分裂 需要判断
  while(!ctx.write_set_.empty()){
    // 拿到当前父节点信息
    auto parent_page = ctx.write_set_.back().AsMut<InternalPage>();
    left_pgid = ctx.write_set_.back().GetPageId();
    insert_pos = ctx.indexs_store_.top() + 1;
    ctx.indexs_store_.pop();

    // 先插入子节点传递的分裂点键值 使得当前父节点键值数量最大
    InsertInnerPageNode(insert_pos, split_key, right_pgid, parent_page);
    // 插如果入节点后小于 最大值数量 -1 则不需要分裂
    if(parent_page->GetSize() <= internal_max_size_ - 1){
      new_root_page = false;
      break;
    }
    // 否则需要进行划分
    // 划分分裂点
    split_pos = (parent_page->GetSize() >> 1) + 1;
    split_key = parent_page->KeyAt(split_pos);
    right_pgid = bpm_->NewPage();
    
    auto new_page_guard = bpm_->WritePage(right_pgid);
    auto new_inner_page = new_leaf_guard.AsMut<InternalPage>();
    page_id_t splitval = parent_page->ValueAt(split_pos);

    SplitPage(split_pos, right_pgid, parent_page, new_inner_page,false); 
    // 新建的内部节点哨兵需要有值 直接把分裂点的值给它
    new_inner_page->SetValueAt(0,splitval);  
    ctx.write_set_.pop_back();
  }
  // 如果会影响到根节点
  if(new_root_page){
    // 开始新建根节点并设置值
    auto new_root_pgid = bpm_->NewPage();
    auto new_root_page_guard = bpm_->WritePage(new_root_pgid);
    auto root_page = new_root_page_guard.AsMut<InternalPage>();
    root_page->Init(internal_max_size_);
    // 这里必须手动设置值 因为不需要插入任何键
    root_page->SetValueAt(0,left_pgid);
    InsertInnerPageNode(1, split_key, right_pgid, root_page);

    auto h_page_guard =bpm_->WritePage(header_page_id_); 
    ctx.write_set_.clear();
    auto header_page = h_page_guard.AsMut<BPlusTreeHeaderPage>();
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
    auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
    if(header_page->root_page_id_ == INVALID_PAGE_ID){
      return;
    }
  }

  // Declaration of context instance.
  Context ctx;
  // 乐观锁 一路读锁
  auto header_page_guard = bpm_->ReadPage(header_page_id_);
  ctx.root_page_id_ = header_page_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  ctx.read_set_.push_back(bpm_->ReadPage(ctx.root_page_id_));
  header_page_guard.Drop();

  // 尝试读锁去寻找叶子节点
  while(!ctx.read_set_.front().As<BPlusTreePage>()->IsLeafPage()){
    auto cur_page = ctx.read_set_.front().As<BPlusTreePage>();
    auto parent_inner_page = static_cast<const InternalPage*>(cur_page);
    int index = KeyBinarySearch(key, parent_inner_page,false);
    // 拿到子节点
    ctx.read_set_.push_back(bpm_->ReadPage(parent_inner_page->ValueAt(index)));
    // 释放刚刚的锁
    ctx.read_set_.pop_front();
  }

  
  auto leaf_pgid = ctx.read_set_.front().GetPageId();
  ctx.read_set_.pop_front();

  // 叶子节点页面加锁
  ctx.write_set_.push_back(bpm_->WritePage(leaf_pgid));
  auto waited_leaf_page = ctx.write_set_.front().AsMut<LeafPage>();
  if(ParentIsSafe(waited_leaf_page, false)){
    int index = KeyBinarySearch(key, waited_leaf_page, true);
    if(index == -1){
      return;
    }
    // 删除这个键值
    DeleteSpeciKeyVal(index, waited_leaf_page);
    return;
  }

  // 不安全则采用悲观锁的方式
  ctx.write_set_.clear();
  ctx.root_page_id_ = bpm_->ReadPage(header_page_id_).As<BPlusTreeHeaderPage>()->root_page_id_;
  ctx.write_set_.push_back(bpm_->WritePage(ctx.root_page_id_));
  // 目前包含根节点
  while(!ctx.write_set_.back().AsMut<BPlusTreePage>()->IsLeafPage()){
    auto cur_page = ctx.write_set_.back().As<InternalPage>();
    int index = KeyBinarySearch(key, cur_page, false);
    page_id_t next_pgid = cur_page->ValueAt(index);
    auto next_page_guard = bpm_->WritePage(next_pgid);
    auto next_page = next_page_guard.As<BPlusTreePage>();
    if(ParentIsSafe(next_page, false)){
      ctx.write_set_.clear();
    }
    ctx.indexs_store_.push(index);
    ctx.write_set_.emplace_back(std::move(next_page_guard));
  }

  // 现在最后一个write_set是叶子节点
  int dl_pos = KeyBinarySearch(key, ctx.write_set_.back().AsMut<LeafPage>(), true);
  if(dl_pos == -1){
    return;
  }

  if(ParentIsSafe(ctx.write_set_.back().AsMut<LeafPage>(), false)){
    DeleteSpeciKeyVal(dl_pos, waited_leaf_page);
    return;
  }

  // 叶子节点会发生借用或者合并 
  auto cur_page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto cur_page = cur_page_guard.AsMut<LeafPage>();
  // 先进行删除操作
  DeleteSpeciKeyVal(dl_pos, cur_page);

  if(cur_page_guard.GetPageId() == ctx.root_page_id_){
    AdjustRoot(cur_page);
    auto dl_pgid = cur_page_guard.GetPageId();
    cur_page_guard.Drop();
    bpm_->DeletePage(dl_pgid);
    return;
  }

  // 更新next_pgid
  int leaf_page_index = ctx.write_set_.back().As<LeafPage>()->;

  // auto rsibpgid = INVALID_PAGE_ID;
  // auto lsibpgid = INVALID_PAGE_ID;
  // bool op_new_root = true;
  // while(!ctx.write_set_.empty()){
  //   auto parent_page = ctx.write_set_.back().AsMut<InternalPage>();
  //   int pre_pos = ctx.indexs_store_.top();
  //   ctx.indexs_store_.pop();
  //   // 左借用 右借用 左合并 右合并


  // }


    

  
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { 

  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

/**
 * @brief Other helper function!
 * @return Page id of the root of this tree
 *
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::KeyBinarySearch(const KeyType& key,const BPlusTreePage* page,bool is_leaf)->int{
  
  int l = 0;
  int r = page->GetSize() - 1;
  auto mid = l + (r - l) / 2;

  if(is_leaf){
    auto leaf_page = static_cast<const LeafPage*>(page);
    while(l <= r){
      // mid < target
      if(comparator_(leaf_page->KeyAt(mid),key) == - 1){
        l = mid + 1;
      }
      // mid > target
      else if(comparator_(leaf_page->KeyAt(mid),key) == 1){
          r = mid - 1;
      }
      else {
        return mid;
      }
      mid = l + (r - l) / 2;
    }
    return - 1;    
  }

  // 查找内部节点
  l = 1,r = page->GetSize() - 1,mid = l + (r - l) / 2;
  auto inner_page = static_cast<const InternalPage*>(page);

  // 比第一个有效键值小
  if(comparator_(inner_page->KeyAt(l),key) == 1){
    return 0;
  }

  // 找到第一个大于等于key的节点
  while(l <= r){
    if(comparator_(inner_page->KeyAt(mid),key) == -1){
      l = mid + 1;
    } 
    else {
      r = mid - 1;
    }
    mid = l + (r - l) / 2;
  }
  if(l != inner_page->GetSize()  && comparator_(key,inner_page->KeyAt(l)) == 0){
    return l;
  }
  return l - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LowerBound(const KeyType& key,const BPlusTreePage* target_page,bool is_leaf)->int{
  int res = -1;
  if(is_leaf){
    auto leaf_page = static_cast<const LeafPage*>(target_page);
    int l = 0;
    int r = leaf_page->GetSize() - 1;
    auto mid = l + (r - l) / 2;
    while(l <= r){
      // mid < target
      if(comparator_(leaf_page->KeyAt(mid),key) == - 1){
        l = mid + 1;
      }
      // mid >= target
      else{
        r = mid - 1;
      }
      mid = l + (r - l) / 2;
    }
    res = l;
  }
  else{
    auto inner_page = static_cast<const InternalPage*>(target_page);
    int l = 1;
    int r = inner_page->GetSize() - 1;
    auto mid = l + (r - l) / 2;
    while(l <= r){
      // mid < target
      if(comparator_(inner_page->KeyAt(mid),key) == - 1){
        l = mid + 1;
      }
      // mid >= target
      else{
        r = mid - 1;
      }
      mid = l + (r - l) / 2;
    }
    res = l;
  }
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertLeafPageNode(int insert_pos,const KeyType& key,const ValueType& val,LeafPage* leaf_page){
  
  int org_size = leaf_page->GetSize();
  leaf_page->ChangeSizeBy(1);
  for(int i = insert_pos;i < org_size;i++){
    // 键和值全部往后移动一位
    leaf_page->SetKeyAt(i+1, leaf_page->KeyAt(i));
    leaf_page->SetValueAt(i+1, leaf_page->ValueAt(i));
  }
  leaf_page->SetKeyAt(insert_pos, key);
  leaf_page->SetValueAt(insert_pos, val);
  
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInnerPageNode(int insert_pos,const KeyType& key,const page_id_t& val,InternalPage* inner_page){

  int org_size = inner_page->GetSize();
  inner_page->ChangeSizeBy(1);
  for(int i = insert_pos;i < org_size ;i++){
    // 哨兵键不能直接移动
    if(i > 0){
      inner_page->SetKeyAt(i+1, inner_page->KeyAt(i));
    }
    inner_page->SetValueAt(i+1, inner_page->ValueAt(i));
  }
  // 单独添加第一个合法键
  if(insert_pos > 0){
    inner_page->SetKeyAt(insert_pos, key);
  }
  else{
    inner_page->SetKeyAt(insert_pos + 1, key);
  }
  
  inner_page->SetValueAt(insert_pos, val);
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitPage(int split_pos,int new_pgid,BPlusTreePage* origional_page,BPlusTreePage* new_page,bool is_leaf){
  // 针对叶子节点
  if(is_leaf){
    auto org_leaf_page = static_cast<LeafPage*>(origional_page);
    auto new_leaf_page = static_cast<LeafPage*>(new_page);
    // 初始化新页面
    new_leaf_page->Init(leaf_max_size_);
    // 更新相关元信息
    new_leaf_page->SetNextPageId(org_leaf_page->GetNextPageId());
    org_leaf_page->SetNextPageId(new_pgid);
    // 将当前节点的分割点及以后键值复制到新的节点里面
    for(int i = split_pos;i <leaf_max_size_;i++){
      InsertLeafPageNode(i - split_pos, org_leaf_page->KeyAt(i), 
      org_leaf_page->ValueAt(i), new_leaf_page);
    }
    org_leaf_page->SetSize(split_pos);
  }
  else{
    // inner分裂的时候不需要保存split_key
    auto org_inner_page = static_cast<InternalPage*>(origional_page);
    auto new_inner_page = static_cast<InternalPage*>(new_page);
    // 初始化新页面
    new_inner_page->Init(internal_max_size_);
    // 将当前节点的分割点及以后键值复制到新的节点里面
    // inner比较特殊 必须从1开始 0是哨兵节点
    for(int i = split_pos + 1;i <internal_max_size_;i++){
      InsertInnerPageNode(i - split_pos, org_inner_page->KeyAt(i), 
      org_inner_page->ValueAt(i), new_inner_page);
    }
    // 原节点丢弃分割点
    org_inner_page->SetSize(split_pos);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteSpeciKeyVal(int delete_pos,LeafPage* leaf_page){
  for(int i = delete_pos;i < leaf_page->GetSize() - 1;i++){
    leaf_page->SetKeyAt(i, leaf_page->KeyAt(i+1));
    leaf_page->SetValueAt(i, leaf_page->ValueAt(i+1));
  }
  leaf_page->ChangeSizeBy(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ParentIsSafe(const BPlusTreePage* cur_page,bool is_insert)->bool{
  if(is_insert){
    return (cur_page->GetSize() < cur_page->GetMaxSize() - 1);
  }  
  return cur_page->GetSize() > cur_page->GetMinSize();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage* old_root_page){
  if(!old_root_page->IsLeafPage() && old_root_page->GetSize() == 1){
    auto root_page = static_cast<InternalPage*>(old_root_page);
    page_id_t only_child = root_page->ValueAt(0);
    auto header_page_guard = bpm_->WritePage(header_page_id_);
    auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = only_child;
  }
  else if(old_root_page->IsLeafPage() && old_root_page->GetSize() == 0){
    auto header_page_guard = bpm_->WritePage(header_page_id_);
    auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = INVALID_PAGE_ID;
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowSibLeft(BPlusTreePage* sib_page,BPlusTreePage* poor_page,bool is_leaf,const KeyType &key)->KeyType{
  int sib_size = sib_page->GetSize();
  KeyType left_sib_key;
  if(is_leaf){
    auto sib_leaf_page = static_cast<LeafPage*>(sib_page);
    auto poor_leaf_page = static_cast<LeafPage*>(poor_page);
    // 拿走左兄弟最大的值
    left_sib_key = sib_leaf_page->KeyAt(sib_size - 1);
    auto val = sib_leaf_page->ValueAt(sib_size - 1);
    InsertLeafPageNode(0, key, val, poor_leaf_page);
  }
  else{
    auto sib_inner_page = static_cast<InternalPage*>(sib_page);
    auto poor_inner_page = static_cast<InternalPage*>(poor_page);
    // 保证inner_page 的第一个哨兵键不能被访问
    // 拿走左兄弟最大的键值
    left_sib_key = sib_inner_page->KeyAt(sib_size - 1);
    auto val = sib_inner_page->ValueAt(sib_size - 1); 
    InsertInnerPageNode(0, key, val, poor_inner_page);
  }
  sib_page->ChangeSizeBy(-1);
  return left_sib_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowSibRight(BPlusTreePage* sib_page,BPlusTreePage* poor_page,bool is_leaf,const KeyType &key)->KeyType{
  int poor_size = poor_page->GetSize();
  int org_size = sib_page->GetSize();
  KeyType right_sib_key;
  if(is_leaf){
    auto sib_leaf_page = static_cast<LeafPage*>(sib_page);
    auto poor_leaf_page = static_cast<LeafPage*>(poor_page);
    // 拿走右兄弟最小的值
    auto val = sib_leaf_page->ValueAt(0);
    right_sib_key = sib_leaf_page->KeyAt(0);
    InsertLeafPageNode(poor_size, key, val, poor_leaf_page);
    // 叶子节点直接两个一块往左移
    for(int i = 0;i < org_size - 1;i++){
      sib_leaf_page->SetValueAt(i,sib_leaf_page->ValueAt(i + 1));
      sib_leaf_page->SetKeyAt(i,sib_leaf_page->KeyAt(i + 1));
    }
  }
  else{
    auto sib_inner_page = static_cast<InternalPage*>(sib_page);
    auto poor_inner_page = static_cast<InternalPage*>(poor_page);
    // 保证inner_page 的第一个哨兵键不能被访问
    // 拿走右兄弟最小的值
    auto val = sib_inner_page->ValueAt(0); 
    right_sib_key = sib_inner_page->KeyAt(1);
    
    InsertInnerPageNode(poor_size, key, val, poor_inner_page);
    // 叶子节点直接两个一块往左移
    for(int i = 0;i < org_size - 1;i++){
      sib_inner_page->SetValueAt(i,sib_inner_page->ValueAt(i + 1));
      if(i > 0){
        sib_inner_page->SetKeyAt(i,sib_inner_page->KeyAt(i + 1));
      }
    }
  }
  sib_page->ChangeSizeBy(-1);
  return right_sib_key;
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Redistribute(InternalPage* parent,int child_index,bool is_leaf,BPlusTreePage* child_page)->bool{
 
  // 向左借简而言之就是 把左边兄弟的最大节点值借走放到当前节点值的第一个位置
  // 左兄弟的父节点键不变 当前节点的父节点键就必须更新为左节点借走的键
  // 对于当前节点的第一个键 则为之前父节点对应当前节点的键
  auto redistribute_left = [&,this](bool is_leaf)->bool{
    // 判断是否有左兄弟
    if(child_index - 1 >= 0){
      page_id_t lsib_pgid = parent->ValueAt(child_index - 1);
      auto left_sib_guard = bpm_->WritePage(lsib_pgid);
      auto left_sib_page = left_sib_guard.AsMut<BPlusTreePage>();
      // 确认左兄弟满足要求
      if(left_sib_page->GetSize() >= left_sib_page->GetMinSize() + 1){

        KeyType parent_key = parent->KeyAt(child_index);
        KeyType borrow_key = BorrowSibLeft(left_sib_page, child_page, is_leaf,parent_key);
        parent->SetKeyAt(child_index, borrow_key);
        return true;
      }
    }
    return false;
  };

  // 向右借简而言之就是 把右边兄弟的最小节点值借走放到当前节点值的最后一个位置
  // 那么对应的右兄弟的父节点键变为右兄弟的第一个有效键 
  // 当前节点的最后一个键就必须更新为之前的右兄弟父节点对应键
  auto redistribute_right = [&,this](bool is_leaf)->bool{
    // 判断是否有右兄弟
    if(child_index + 1 < parent->GetSize()){
      // 先找到右兄弟
      page_id_t rsib_pgid = parent->ValueAt(child_index + 1);
      auto right_sib_guard = bpm_->WritePage(rsib_pgid);
      auto right_sib_page = right_sib_guard.AsMut<BPlusTreePage>();
      if(right_sib_page->GetSize() >= right_sib_page->GetMinSize() + 1){
        
        KeyType parent_key = parent->KeyAt(child_index + 1);
        KeyType borrow_key = BorrowSibRight(right_sib_page, child_page, is_leaf, parent_key);
        parent->SetKeyAt(child_index + 1, borrow_key);
        return true;
      }
    }
    return false;
  };

  return (redistribute_left(is_leaf) || redistribute_right(is_leaf));
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
