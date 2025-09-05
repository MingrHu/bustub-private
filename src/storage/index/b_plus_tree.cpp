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
        InsertLeafPageNode(0, key, value, leaf_page,true);
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
    InsertLeafPageNode(insert_pos, key, value, waited_leaf_page,false);
    return true;
  }

  // 乐观和悲观的策略上 只要你采用的是悲观策略 就必须全程持有锁 
  // 直到当前节点的父节点安全时才能释放之前的锁 否则不能释放
  ctx.write_set_.clear();
  ctx.write_set_.push_back(bpm_->WritePage(header_page_id_));
  ctx.root_page_id_ = ctx.write_set_.back().As<BPlusTreeHeaderPage>()->root_page_id_;
  ctx.write_set_.push_back(bpm_->WritePage(ctx.root_page_id_));
  // 默认是包含header节点的
  bool include_header = true;

  // 现在write里面有header和root
  // 执行悲观查找
  while(!ctx.write_set_.back().As<BPlusTreePage>()->IsLeafPage()){
    // 这里需要把最新的拿出来然后再放回
    auto cur_page_guard = std::move(ctx.write_set_.back());
    auto cur_page = cur_page_guard.As<InternalPage>();
    ctx.write_set_.pop_back();
    if(ParentIsSafe(cur_page, true)){
      include_header = false;
      ctx.write_set_.clear();
    }
    int index = KeyBinarySearch(key, cur_page, false);
    auto next_page_guard = bpm_->WritePage(cur_page->ValueAt(index));
    // 记录内部节点的索引信息
    ctx.indexs_store_.push(index);
    ctx.write_set_.emplace_back(std::move(cur_page_guard));
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
    InsertLeafPageNode(insert_pos, key, value,leaf_page,false);
    return true;
  }
  // 执行分裂
  else{
    auto leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
    int insert_pos = LowerBound(key,leaf_page,true);
    if(comparator_(leaf_page->KeyAt(insert_pos),key) == 0){
      return false;
    }
    InsertLeafPageNode(insert_pos, key, value,ctx.write_set_.back().AsMut<LeafPage>(),false);
    // 先分裂叶子节点
    int split_pos = leaf_page->GetSize() >> 1;
    KeyType insert_key = leaf_page->KeyAt(split_pos);
    page_id_t new_pgid = bpm_->NewPage();

    auto new_leaf_guard = bpm_->WritePage(new_pgid);
    auto new_leaf_page = new_leaf_guard.AsMut<LeafPage>();
    new_leaf_page->Init(leaf_max_size_);

    SplitPage(split_pos, new_pgid, leaf_page, new_leaf_page,true);    
    ctx.write_set_.pop_back();

    // 分裂会影响到根节点 意味着write_set里面包含header页面
    // 当前分裂孩子页面的一定是满的
    if(include_header){
      // 先声明分裂点和分裂键
      while(ctx.write_set_.size() > 1){
        auto child_page = ctx.write_set_.back().AsMut<InternalPage>();
        insert_pos = ctx.indexs_store_.top() + 1;
        ctx.indexs_store_.pop();
        InsertInnerPageNode(insert_pos, insert_key, new_pgid, child_page, false);
        split_pos = (child_page->GetSize() >> 1) + 1;

        
      }

    }
    else{
      
    }

  }


  if(leaf_page->GetSize() < leaf_max_size_){
    int insert_index = LowerBound(key, leaf_page);
    // 重复键
    if(insert_index != leaf_page->GetSize() && comparator_(key,leaf_page->KeyAt(insert_index)) == 0){
      ctx.write_set_.pop_back();
      return false;
    }
    // 插入值
    InsertLeafPageNode(insert_index, key, value, leaf_page,false);

    // 如果叶子节点需要分裂
    // 分裂的流程是：当前节点加锁 当前节点分裂 保存向上传递的信息 当前节点解锁 寻找父节点/创建父节点 加锁完成插入
    // 如果又发生分裂 则重复上述流程
    // 向上传递分两种情况：
    // 1.没有父节点了 得自己手动创建一个父节点 所以需要分裂的两部分都传递上去 
    // 2.存在父节点 那么左半部分和父节点关系不改变 右半部分和父节点重新建立关系 
    // 且需要分割点传递到父节点中 父节点直接将分割点放在原来的索引位置后一位
    // leaf_max_size_ >> 1: 至少保留一位
    if(leaf_page->GetSize() == leaf_max_size_){
      int split_pos = leaf_max_size_ >> 1;
      // 创建新页
      auto new_pgid = bpm_->NewPage();
      auto new_write_guard = bpm_->WritePage(new_pgid);
      auto new_page = new_write_guard.AsMut<LeafPage>();
      SplitPage(split_pos, new_pgid, leaf_page, new_page,true);
      // 需要向上传递的键和值
      KeyType split_key = leaf_page->KeyAt(split_pos);
      page_id_t right_pgid = new_pgid; 
      page_id_t left_pgid = ctx.write_set_.front().GetPageId();
      auto parent_pgid = INVALID_PAGE_ID;
      // 只要完成了分裂并且拿到当前的信息就释放锁
      ctx.write_set_.pop_front();

      // 对叶节点分裂传递预处理
      // 如果只有叶子节点 则必须再创建新的父节点
      if(ctx.read_set_.empty()){
        parent_pgid = bpm_->NewPage();
        DeliverToHead(parent_pgid,ctx,left_pgid,right_pgid,split_key);
      }
      // 否则存在父节点 则当前节点信息上传到父节点 并获取当前父节点的ID
      else{
        parent_pgid = DeliverToInnerPage(ctx, right_pgid, split_key);
      }

      // 对父节点进行检查 查看键是否满了
      while(CheckParentPage(parent_pgid)){
        auto parent_page_guard = bpm_->WritePage(parent_pgid);
        auto parent_page = parent_page_guard.AsMut<InternalPage>();
        // 记录往上传递的信息
        split_pos = (internal_max_size_ >> 1) + 1;
        split_key = parent_page->KeyAt(split_pos);
        left_pgid = parent_pgid;
        right_pgid = bpm_->NewPage();
        // 开始分裂
        auto brother_page_guard = bpm_->WritePage(right_pgid);
        auto brother_page = brother_page_guard.AsMut<InternalPage>();
        SplitPage(split_pos, right_pgid, parent_page, brother_page,false);
        brother_page_guard.Drop();
        parent_page_guard.Drop();
        // 分裂完毕 准备传递
        if(ctx.path_.empty()){
          DeliverToHead(parent_pgid,ctx,left_pgid,right_pgid,split_key);
          break;
        }
        // 当前节点信息上传 并获取当前父节点的ID
        parent_pgid = DeliverToInnerPage(ctx, right_pgid, split_key);
      }
    }
    else{
      // 释放叶子页面
      ctx.write_set_.pop_front();
    }
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
  // Declaration of context instance.
  Context ctx;
  auto header_page_guard = bpm_->ReadPage(header_page_id_);
  ctx.read_set_.emplace_back(std::move(header_page_guard));
  auto op_page = header_page_guard.As<BPlusTreeHeaderPage>();
  // 判空
  if(op_page->root_page_id_ == INVALID_PAGE_ID){
    return;
  }
  auto root_pgid = op_page->root_page_id_;
  ctx.read_set_.pop_front();
  ctx.read_set_.push_back(bpm_->ReadPage(root_pgid));

  auto cur_page = ctx.read_set_.back().As<BPlusTreePage>();
  auto next_pgid = root_pgid;

  // 尝试读锁去寻找叶子节点
  while(!cur_page->IsLeafPage()){
    auto inner_page = static_cast<const InternalPage*>(cur_page);
    int index = InnerBinarySearch(key, inner_page);
    // 收集内部节点的路径信息
    ctx.path_.push({next_pgid,index});
    // 更新值
    next_pgid = inner_page->ValueAt(index);
    ctx.read_set_.pop_front();
    ctx.read_set_.push_back(bpm_->ReadPage(next_pgid));
    cur_page = ctx.read_set_.front().As<BPlusTreePage>();
  }

  auto leaf_pgid = ctx.read_set_.front().GetPageId();
  ctx.read_set_.pop_front();
  // 叶子节点页面加锁
  auto leaf_page_guard = bpm_->WritePage(leaf_pgid);
  LeafPage* leaf_page = leaf_page_guard.AsMut<LeafPage>();
  int index = LeafBinarySearch(key, leaf_page);
  if(index == -1){
    return;
  }
  // 删除这个键值
  DeleteSpeciKeyVal(index, leaf_page);
  // 判断这个节点是否为根节点
  if(ctx.path_.empty()){
    // 标记树为空
    if(leaf_page->GetSize() == 0){
      auto head_guard = bpm_->WritePage(header_page_id_);
      auto head_page = head_guard.AsMut<BPlusTreeHeaderPage>();
      head_page->root_page_id_ = INVALID_PAGE_ID;
    }
    return;
  }

  // 查看这个节点键值数量是否大于等于最小阈值
  if(leaf_page->GetSize() >= leaf_page->GetMinSize()){
    return;
  }
  // 需要去左边和右边尝试借节点


  

  //auto pgid = ctx.read_set_.front().GetPageId();
  ctx.read_set_.pop_front();

  while(!ctx.path_.empty()){

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
void BPLUSTREE_TYPE::InsertLeafPageNode(int insert_pos,const KeyType& key,const ValueType& val,LeafPage* leaf_page,bool init){
  if(init){
    leaf_page->Init(leaf_max_size_);
  }
  else{
    for(int i = insert_pos;i < leaf_page->GetSize();i++){
      // 键和值全部往后移动一位
      leaf_page->SetKeyAt(i+1, leaf_page->KeyAt(i));
      leaf_page->SetValueAt(i+1, leaf_page->ValueAt(i));
    }
  }
  leaf_page->SetKeyAt(insert_pos, key);
  leaf_page->SetValueAt(insert_pos, val);
  leaf_page->ChangeSizeBy(1);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInnerPageNode(int insert_pos,const KeyType& key,const page_id_t& val,InternalPage* inner_page,bool init){
  if(init){
    inner_page->Init(internal_max_size_);
  }
  else{
    for(int i = insert_pos;i < inner_page->GetSize();i++){
      inner_page->SetKeyAt(i+1, inner_page->KeyAt(i));
      inner_page->SetValueAt(i+1, inner_page->ValueAt(i));
    }
  }
  // 哨兵位置不放键
  if(!init){
    inner_page->SetKeyAt(insert_pos, key);
  }
  inner_page->SetValueAt(insert_pos, val);
  inner_page->ChangeSizeBy(1);
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
      new_leaf_page->ValueAt(i), new_leaf_page,false);
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
      new_inner_page->ValueAt(i), new_inner_page,false);
    }
    // 原节点丢弃分割点
    org_inner_page->SetSize(split_pos);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CheckParentPage(page_id_t parent_id)->bool{
  auto parent_page_guard = bpm_->ReadPage(parent_id);
  auto parent_page = parent_page_guard.As<const InternalPage>();
  return parent_page->GetSize() == internal_max_size_;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeliverToHead(page_id_t parent_id,Context& ctx,page_id_t left_pgid,page_id_t right_pgid,const KeyType& split_key){
  auto parent_page_guard = bpm_->WritePage(parent_id);
  auto parent_page = parent_page_guard.AsMut<InternalPage>();
  InsertInnerPageNode(0,KeyType{},left_pgid , parent_page,true);
  InsertInnerPageNode(1, split_key, right_pgid,parent_page,false);
  // 锁住header从而更新信息
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = parent_id;
  ctx.header_page_ = std::nullopt;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeliverToInnerPage(Context& ctx,page_id_t right_pgid,const KeyType& split_key)->page_id_t{

  auto parent_pgid = ctx.path_.top().first;
  auto parent_page_guard = bpm_->WritePage(parent_pgid);
  auto insert_pos = ctx.path_.top().second + 1;
  auto parent_page = parent_page_guard.AsMut<InternalPage>();
  ctx.path_.pop();
  InsertInnerPageNode(insert_pos, split_key, right_pgid,parent_page,false);
  return parent_pgid;
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

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
