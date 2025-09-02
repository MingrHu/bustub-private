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
  if(guard.GetPageId() == INVALID_PAGE_ID){
    return false;
  }

  ctx.read_set_.push_back(guard);

  auto index = FindBPlusTreeLeafNode(key, ctx);
  // 没找到
  if(ctx.read_set_.empty()){
    return false;
  }

  ReadPageGuard read_guard = std::move(ctx.read_set_.front());
  auto target_val = read_guard.As<LeafPage>();
  result->push_back(target_val->ValueAt(index));
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
  Context ctx;
  ctx.read_set_.push_back(bpm_->ReadPage(header_page_id_));
  ctx.root_page_id_ = ctx.read_set_.front().As<BPlusTreeHeaderPage>()->root_page_id_;
  // 判空
  if(ctx.root_page_id_ == INVALID_PAGE_ID){
    ctx.read_set_.pop_front();
    // 升级写锁
    ctx.header_page_ = bpm_->WritePage(header_page_id_);
    auto root_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    // 防止多次重写
    if(root_page->root_page_id_ == INVALID_PAGE_ID){
      // 获取新的页 并设置该页的叶子节点值
      auto new_pgid = bpm_->NewPage();
      // 初始化根节点
      auto write_guard = bpm_->WritePage(new_pgid);
      LeafPage* leaf_page = write_guard.AsMut<LeafPage>();
      // 更新根节点
      InsertLeafPageNode(0, key, value, leaf_page,true);
      // 更新根节点的页ID值
      ctx.root_page_id_ = new_pgid; 
      root_page->root_page_id_ = new_pgid;     
      return true;
    }
    // 释放锁
    ctx.header_page_ = std::nullopt;
  }

  if(ctx.read_set_.empty()){
    ctx.read_set_.push_back(bpm_->ReadPage(header_page_id_));
  }
  auto root_pgid = ctx.root_page_id_;
  // 释放header避免死锁
  ctx.read_set_.pop_front();

  // 加入根页面 现在只有root页面  
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

  // 对待操作的叶子节点加锁
  ctx.write_set_.push_back(bpm_->WritePage(leaf_pgid));
  LeafPage* leaf_page = ctx.write_set_.front().AsMut<LeafPage>();

  if(leaf_page->GetSize() < leaf_max_size_){
    int insert_index = LeafBinarySearch(key, leaf_page);
    // 重复键
    if(insert_index != leaf_page->GetSize() && comparator_(key,leaf_page->KeyAt(insert_index))){
      ctx.write_set_.pop_back();
      return false;
    }
    // 插入值
    InsertLeafPageNode(insert_index, key, value, leaf_page,false);

    // 如果叶子节点需要分裂
    if(leaf_page->GetSize() == leaf_max_size_){
      int split_pos = leaf_max_size_ >> 1;
      // 创建新页
      auto new_pgid = bpm_->NewPage();
      auto new_write_guard = bpm_->WritePage(new_pgid);
      LeafPage* new_leaf_page = new_write_guard.AsMut<LeafPage>();
      SplitPage(split_pos, new_pgid, leaf_page, new_leaf_page);
      // 需要向上传递的键和值
      KeyType split_key = leaf_page->KeyAt(split_pos);
      page_id_t child_pgid = new_pgid;  
      // 如果只有叶子节点
      if(ctx.read_set_.empty()){

      }

      // 往上递推节点有两种情况
      // 如果树有内部节点
      while(!ctx.path_.empty()){
        auto parent_page_guard = bpm_->WritePage(ctx.path_.top().first);
        auto pre_insert_pos = ctx.path_.top().second;
        ctx.path_.pop();
        InternalPage* parent_page = parent_page_guard.AsMut<InternalPage>();
        // 这里子节点的split_key可以直接插入父节点对应的位置
        InsertInnerPageNode(pre_insert_pos, split_key, child_pgid,parent_page,false);
        
        if(parent_page->GetSize() == internal_max_size_){
          split_pos = (internal_max_size_ >> 1) + 1;
          SplitPage(split_pos, child_pgid, parent_page,false);
        }

      }

    }
    else{
      // 释放叶子页面
      ctx.write_set_.pop_front();
    }
  }

  

  {
    

    // 判断是否需要分裂
    if(leaf_page->GetSize() == leaf_page->GetMaxSize()){
      
      
      int new_root_page_id = bpm_->NewPage();
      // 产生新的根节点
      auto root_guard=bpm_->WritePage(new_root_page_id);
      header_page_id_ = new_root_page_id;
      root_page->root_page_id_ = new_root_page_id;
    }
    ctx.header_page_ = std::nullopt;
  }




  
  auto page = read_guard.As<BPlusTreePage>();

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
  UNIMPLEMENTED("TODO(P2): Add implementation.");
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
auto BPLUSTREE_TYPE::LeafBinarySearch(const KeyType& key,const LeafPage* leaf_page)->int{
  
  int l = 0;
  int r = leaf_page->GetSize() - 1;
  auto mid = l + (r - l) / 2;
  // 寻找第一个大于等于key的节点
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
  if(l == leaf_max_size_ || comparator_(key,leaf_page->KeyAt(l)) == 0){
    return l;
  }

  return l - 1;
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InnerBinarySearch(const KeyType& key,const InternalPage* inner_page)->int{
  
  int start = 1;
  if(comparator_(inner_page->KeyAt(start),key) == 1){
    return 0;
  }
  int end = inner_page->GetSize() - 1;
  auto mid = start + (end - start) / 2;
  // 找到第一个大于等于key的节点
  while(start <= end){
    if(comparator_(inner_page->KeyAt(mid),key) == -1){
      start = mid + 1;
    } 
    else {
      end = mid - 1;
    }
    mid = start + (end - start) / 2;
  }
  if(start == internal_max_size_ || comparator_(key,inner_page->KeyAt(start)) == 0){
    return start;
  }
  return start - 1;
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindBPlusTreeLeafNode(const KeyType& key,Context& ctx)->int{

  int index = -1;
  while(!ctx.read_set_.empty()){
    // 获取页面 并转为B+树页面
    auto guard = std::move(ctx.read_set_.front());
    // 待操作页面
    auto op_page = guard.As<BPlusTreePage>();

    if(op_page->IsLeafPage()){
      const auto leaf_page = static_cast<const LeafPage*>(op_page);
      index = LeafBinarySearch(key, leaf_page);
      if(index == leaf_page->GetSize() || comparator_(leaf_page->KeyAt(index),key) != 0){
        ctx.read_set_.pop_front();
      }
      break;
    }
    
    const auto inner_page = static_cast<const InternalPage*>(op_page);
    // inner_page 查找的要特殊一些 如果超出最大值 按最大值找
    index = InnerBinarySearch(key, inner_page);
    if(index == inner_page->GetSize()){
      index -= 1;
    }
    ctx.read_set_.pop_front();
    int pgid = inner_page->ValueIndex(index);
    // 放入下一个值的页面
    auto readguard = bpm_->ReadPage(pgid);
    ctx.read_set_.push_back(readguard);
  }
  return index;
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
  inner_page->SetKeyAt(insert_pos, key);
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
    org_inner_page->SetSize(split_pos + 1);
  }
}


template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
