//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <chrono>
#include <climits>
#include <string>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
{
  replacer_size_ = num_frames;
  k_ = k;
  current_timestamp_ = 0;
  curr_size_ = 0;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame is successfully evicted, or `std::nullopt` if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t>
{ 
  if(curr_size_ == 0)
    return std::nullopt; 
  // 拿到第一个最小频率对应的最长k距离结点
  LRUKNode* dlnode = nullptr;
  latch_.lock();
  for(const auto& it:freq_map_){
    dlnode = GetDlnode(it.second);
    if(dlnode!=nullptr && dlnode != it.second)
      break;
  }
  // 删除并记录信息
  frame_id_t res = dlnode->fid_;
  int fq = dlnode->history_.size();
  // 从所有的map里删除 
  Remove(res);
  // 检查被淘汰的节点访问频率是否存在
  if(Isemptynodelist(freq_map_[fq])){
    freq_map_.erase(fq);
  }
  curr_size_ -= 1;
  latch_.unlock();
  return res;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type)
{
  latch_.lock();
  BUSTUB_ASSERT(!Validframe_id(frame_id), "Invalid frame id!");
  // 获取当前时间戳
  auto nowt = std::chrono::system_clock::now();
  current_timestamp_ = static_cast<ll>(std::chrono::
    duration_cast<std::chrono::microseconds>(nowt.time_since_epoch()).count());
  
  if(node_store_.find(frame_id) != node_store_.end()){
    LRUKNode* node = node_store_[frame_id];
    int fq = node->history_.size();
    node->Updatehistory(current_timestamp_);
    RemoveNodeinFreqmap(node);
    PutnodeinFreqmap(node);
    // 检查更新前的频率对应的节点是否为空
    if(Isemptynodelist(freq_map_[fq])){
      freq_map_.erase(fq);
    }
  }
  else{
    LRUKNode* newnode = new LRUKNode(k_,frame_id);
    newnode->history_.push_back(current_timestamp_);
    node_store_[frame_id] = newnode;
    PutnodeinFreqmap(newnode);
  }
  latch_.unlock();
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {

    BUSTUB_ASSERT(!Validframe_id(frame_id), "Invalid frame id!");
    if (node_store_.find(frame_id) == node_store_.end()) 
      return;
    LRUKNode* cur_node = node_store_[frame_id];

    latch_.lock();
    // 计算具备逐出资格的总帧数
    curr_size_ += cur_node->is_evictable_ ? 
    (set_evictable ? 0 : -1): (set_evictable ? 1 : 0);
    cur_node->is_evictable_ = set_evictable;
    latch_.unlock();
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id)
 {
  if(node_store_.find(frame_id) == node_store_.end())
    return;
  latch_.lock();
  LRUKNode* dlnode = node_store_[frame_id];
  BUSTUB_ASSERT(dlnode->is_evictable_, "This frame can not remove!");
  RemoveNodeinFreqmap(dlnode);
  delete (dlnode);
  node_store_.erase(frame_id);
  latch_.unlock();
 }

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t 
{ 
  return curr_size_; 
}

bool LRUKReplacer::Validframe_id(frame_id_t frame_id){
  return frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_;
}

bool LRUKReplacer::Isemptynodelist(LRUKNode* head){
  return head->next_ == head;
}

void LRUKReplacer::PutnodeinFreqmap(LRUKNode* newnode){
    int fq = newnode->history_.size();
    if(freq_map_.find(fq) == freq_map_.end()){
      LRUKNode* head = new LRUKNode();
      head->next_ = head,head->prev_ = head;
      freq_map_[fq] = head;
    }
    LRUKNode* head = freq_map_[fq];
    Pushback(head,newnode);
}

void LRUKReplacer::RemoveNodeinFreqmap(LRUKNode* dlnode){
  LRUKNode* prev = dlnode->prev_,*next = dlnode->next_;
  prev->next_ = next;
  next->prev_ = prev;
}

LRUKNode* LRUKReplacer::GetDlnode(LRUKNode* head)
{
  LRUKNode* pos = head->prev_;
  while(pos != head && pos->is_evictable_ != true)
    pos = pos->prev_;
  return pos;
}

void LRUKReplacer::Pushback(LRUKNode* curnode,LRUKNode* newnode){
  LRUKNode* next = curnode->next_;
  curnode->next_ = newnode,next->prev_ = newnode;
  newnode->prev_ = curnode,newnode->next_ = next;
}

void LRUKNode::Updatehistory(ll timestamp){
  if(history_.size() == k_){
    history_.pop_front();
  }
  history_.push_back(timestamp);
}



}  // namespace bustub
