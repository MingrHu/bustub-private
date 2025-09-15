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
#include <mutex>
#include <optional>
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
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) {
  replacer_size_ = num_frames;
  node_store_.reserve(num_frames);
  k_ = k;
  current_timestamp_.store(0);
  curr_size_ = 0;
  head_ = new LRUKNode();
  head_->prev_ = head_;
  head_->next_ = head_;

  evict_ = 0;
  access_ = 0;
  is_done_.store(0);
  background_thread_.emplace([&]{StartThreadFunc();});
}

LRUKReplacer::~LRUKReplacer() {

  if (background_thread_.has_value()) {
    background_thread_->join();
  }
  for (auto p : node_store_) {
    if (p.second != nullptr) {
      delete p.second;
      p.second = nullptr;
    }
  }
  delete head_;
  head_ = nullptr;
  request_queue_.Put(std::nullopt);
  // printf("LRU-K replacer info: Access_call: %zu  Evict_call: %zu  \n",access_,evict_);
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
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::optional<frame_id_t> fid = std::nullopt;
  latch_.lock();
  evict_ += 1;
  if (curr_size_ != 0) {
    // 尝试找对应的节点最久未被访问节点
    LRUKNode *dlnode = nullptr;
    dlnode = GetDlnode();
    if (dlnode != nullptr) {
      fid = dlnode->fid_;
      // 从frekmap移除
      RemoveNodeInFreqKmap(dlnode);
      RemoveNodeInList(dlnode);
      // 重置初始化这个节点 不释放内存
      // 避免频繁申请释放空间
      dlnode->Clearcurnode();
    }
    curr_size_ -= 1;
  }
  latch_.unlock();
  return fid;
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
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // BUSTUB_ENSURE(!Validframeid(frame_id), "Invalid frame id!");
  // 更新当前时间戳
  auto timestamp = Updatetimestamp();
  latch_.lock();
  access_ += 1;
  // 当前帧存在
  if (node_store_.find(frame_id) != node_store_.end()) {
    auto node = node_store_[frame_id];
    // 如果这个节点访问次数大于等于K 尝试删除map里面的
    RemoveNodeInFreqKmap(node);
    // 把节点添加到候补队列或K频率表
    PushNode(frame_id, node, timestamp);
  } else {
    auto newnode = new LRUKNode(k_, frame_id);
    PushNode(frame_id, newnode, timestamp);
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
  // BUSTUB_ENSURE(!Validframeid(frame_id), "Invalid frame id!");
  latch_.lock();
  if (node_store_.find(frame_id) != node_store_.end()) {
    LRUKNode *cur_node = node_store_[frame_id];
    // 计算具备逐出资格的总帧数
    curr_size_ += (cur_node->is_evictable_ ? (set_evictable ? 0 : -1) : (set_evictable ? 1 : 0));
    cur_node->is_evictable_ = set_evictable;
  }
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
void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (node_store_.find(frame_id) != node_store_.end()) {
    LRUKNode *dlnode = node_store_[frame_id];
    // BUSTUB_ASSERT(dlnode->is_evictable_, "This frame can not remove!");
    RemoveNodeInList(dlnode);
    RemoveNodeInFreqKmap(dlnode);
    node_store_.erase(frame_id);
    curr_size_ -= 1;
    // std::unique_lock lacth(fk_latch_);
    // while(is_done_.load() != 0){
    //   cv_.wait(lacth);
    // }
    // delete dlnode;
    // dlnode = nullptr;
  }
  latch_.unlock();
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  latch_.lock();
  auto res = curr_size_;
  latch_.unlock();
  return res;
}

auto LRUKReplacer::Validframeid(frame_id_t frame_id) const -> bool {
  return frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_;
}

void LRUKReplacer::RemoveNodeInFreqKmap(LRUKNode *dlnode) {
  is_done_.fetch_add(1);
  std::optional<Task> task = std::make_optional(Task{dlnode,0,false});
  request_queue_.Put(task);
}

void LRUKReplacer::RemoveNodeInList(LRUKNode *dlnode) {
  if (dlnode == nullptr || dlnode->prev_ == nullptr || dlnode->next_ == nullptr) {
    return;
  }
  auto prev = dlnode->prev_;
  auto next = dlnode->next_;
  prev->next_ = next;
  next->prev_ = prev;
  dlnode->next_ = nullptr;
  dlnode->prev_ = nullptr;
}

auto LRUKReplacer::GetDlnode() -> LRUKNode * {
  std::unique_lock latch(fk_latch_);
  while(is_done_.load() != 0){
    cv_.wait(latch);
  }
  // 保证频率表不是正在更新
  // 这个时候再访问就没问题了
  LRUKNode *pos = head_->prev_;
  while (pos != head_ && !pos->is_evictable_) {
    pos = pos->prev_;
  }
  if (pos == head_) {
    pos = nullptr;
    for (const auto &p : freqk_map_) {
      if (p.second->is_evictable_) {
        pos = p.second;
        break;
      }
    }
  }
  return pos;
}

void LRUKReplacer::HeadPush(LRUKNode *node) {
  LRUKNode *next = head_->next_;
  head_->next_ = node, next->prev_ = node;
  node->prev_ = head_, node->next_ = next;
}

void LRUKNode::Updatehistory(size_t timestamp) {
  if (history_.size() == k_) {
    history_.pop_front();
  }
  history_.push_back(timestamp);
}

void LRUKReplacer::PushNode(frame_id_t fid, LRUKNode *node, size_t timestamp) {

  is_done_.fetch_add(1);
  // 存储或更新帧
  node_store_[fid] = node;
  std::optional<Task> task = std::make_optional(Task{node,timestamp,true});
  request_queue_.Put(task);
}

auto LRUKReplacer::Updatetimestamp() -> size_t { return current_timestamp_.fetch_add(1, std::memory_order_relaxed); }

void LRUKReplacer::OutputInfo(size_t& access_call,size_t& evict_call){
  access_call = access_;
  evict_call = evict_;
}

void LRUKReplacer::StartThreadFunc(){
  std::optional<Task> task = std::nullopt;
  while((task = request_queue_.Get(),task!=std::nullopt)){
    // 如果是插入操作
    if(task->is_insert_){
      // 先更新时间戳
      task->node_->Updatehistory(task->timestamp_);
      // 如果不满k
      if (task->node_->history_.size() == 1 && k_ > 1) {
        HeadPush(task->node_);
      } 
      // 如果在频率k表里面
      else if (task->node_->history_.size() == k_) {
        RemoveNodeInList(task->node_);
        freqk_map_[task->node_->history_.front()] = task->node_;
      }
    }
    // 否则是删除操作
    else{
      if (task->node_ != nullptr && task->node_->history_.size() == k_) {
        freqk_map_.erase(task->node_->history_.front());
      }
    }
    // 更新标志
    {
      std::unique_lock latch(fk_latch_);
      is_done_.fetch_sub(1);
      cv_.notify_one();
    } 
  }
}

}  // namespace bustub
