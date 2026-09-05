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
}

LRUKReplacer::~LRUKReplacer() {
  for (auto p : node_store_) {
    if (p.second != nullptr) {
      delete p.second;
      p.second = nullptr;
    }
  }
  delete head_;
  head_ = nullptr;
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
  std::lock_guard latch(latch_);
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
  std::lock_guard latch(latch_);
  // 当前帧已经有内存了
  if (node_store_.count(frame_id) != 0) {
    auto node = node_store_[frame_id];
    // 如果这个节点访问次数大于等于K 尝试删除map里面的
    RemoveNodeInFreqKmap(node);
    // 把节点添加到队列或频率表
    PushNode(frame_id, node, timestamp);
  } else {
    auto newnode = new LRUKNode(k_, frame_id);
    PushNode(frame_id, newnode, timestamp);
  }
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
    delete dlnode;
    dlnode = nullptr;
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
  if (dlnode != nullptr && dlnode->history_.size() == k_) {
    freqk_map_.erase(dlnode->history_.front());
    return;
  }
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

void LRUKReplacer::Pushback(LRUKNode *node) {
  LRUKNode *next = head_->next_;
  head_->next_ = node, next->prev_ = node;
  node->prev_ = head_, node->next_ = next;
}

void LRUKReplacer::PushNode(frame_id_t fid, LRUKNode *node, size_t timestamp) {
  node->Updatehistory(timestamp);
  if (node->history_.size() == 1 && k_ > 1) {
    Pushback(node);
  } else if (node->history_.size() == k_) {
    RemoveNodeInList(node);
    freqk_map_[node->history_.front()] = node;
  }
  node_store_[fid] = node;
}

auto LRUKReplacer::Updatetimestamp() -> size_t { return current_timestamp_.fetch_add(1, std::memory_order_relaxed); }

}  // namespace bustub
