//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>
#include <map>

#include "common/config.h"
#include "common/macros.h"
#include "type/type.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

using ll = int64_t;

class LRUKNode {
 public:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.
  
  // 保存访问的历史记录 用us记录 故采用ll
  // 统一push_back插入
  std::list<ll> history_;
  // list能容纳的最大访问记录个数
  size_t k_;
  // 帧ID
  frame_id_t fid_;
  // 某些帧不可被删除
  bool is_evictable_;
  LRUKNode * prev_,*next_;

  LRUKNode():k_(0),fid_(0),is_evictable_(false),prev_(nullptr),next_(nullptr){};

  LRUKNode(size_t k,frame_id_t fid):k_(k),fid_(fid),is_evictable_(false){};

  void Updatehistory(ll timestamp);
};
/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() {
    for (auto &p : node_store_) {
      delete p.second;
    }
    for(auto&p:freq_map_){
      delete p.second;
    }
  };

  auto Evict() -> std::optional<frame_id_t>;

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  void Remove(frame_id_t frame_id);

  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  ll current_timestamp_;
  size_t curr_size_;
  size_t replacer_size_;
  size_t k_;
  std::mutex latch_;

  // 记录帧对应的LRUKNode节点信息
  std::unordered_map<frame_id_t, LRUKNode*> node_store_;
  // 记录频率对应的头节点信息
  std::map<int, LRUKNode*> freq_map_;
  
  // 检测帧id是否合法
  auto Validframeid(frame_id_t frame_id)const->bool;
  // 检查频率链表是否为空
  auto Isemptynodelist(LRUKNode* head)const->bool;
  // 往频率链表添加新节点
  void PutnodeinFreqmap(LRUKNode* newnode);
  // 获取频率链表待删除节点
  auto GetDlnode(LRUKNode* head)->LRUKNode*;
  // 删除频率链表指定节点
  void RemoveNodeinFreqmap(LRUKNode* dlnode);
  // 往节点后添加新节点
  void Pushback(LRUKNode* curnode,LRUKNode* newnode);

};

}  // namespace bustub
