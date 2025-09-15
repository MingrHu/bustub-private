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

#include <limits>
#include <list>
#include <map>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
 public:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.

  // 统一push_back插入
  std::list<size_t> history_;
  // list能容纳的最大访问记录个数
  size_t k_;
  // 帧ID
  frame_id_t fid_;
  // 某些帧不可被删除
  bool is_evictable_;

  bool is_initial_;

  LRUKNode *prev_, *next_;

  LRUKNode() : k_(0), fid_(0), is_evictable_(false), is_initial_(false), prev_(nullptr), next_(nullptr){};

  LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid), is_evictable_(false), is_initial_(false){};

  void Updatehistory(size_t timestamp);

  void Clearcurnode() {
    is_evictable_ = false;
    is_initial_ = false;
    history_.clear();
    prev_ = nullptr, next_ = nullptr;
  }
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
  ~LRUKReplacer();

  auto Evict() -> std::optional<frame_id_t>;

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  void Remove(frame_id_t frame_id);

  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  std::atomic<size_t> current_timestamp_;
  size_t curr_size_;
  size_t replacer_size_;
  size_t k_;
  std::mutex latch_;

  // 测试调用次数
  size_t access_call{0};
  size_t evict_call{0};

  // 记录帧对应的LRUKNode节点信息
  std::unordered_map<frame_id_t, LRUKNode *> node_store_;
  // 记录频率K对应的节点信息
  std::map<size_t, LRUKNode *> freqk_map_;
  // 记录小于K对应的节点信息
  LRUKNode *head_;
  // 检测帧id是否合法
  auto Validframeid(frame_id_t frame_id) const -> bool;
  // 获取队列待淘汰节点
  auto GetDlnode() -> LRUKNode *;
  // 删除map指定节点
  void RemoveNodeInFreqKmap(LRUKNode *dlnode);
  // 删除List指定节点
  void RemoveNodeInList(LRUKNode *dlnode);
  // 往队列哨兵节点后添加新节点
  void Pushback(LRUKNode *newnode);
  // 往freqmap和nodestore添加节点
  void PushNode(frame_id_t fid, LRUKNode *node, size_t timestamp);
  // 更新当前时间戳
  auto Updatetimestamp() -> size_t;
};

}  // namespace bustub
