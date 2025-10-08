//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"

// 自定义哈希键
struct HashJoinKey{

  std::vector<bustub::Value> keys_;
  // 防止隐式子转换
  explicit HashJoinKey(const std::vector<bustub::Value> keys):keys_(keys){};

  HashJoinKey() = default;

  auto operator==(const HashJoinKey& other)const->bool{
    if(other.keys_.size() != keys_.size()){
      return false;
    }
    for(size_t i = 0;i < other.keys_.size();i++){
      if(keys_[i].CompareEquals(other.keys_[i]) != bustub::CmpBool::CmpTrue){
        return false;
      }
    }
    return true;
  }
};

// 自定义哈希特化
template <>
struct std::hash<HashJoinKey> {
  auto operator()(const HashJoinKey& key) const -> size_t {
    size_t hash_val = 0;
    for (const auto& val : key.keys_) {
      // 计算当前键的哈希值
      size_t val_hash = bustub::HashUtil::HashValue(&val);
      // 使用异或 + 黄金比例魔法数混合哈希值
      hash_val ^= val_hash + 0x9e3779b9 + (hash_val << 6) + (hash_val >> 2);
    }
    return hash_val;
  }
};

namespace bustub {
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  
  std::unordered_map<HashJoinKey,std::vector<Tuple>> hash_table_;
  // 记录下一个外表元组及相关信息
  bool next_left_tuple;
  HashJoinKey left_key;
  size_t inner_indx;
  // 当前的外表元组
  Tuple left_tuple_{};
  RID left_rid_{};
};

}  // namespace bustub
