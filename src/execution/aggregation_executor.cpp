//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <utility>
#include <vector>
#include "storage/table/tuple.h"
#include "type/value.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  Tuple child_tp{};
  RID child_rid{};
  // 2024fall会多次调用Init 需要多次重置初始值
  // 因此需要重置分组值聚合哈希表 否则数据错误
  child_executor_->Init();
  aht_.Clear();
  while (child_executor_->Next(&child_tp, &child_rid)) {
    auto key = MakeAggregateKey(&child_tp);
    auto value = MakeAggregateValue(&child_tp);
    aht_.InsertCombine(key, value);
  }
  // 如果没有聚合的groupbys（分组）
  // 则需要生成默认的键值
  // 这个默认情况必须满足只有一个聚合列才能输出 不能存在多个例如
  // select v5, min(v1), sum(v2), count(*) from t1
  // 必须是select min(v1) from t1
  if (aht_.Begin() == aht_.End() && GetOutputSchema().GetColumnCount() == 1) {
    aht_.InsertInitialKeyVal();
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 在plan_的输出格式中可以看到tuple的value是先放聚合字段再放聚合值
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> values;
  auto key = aht_iterator_.Key();
  auto val = aht_iterator_.Val();
  values.reserve(key.group_bys_.size() + val.aggregates_.size());

  values.insert(values.end(), key.group_bys_.begin(), key.group_bys_.end());
  values.insert(values.end(), val.aggregates_.begin(), val.aggregates_.end());

  ++aht_iterator_;
  *tuple = {values, &plan_->OutputSchema()};
  *rid = {};
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
