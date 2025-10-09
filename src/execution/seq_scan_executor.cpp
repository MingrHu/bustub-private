//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include <vector>
#include "common/macros.h"
#include "storage/table/table_iterator.h"
#include "type/value.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  BUSTUB_ENSURE(table_info_, "table_info is nullptr");
  table_iter_ = std::make_shared<TableIterator>(table_info_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!table_iter_->IsEnd()) {
    // 获取当前表堆迭代器的元组相关信息
    *rid = table_iter_->GetRID();
    auto [meta, tp] = table_iter_->GetTuple();

    ++(*table_iter_);

    // 被删除的元组直接跳过
    if (meta.is_deleted_) {
      continue;
    }

    if (plan_->filter_predicate_ != nullptr) {
      auto need_filt = plan_->filter_predicate_->Evaluate(&tp, plan_->OutputSchema());
      if (need_filt.IsNull() || !need_filt.GetAs<bool>()) {
        continue;
      }
    }

    // 把Tuple的值按照schema的模式传回tuple
    std::vector<Value> res;
    size_t size = plan_->OutputSchema().GetColumnCount();
    res.reserve(size);
    for (size_t col_idx = 0; col_idx < size; col_idx++) {
      // 必须先按照表堆的格式获取数据 元组的列就是col_idx即可
      res.emplace_back(tp.GetValue(&table_info_->schema_, col_idx));
    }

    *tuple = {res, &plan_->OutputSchema()};
    return true;
  }
  return false;
}

}  // namespace bustub
