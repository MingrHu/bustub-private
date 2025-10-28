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
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/table_heap.h"
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
    BUSTUB_ENSURE(meta.ts_ >= 0, "meta ts error!");
    ++(*table_iter_);
    // const Schema& schema,const RID& rid,TableHeap* table_heap,
    // Transaction* txn,TransactionManager* txn_manager
    auto res_tuple = AcquireSpecTuple(table_info_->schema_, *rid, table_info_->table_.get(),
                                      exec_ctx_->GetTransaction(), exec_ctx_->GetTransactionManager());
    if (res_tuple.has_value()) {
      // 过滤条件 原本是加在最新的元组上的 现在应用在过去的元组上
      auto spec_tuple = res_tuple.value();
      if (plan_->filter_predicate_ != nullptr) {
        auto need_filt = plan_->filter_predicate_->Evaluate(&spec_tuple, plan_->OutputSchema());
        if (need_filt.IsNull() || !need_filt.GetAs<bool>()) {
          continue;
        }
      }
      // 产生元组列值
      auto res = Helperfunc(plan_, spec_tuple, table_info_.get());
      *tuple = {res, &plan_->OutputSchema()};
      return true;
    }
  }
  return false;
}

auto SeqScanExecutor::Helperfunc(const SeqScanPlanNode *plan, const Tuple &tp, const TableInfo *table_info) const
    -> std::vector<Value> {
  std::vector<Value> res;
  size_t size = plan->OutputSchema().GetColumnCount();
  res.reserve(size);
  // 把Tuple的值按照schema的模式传回tuple
  for (size_t col_idx = 0; col_idx < size; col_idx++) {
    // 必须先按照表堆的格式获取数据 元组的列就是col_idx即可
    res.emplace_back(tp.GetValue(&table_info->schema_, col_idx));
  }
  return res;
}

}  // namespace bustub
