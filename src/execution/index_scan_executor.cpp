//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <vector>
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // 初始化堆
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  point_search_res_.clear();

  // pred_keys 本质上是存储谓词的常量值
  if (!plan_->pred_keys_.empty()) {
    point_search_ = true;
    // 尝试根据常量表达式的值 直接获取这个值所在索引的键
    // 索引的键本质上也是元组 根据一个或多个列的模式来规定元组
    for (const auto &exp : plan_->pred_keys_) {
      std::vector<Value> values;
      // 这里的schema必须是索引的键schema
      values.emplace_back(exp->Evaluate(nullptr, index_info_->key_schema_));
      Tuple index_key = {values, &index_info_->key_schema_};
      std::vector<RID> point_temp_res;
      // ScanKey 方法主要是根据索引键进行单点查询 查询的结果放入result 一般只有一个RID
      // 如果有多个常量谓词 则必须保存所有的
      index_info_->index_->ScanKey(index_key, &point_temp_res, exec_ctx_->GetTransaction());
      if (point_temp_res.empty()) {
        continue;
      }
      point_search_res_.push_back(point_temp_res[0]);
    }
    cur_pos_ = 0;
  } else {
    point_search_ = false;
    auto tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
    iter_ = tree->GetBeginIterator();
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (point_search_) {
    while (cur_pos_ < point_search_res_.size()) {
      // 参考SeqScan
      auto cur_rid = point_search_res_[cur_pos_++];
      auto [cur_meta, cur_tuple] = table_info_->table_->GetTuple(cur_rid);
      if (cur_meta.is_deleted_) {
        continue;
      }

      if (plan_->filter_predicate_ != nullptr) {
        auto need_filt = plan_->filter_predicate_->Evaluate(&cur_tuple, table_info_->schema_);
        if (need_filt.IsNull() || !need_filt.GetAs<bool>()) {
          continue;
        }
      }
      std::vector<Value> val;
      for (size_t col_idx = 0; col_idx < plan_->OutputSchema().GetColumnCount(); col_idx++) {
        val.emplace_back(cur_tuple.GetValue(&table_info_->schema_, col_idx));
      }
      *tuple = {val, &plan_->OutputSchema()};
      *rid = cur_rid;
      return true;
    }
  } else {
    // 参考seqscan
    while (!iter_.IsEnd()) {
      auto [key, value] = *iter_;
      ++iter_;

      auto [cur_meta, cur_tuple] = table_info_->table_->GetTuple(value);
      if (cur_meta.is_deleted_) {
        continue;
      }

      if (plan_->filter_predicate_ != nullptr) {
        auto need_filt = plan_->filter_predicate_->Evaluate(&cur_tuple, table_info_->schema_);
        if (need_filt.IsNull() || !need_filt.GetAs<bool>()) {
          continue;
        }
      }

      std::vector<Value> val;
      for (size_t col_idx = 0; col_idx < plan_->OutputSchema().GetColumnCount(); col_idx++) {
        val.emplace_back(cur_tuple.GetValue(&table_info_->schema_, col_idx));
      }
      *tuple = {val, &plan_->OutputSchema()};
      *rid = value;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
