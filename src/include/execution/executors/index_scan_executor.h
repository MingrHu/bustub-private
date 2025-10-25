//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/extendible_hash_table_index.h"
#include "storage/index/index_iterator.h"
#include "storage/index/int_comparator.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */
// 要么点查询 要么范围扫描
class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The index scan plan node to be executed. */
  /* 对于IndexScanplanNode的成员
    table_oid_：要扫描的表的标识符
    index_oid_：要使用的索引的标识符
    filter_predicate_：可选的过滤谓词表达式，用于在索引扫描时过滤数据
    pred_keys_：用于点查找的常量值表达式向量（例如 WHERE 子句中的具体值）
  */
  const IndexScanPlanNode *plan_;

  bool point_search_;

  std::vector<RID> point_search_res_;

  IndexIterator<IntegerKeyType_BTree, IntegerValueType_BTree, IntegerComparatorType_BTree> iter_;

  size_t cur_pos_;

  std::shared_ptr<TableInfo> table_info_;

  std::shared_ptr<IndexInfo> index_info_;

  auto Helperfunc(const IndexScanPlanNode *plan, const Tuple &tp, const TableInfo *table_info) const
      -> std::vector<Value>;
};

}  // namespace bustub
