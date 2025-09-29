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
#include "storage/index/index_iterator.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),plan_(plan) {}

void IndexScanExecutor::Init() { 
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);

  if(!plan_->pred_keys_.empty()){
    point_search_ = true;
    // 尝试根据常量表达式获取索引的键
    // 索引的键本质上也是元组 根据一个或多个列的模式来规定元组
    std::vector<Value> values;
    for(const auto& exp:plan_->pred_keys_){
        // 这里的schema必须是索引的键schema
        values.emplace_back(exp->Evaluate({},index_info->key_schema_));
    }
    Tuple index_key = {values,&index_info->key_schema_};
    // ScanKey 方法主要是根据索引键进行单点查询 查询的结果放入result 一般只有一个RID
    index_info->index_->ScanKey(index_key,&point_search_res_ , exec_ctx_->GetTransaction());
    cur_pos = 0;
  }
  else{
    point_search_ = false;
    auto tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
    iter_ = tree->GetBeginIterator();
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if(point_search_){
    while(cur_pos < point_search_res_.size()){
      
    }
  }
  else{
    // 参考seqscan
    while(!iter_.IsEnd()){

    }
  }
  return false;
}

}  // namespace bustub
