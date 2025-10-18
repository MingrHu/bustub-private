#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  // TODO(fall2023): implement me!
  current_reads_[read_ts] += 1;
  pq_.push(read_ts);
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  if(current_reads_[read_ts] == 0){
    return;
  }
  current_reads_[read_ts] -= 1;
  // 本质上是懒删除
  // 指定删除的值恰好是水位线的值 就需要重新去更新水位线
  // 能这样做的原因是初始的水位线和第一个插入值一致
  if(watermark_ == read_ts){
    while(!pq_.empty()){
      int val = pq_.top();
      if(current_reads_[val] > 0){
        watermark_ = val;
        return;
      }
      pq_.pop();
    }
    watermark_ = commit_ts_;
  }
}

}  // namespace bustub
