//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.cpp
//
// Identification: src/primer/hyperloglog_presto.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog_presto.h"
#include <bitset>
#include <cstdint>
#include "common/util/hash_util.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits)
    : cardinality_(0), b_(n_leading_bits), flag_(false) {
  if (b_ >= 0) {
    flag_ = true;
    dense_bucket_.resize(1 << b_, 0);
  }
}

/** @brief Element is added for HLL calculation. */
template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  hash_t hashval = CalculateHash(val);
  std::bitset<BITSET_CAPACITY> bf(hashval);
  // 取index
  std::bitset<BITSET_CAPACITY> tempbf(bf >> (BITSET_CAPACITY - b_));
  uint64_t index = tempbf.to_ullong();

  // 计算LSBS 最右边的连续0位数
  int16_t lsbsnum = 0;
  int16_t msbs = 0;
  for (size_t i = 0; i < bf.size() - b_; i++) {
    if (!bf[i]) {
      lsbsnum = i + 1;
    } else {
      break;
    }
  }

  std::bitset<TOTAL_BUCKET_SIZE> lsbs(lsbsnum);
  for (size_t i = 0; i < lsbs.size(); i++) {
    if (lsbs[i]) {
      msbs = i + 1;
    }
  }
  mtx_.lock();
  if (msbs <= DENSE_BUCKET_SIZE) {
    std::bitset<DENSE_BUCKET_SIZE> dense(lsbs.to_ullong());
    dense_bucket_[index] = (dense.to_ullong() > dense_bucket_[index].to_ullong()) ? dense : dense_bucket_[index];
  } else {
    // 取出溢出位
    std::bitset<OVERFLOW_BUCKET_SIZE> over((lsbs >> DENSE_BUCKET_SIZE).to_ullong());
    uint8_t mask = (1 << DENSE_BUCKET_SIZE) - 1;
    std::bitset<DENSE_BUCKET_SIZE> dense(mask & lsbs.to_ullong());
    uint8_t curval = (over.to_ullong() << DENSE_BUCKET_SIZE) + dense.to_ullong();
    // 比较当前index下寄存器值决定是否进行更新
    // 更新的话需要更新溢出和密集桶两个的值
    uint8_t registerval = (overflow_bucket_[index].to_ullong() << DENSE_BUCKET_SIZE) + dense_bucket_[index].to_ullong();
    if (curval > registerval) {
      overflow_bucket_[index] = over;
      dense_bucket_[index] = dense;
    }
  }
  mtx_.unlock();
}

/** @brief Function to compute cardinality. */
template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  if (!flag_) {
    return;
  }
  uint64_t m = 1ULL << b_;
  double sum = 0;
  for (uint64_t j = 0; j < dense_bucket_.size(); j++) {
    if (overflow_bucket_.find(j) != overflow_bucket_.end()) {
      std::bitset<TOTAL_BUCKET_SIZE> over(overflow_bucket_[j].to_ullong());
      double p = (over << DENSE_BUCKET_SIZE).to_ullong() + dense_bucket_[j].to_ullong();
      sum += std::pow(2.0, -p);
    } else {
      double p = dense_bucket_[j].to_ullong();
      sum += std::pow(2.0, -p);
    }
  }
  double fval = CONSTANT * m * m / sum;
  mtx_.lock();
  cardinality_ = static_cast<size_t>(fval);
  mtx_.unlock();
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
