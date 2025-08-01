//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.cpp
//
// Identification: src/primer/hyperloglog.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog.h"
#include <sys/types.h>
#include <cmath>
#include <cstdint>
#include "common/util/hash_util.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0), b_(0), flag_(false) {
  if (n_bits < 0 || n_bits >= 64) {
    flag_ = true;
    return;
  }
  b_ = n_bits;
  regist_.resize((1 << n_bits), 0);
}

/**
 * @brief Function that computes binary.
 *
 * @param[in] hash
 * @returns binary of a given hash
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  std::bitset<BITSET_CAPACITY> res(hash);
  return res;
}

/**
 * @brief Function that computes leading zeros.
 *
 * @param[in] bset - binary values of a given bitset
 * @returns leading zeros of given binary set
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  uint64_t res = 0;

  for (int i = bset.size() - 1; i >= 0; i--) {
    bool bit = bset[i];
    if (bit) {
      res = bset.size() - i;
      break;
    }
  }
  return res;
}

/**
 * @brief Adds a value into the HyperLogLog.
 *
 * @param[in] val - value that's added into hyperloglog
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  if (flag_) {
    return;
  }
  hash_t hashval = CalculateHash(val);
  std::bitset<BITSET_CAPACITY> bf = ComputeBinary(hashval);
  // 确定index
  uint64_t index = (bf >> (BITSET_CAPACITY - b_)).to_ullong();

  // 除开index位后再找左边第一个置位
  uint64_t mask = (b_ == 0ULL ? UINT64_MAX : (1ULL << (BITSET_CAPACITY - b_)) - 1);
  std::bitset<BITSET_CAPACITY> tempbf = ComputeBinary(mask & hashval);
  uint64_t msb = PositionOfLeftmostOne(tempbf) - b_;

  mtx_.lock();
  regist_[index] = fmax(regist_[index], msb);
  mtx_.unlock();
}

/**
 * @brief Function that computes cardinality.
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  if (flag_) {
    return;
  }
  uint64_t m = 1ULL << b_;
  double sum = 0;
  for (const auto &p : regist_) {
    sum += std::pow(2.0, -p);
  }
  double fval = CONSTANT * m * m / sum;
  cardinality_ = static_cast<size_t>(fval);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
