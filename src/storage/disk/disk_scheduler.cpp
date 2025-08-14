//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// thatright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <utility>
#include "common/config.h"
#include "storage/disk/disk_manager.h"

namespace bustub {
// 四参数构造函数
DiskRequest::DiskRequest(bool is_write, char *data, page_id_t pgid, std::promise<bool> callbak)
    : is_write_(is_write), data_(data), page_id_(pgid), callback_(std::move(callbak)){};

// 移动构造函数
DiskRequest::DiskRequest(DiskRequest &&that) noexcept {
  this->callback_ = std::move(that.callback_);
  this->data_ = that.data_;
  this->is_write_ = that.is_write_;
  this->page_id_ = that.page_id_;
}

// 移动赋值函数
auto DiskRequest::operator=(DiskRequest &&that) noexcept -> DiskRequest & {
  if (this != &that) {
    this->callback_ = std::move(that.callback_);
    this->data_ = that.data_;
    this->is_write_ = that.is_write_;
    this->page_id_ = that.page_id_;
  }
  return *this;
}

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // throw NotImplementedException(
  //     "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the
  //     " "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param r The request to be scheduled.
 */
void DiskScheduler::Schedule(DiskRequest r) {
  if (r.data_ == nullptr || r.page_id_ == INVALID_PAGE_ID) {
    r.callback_.set_value(false);
    return;
  }
  std::optional<DiskRequest> opreq(std::move(r));
  request_queue_.Put(std::move(opreq));
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
 * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
 */
void DiskScheduler::StartWorkerThread() {
  while (true) {
    auto task_opt = request_queue_.Get();
    if (task_opt.has_value()) {
      auto task = std::move(task_opt.value());
      if (task.is_write_) {
        disk_manager_->WritePage(task.page_id_, task.data_);
      } else {
        disk_manager_->ReadPage(task.page_id_, task.data_);
      }
      task.callback_.set_value(true);
    }
    // 析构时退出
    else {
      break;
    }
  }
}

}  // namespace bustub
