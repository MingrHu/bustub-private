//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;

/**
 * @brief A helper class for `BufferPoolManager` that manages a frame of memory and related metadata.
 *
 * This class represents headers for frames of memory that the `BufferPoolManager` stores pages of data into. Note that
 * the actual frames of memory are not stored directly inside a `FrameHeader`, rather the `FrameHeader`s store pointer
 * to the frames and are stored separately them.
 *
 * ---
 *
 * Something that may (or may not) be of interest to you is why the field `data_` is stored as a vector that is
 * allocated on the fly instead of as a direct pointer to some pre-allocated chunk of memory.
 *
 * In a traditional production buffer pool manager, all memory that the buffer pool is intended to manage is allocated
 * in one large contiguous array (think of a very large `malloc` call that allocates several gigabytes of memory up
 * front). This large contiguous block of memory is then divided into contiguous frames. In other words, frames are
 * defined by an offset from the base of the array in page-sized (4 KB) intervals.
 *
 * In BusTub, we instead allocate each frame on its own (via a `std::vector<char>`) in order to easily detect buffer
 * overflow with address sanitizer. Since C++ has no notion of memory safety, it would be very easy to cast a page's
 * data pointer into some large data type and start overwriting other pages of data if they were all contiguous.
 *
 * If you would like to attempt to use more efficient data structures for your buffer pool manager, you are free to do
 * so. However, you will likely benefit significantly from detecting buffer overflow in future projects (especially
 * project 2).
 */
class FrameHeader {
  friend class DiskManagerProxy;
  friend class BufferPoolManager;
  friend class ReadPageGuard;
  friend class WritePageGuard;
  friend class PageGuard;

 public:
  explicit FrameHeader(frame_id_t frame_id);

 private:
  auto GetData() const -> const char *;
  auto GetDataMut() -> char *;
  void Reset();

  /** @brief The frame ID / index of the frame this header represents. */
  const frame_id_t frame_id_;

  /** @brief The readers / writer latch for this frame. */
  std::shared_mutex rwlatch_;

  /** @brief The number of pins on this frame keeping the page in memory. */
  std::atomic<size_t> pin_count_;

  /** @brief The dirty flag. */
  std::atomic<bool> is_dirty_;

  /**
   * @brief A pointer to the data of the page that this frame holds.
   *
   * If the frame does not hold any page data, the frame contains all null bytes.
   */
  std::vector<char> data_;

  /**
   * TODO(P1): You may add any fields or helper functions under here that you think are necessary.
   *
   * One potential optimization you could make is storing an optional page ID of the page that the `FrameHeader` is
   * currently storing. This might allow you to skip searching for the corresponding (page ID, frame ID) pair somewhere
   * else in the buffer pool manager...
   */
  page_id_t pgid_{INVALID_PAGE_ID};

  bool iswriting_{false};

  bool isloading_{false};

  // 控制iswrting 和 isloading的变量
  std::condition_variable frame_cv_;

  // 帧的互斥变量 用于控制条件变量
  std::mutex io_latch_;
};

// 线程池实例化防止被多次调用
static inline std::once_flag thread_once;

class ThreadPool {
 public:
  static auto GetInstance(int num = 32) -> std::shared_ptr<ThreadPool> {
    std::call_once(thread_once, InitInstance, num);
    return pool;
  }

  ThreadPool(const ThreadPool &that) = delete;
  auto operator=(const ThreadPool &that) = delete;

  explicit ThreadPool(int num) {
    for (int i = 0; i < num; i++) {
      threads_.emplace_back([this]() -> void {
        while (true) {
          // 执行的时候保护
          std::function<void()> task;
          {
            // 无任务则睡眠 否则唤醒执行任务
            std::unique_lock latch(task_que_mtx_);
            // 谓词表达: 防止虚假唤醒 允许结束条件是队列空或线程池停止
            cv_.wait(latch, [this]() -> bool { return (!task_que_.empty() || stop_flag_); });
            if (stop_flag_ && task_que_.empty()) {
              return;
            }
            task = std::move(task_que_.front());
            task_que_.pop();
          }
          try {
            task();
          } catch (const std::exception &e) {
            std::cerr << "线程池执行任务时发生错误:" << e.what() << std::endl;
          }
        }
      });
    }
  }

  ~ThreadPool() {
    {
      std::unique_lock latch(task_que_mtx_);
      stop_flag_ = true;
    }

    cv_.notify_all();
    // 逐个等待结束
    for (auto &thread : threads_) {
      thread.join();
    }
  }

  template <class Func, class... Args>
  void PushtTask(Func &&funcname, Args &&...funcargs) {
    // bind主要是为了生成一个新的无参无返回值的函数
    // 和function<void()> 对应
    // std::bind(std::forward<Func>(funcname), std::forward<Args>(funcargs)...);
    // 更现代的打包函数方式：lamada表达式捕获参数 apply展开函数参数
    // 从而使的参数通过完美转发可应用传递到实际的传入函数
    auto task = [func = std::forward<Func>(funcname), args = std::make_tuple(std::forward<Args>(funcargs)...)]() {
      return std::apply(func, args);
    };

    {
      std::unique_lock latch(task_que_mtx_);
      task_que_.push(std::move(task));
    }
    cv_.notify_one();
  }

 private:

  static auto InitInstance(int num) -> void {
    if (pool == nullptr) {
      pool = std::make_shared<ThreadPool>(num);
    }
  }

  // 单例模式线程池
  static std::shared_ptr<ThreadPool> pool;

  std::vector<std::thread> threads_;

  std::queue<std::function<void()>> task_que_;

  std::mutex task_que_mtx_;

  std::condition_variable cv_;

  bool stop_flag_{false};
};

class DiskManagerProxy {
 public:
  explicit DiskManagerProxy(std::shared_ptr<DiskScheduler> &disk_scheduler, size_t init_size = 1024 * 512);

  void ScheduleProxy(std::shared_ptr<FrameHeader> &frame, bool iswrite, page_id_t oldpgid,
                     const std::vector<char> &dirty_data);

  void Deletepagecache(std::shared_ptr<FrameHeader> &frame);

  ~DiskManagerProxy() = default;

 private:
  struct ProxyFrame {
    
    ProxyFrame(std::shared_ptr<FrameHeader> &frame, bool iswrite, page_id_t oldpgid);

    // 防止单参数直接隐式转换为结构体对象
    ProxyFrame(ProxyFrame &&that) noexcept;

    auto operator=(ProxyFrame &&that) noexcept -> ProxyFrame &;

    ProxyFrame() = default;

    bool iswrite_{false};

    std::shared_ptr<FrameHeader> frame_;

    page_id_t oldpgid_{INVALID_PAGE_ID};
  };

  auto WorkThread(page_id_t oldpgid, bool iswrite) -> void;

  // 每个页面的缓存
  std::unordered_map<page_id_t, std::vector<char>> page_cache_;
  // 每个页面的缓存标记
  std::unordered_map<page_id_t, bool> page_cache_valid_;
  // 每个页面的锁
  std::unordered_map<page_id_t, std::mutex> page_mtx_;
  // 共用的disk_scheduler
  std::shared_ptr<DiskScheduler> &disk_scheduler_;
  // 管理的内存池
  std::shared_ptr<ThreadPool> thread_pool_;
};

/**
 * @brief The declaration of the `BufferPoolManager` class.
 *
 * As stated in the writeup, the buffer pool is responsible for moving physical pages of data back and forth from
 * buffers in main memory to persistent storage. It also behaves as a cache, keeping frequently used pages in memory for
 * faster access, and evicting unused or cold pages back out to storage.
 *
 * Make sure you read the writeup in its entirety before attempting to implement the buffer pool manager. You also need
 * to have completed the implementation of both the `LRUKReplacer` and `DiskManager` classes.
 */
class BufferPoolManager {
 public:
  BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist = LRUK_REPLACER_K,
                    LogManager *log_manager = nullptr);
  ~BufferPoolManager();

  auto Size() const -> size_t;
  auto NewPage() -> page_id_t;
  auto DeletePage(page_id_t page_id) -> bool;
  auto CheckedWritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown)
      -> std::optional<WritePageGuard>;
  auto CheckedReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> std::optional<ReadPageGuard>;
  auto WritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> WritePageGuard;
  auto ReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> ReadPageGuard;
  auto FlushPageUnsafe(page_id_t page_id) -> bool;
  auto FlushPage(page_id_t page_id) -> bool;
  void FlushAllPagesUnsafe();
  void FlushAllPages();
  auto GetPinCount(page_id_t page_id) -> std::optional<size_t>;
  void Loaddatafromdisk(std::shared_ptr<FrameHeader> &curframe) const;
  void Cleandirtyframe(std::shared_ptr<FrameHeader> &curframe, page_id_t oldpgid);

 private:
  /** @brief The number of frames in the buffer pool. */
  const size_t num_frames_;

  /** @brief The next page ID to be allocated.  */
  std::atomic<page_id_t> next_page_id_;

  /**
   * @brief The latch protecting the buffer pool's inner data structures.
   *
   * TODO(P1) We recommend replacing this comment with details about what this latch actually protects.
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /** @brief The frame headers of the frames that this buffer pool manages. */
  std::vector<std::shared_ptr<FrameHeader>> frames_;

  /** @brief The page table that keeps track of the mapping between pages and buffer pool frames. */
  std::unordered_map<page_id_t, frame_id_t> page_table_;

  /** @brief A list of free frames that do not hold any page's data. */
  std::list<frame_id_t> free_frames_;

  /** @brief The replacer to find unpinned / candidate pages for eviction. */
  std::shared_ptr<LRUKReplacer> replacer_;

  /** @brief A pointer to the disk scheduler. Shared with the page guards for flushing. */
  std::shared_ptr<DiskScheduler> disk_scheduler_;

  /**
   * @brief A pointer to the log manager.
   *
   * Note: Please ignore this for P1.
   */
  LogManager *log_manager_ __attribute__((__unused__));

  /**
   * TODO(P1): You may add additional private members and helper functions if you find them necessary.
   *
   * There will likely be a lot of code duplication between the different modes of accessing a page.
   *
   * We would recommend implementing a helper function that returns the ID of a frame that is free and has nothing
   * stored inside of it. Additionally, you may also want to implement a helper function that returns either a shared
   * pointer to a `FrameHeader` that already has a page's data stored inside of it, or an index to said `FrameHeader`.
   */
  std::shared_ptr<DiskManagerProxy> disk_manger_proxy_;
};

}  // namespace bustub
