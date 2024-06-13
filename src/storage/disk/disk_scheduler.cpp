//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
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

void DiskScheduler::Schedule(DiskRequest r) {
  // 创建 promise 并获取对应的 future
  request_queue_.Put(std::make_optional(std::move(r)));
}

void DiskScheduler::StartWorkerThread() {
  while (true) {
    auto request_opt = request_queue_.Get();
    // 检查是否应该停止线程
    if (!request_opt.has_value()) {
      break;
    }
    // 处理磁盘请求
    DiskRequest &request = request_opt.value();
    if (request.is_write_) {
      disk_manager_->WritePage(request.page_id_, request.data_);
    } else {
      disk_manager_->ReadPage(request.page_id_, request.data_);
    }

    // 设置回调标志
    request.callback_.set_value(true);
  }
}

}  // namespace bustub

