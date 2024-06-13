//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  if (node_store_.empty()) {
    return false;
  }
  size_t max_distance = 0;
  bool found = false;
  frame_id_t candidate_frame_id = -1;

  for (auto &pair : node_store_) {
    const auto &node = pair.second;
    if (!node.is_evictable_) {
      continue;
    }
    size_t distance;
    if (node.history_.size() < k_) {
      distance = std::numeric_limits<size_t>::max();  // 少于 k 次访问视为无穷大
    } else {
      distance = current_timestamp_ - node.history_.front();
    }

    if (distance > max_distance) {
      max_distance = distance;
      candidate_frame_id = pair.first;
      found = true;
    } else if (distance == max_distance && candidate_frame_id > pair.first) {
      // 在后向 k 距离相同的情况下，选择帧 ID 更小的帧（即最近最少使用的帧）
      candidate_frame_id = pair.first;
    }
  }

  if (found) {
    node_store_.erase(candidate_frame_id);
    *frame_id = candidate_frame_id;
    curr_size_--;
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id,  AccessType access_type) {
  std::lock_guard<std::mutex> guard(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    LRUKNode new_node;
    new_node.k_ = k_;  // 假设k_是LRUKNode需要的参数之一
    // 可能还需要设置其他LRUKNode的初始值
    node_store_[frame_id] = new_node;
  }

  current_timestamp_++;
  auto &node = node_store_[frame_id];
  if (node.history_.size() >= k_) {
    node.history_.pop_front();
  }
  node.history_.push_back(current_timestamp_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    throw Exception("Frame ID is invalid");
  }

  auto &node = node_store_[frame_id];
  if (node.is_evictable_ != set_evictable) {
    node.is_evictable_ = set_evictable;
    curr_size_ += set_evictable ? 1 : -1;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  auto &node = node_store_[frame_id];
  if (!node.is_evictable_) {
    throw Exception("Frame is not evictable");
  }

  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub

