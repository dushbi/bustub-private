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

// 驱逐帧
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  // 如果没有可以驱逐元素
  if (curr_size_ == 0) {
    return false;
  }
  // 看访问历史列表里，有无帧可以删除
  for (auto it = new_frame_.rbegin(); it != new_frame_.rend(); it++) {
    auto frame = *it;
    // 如果可以被删除
    if (evictable_[frame]) {
      recorded_cnt_[frame] = 0;
      new_locate_.erase(frame);
      new_frame_.remove(frame);
      curr_size_--;
      time_frame_[frame].clear();
      *frame_id = frame;
      return true;
    }
  }
  // 看缓存队列里有无帧可以删除
  for (auto its = cache_frame_.begin(); its != cache_frame_.end(); its++) {
    auto frames = (*its).first;
    if (evictable_[frames]) {
      recorded_cnt_[frames] = 0;
      cache_frame_.erase(its);
      cache_locate_.erase(frames);
      curr_size_--;
      time_frame_[frames].clear();
      *frame_id = frames;
      return true;
    }
  }
  return false;
}


// 访问逻辑：不满k次放访问历史列表。。。。
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_typ) {
  std::lock_guard<std::mutex> lock(latch_);
  // 如果越界
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  current_timestamp_++;
  recorded_cnt_[frame_id]++;
  auto cnt = recorded_cnt_[frame_id];
  // 在访问时间列表尾部加新的时间戳，旧时间在前，新时间在后
  time_frame_[frame_id].push_back(current_timestamp_);
  // 如果是新加入的记录
  if (cnt == 1) {
    if (curr_size_ == max_size_) {
      frame_id_t frame;
      Evict(&frame);
    }
    evictable_[frame_id] = true;
    curr_size_++;
    // 添加新节点
    new_frame_.push_front(frame_id);
    // 该节点下维护链表
    new_locate_[frame_id] = new_frame_.begin();
  }
  // 如果记录达到k次，则需要从新队列中加入到老队列中
  if (cnt == k_) {
    new_frame_.erase(new_locate_[frame_id]);  // 从新队列中删除
    new_locate_.erase(frame_id);
    auto kth_time = time_frame_[frame_id].front();  // 获取当前页面的倒数第k次出现的时间
    k_time new_cache(frame_id, kth_time);
    auto it = std::upper_bound(cache_frame_.begin(),    	cache_frame_.end(), new_cache, CmpTimestamp);  
    // 找到该插入的位置
    it = cache_frame_.insert(it, new_cache);
    cache_locate_[frame_id] = it;
    return;
  }
  // 如果记录在k次以上，需要将该frame放到指定的位置
  if (cnt > k_) {
    time_frame_[frame_id].erase(time_frame_[frame_id].begin());
    // 去除原来的位置
    cache_frame_.erase(cache_locate_[frame_id]);
    // 获取当前页面的倒数第k次出现的时间
    auto kth_time = time_frame_[frame_id].front();
    k_time new_cache(frame_id, kth_time);
    // 找到该插入的位置
    auto it = std::upper_bound(cache_frame_.begin(), cache_frame_.end(), new_cache, CmpTimestamp);
    it = cache_frame_.insert(it, new_cache);
    cache_locate_[frame_id] = it;
    return;
  }
}



void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (recorded_cnt_[frame_id] == 0) {
    return;
  }
  // true是保留,false是驱逐
  if (!evictable_[frame_id]) {
    // 原本不扔，要求扔
    if (set_evictable) {
      ++max_size_;
      ++curr_size_;
    }
  } else {
    if (!set_evictable) {
      // 原本扔，要求不扔
      --max_size_;
      --curr_size_;
    }
  }
  evictable_[frame_id] = set_evictable;
}


// 移除页面
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  auto cnt = recorded_cnt_[frame_id];
  if (cnt == 0) {
    return;
  }
  if (!evictable_[frame_id]) {
    throw std::exception();
  }
  // 在访问历史列表里
  if (cnt < k_) {
    recorded_cnt_[frame_id] = 0;
    new_frame_.erase(new_locate_[frame_id]);
    new_locate_.erase(frame_id);
    --curr_size_;
    time_frame_[frame_id].clear();
  } else {  // 在缓存队列里
    recorded_cnt_[frame_id] = 0;
    cache_frame_.erase(cache_locate_[frame_id]);
    cache_locate_.erase(frame_id);
    --curr_size_;
    time_frame_[frame_id].clear();
  }
}


auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  return curr_size_;
}
auto LRUKReplacer::CmpTimestamp(const LRUKReplacer::k_time &f1, const LRUKReplacer::k_time &f2) -> bool {
  return f1.second < f2.second;
}

}  // namespace bustub

