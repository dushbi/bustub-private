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
    std::lock_guard<std::mutex> lock(lock_guard_);
    if(curr_size==0)    return false;
    for(auto it = new_frame_.rbegin(); it!=new_frame_.rend();it++){
        auto frame = *it;
        if(evictable_[frame]){
            recorded_cnt_[frame]=0;
            new_locate_.erase(frame);
            new_frame.remove(frame);
            curr_size_--;
            time_frame_[frame].clear();
            *frame_id = frame;
            return true;
        }
    }
    for(auto it = cache_frame_.begin();it!=cache_frame_.end();it++){
        auto frames = (*its).first;
        if(evictable_[frames]){
            recorded_cnt_[frame]=0;
            cache_frame_.erase(it);
            cache_locate_.erase(frames);
            curr_size_--;
            time_frame_[frames].clear();
            *frame_id = frames;
            return true;
        }
    }
    return false; 
    }

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return 0; }

}  // namespace bustub
