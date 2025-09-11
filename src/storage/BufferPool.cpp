#include "../../include/storage/BufferPool.h"
#include <iostream>
#include <algorithm>
#include <iomanip>

BufferPool::BufferPool(size_t poolSize) : poolSize_(poolSize) {
    stats_.totalFrames = poolSize;
}

BufferPool::~BufferPool() {
    flushAllPages();
}

std::shared_ptr<Page> BufferPool::getPage(uint32_t pageId) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // 检查页面是否在缓冲池中
    auto it = frameTable_.find(pageId);
    if (it != frameTable_.end()) {
        // 缓存命中
        stats_.hitCount++;
        auto frame = it->second;
        frame->pinCount++;
        frame->isPinned = true;
        
        // 移动到LRU链表前端
        moveToFront(pageId);
        
        return frame->page;
    }
    
    // 缓存未命中
    stats_.missCount++;
    
    // 检查缓冲池是否已满
    if (frameTable_.size() >= poolSize_) {
        // 需要淘汰页面
        if (!evictPage()) {
            std::cerr << "Failed to evict page from buffer pool" << std::endl;
            return nullptr;
        }
    }
    
    // 这里应该从磁盘加载页面，暂时创建一个新页面
    auto page = std::make_shared<Page>(pageId);
    auto frame = std::make_shared<BufferFrame>(page, pageId);
    frame->pinCount = 1;
    frame->isPinned = true;
    
    // 添加到缓冲池
    frameTable_[pageId] = frame;
    addToFront(pageId);
    stats_.usedFrames++;
    
    return page;
}

bool BufferPool::putPage(std::shared_ptr<Page> page) {
    if (!page) return false;
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    uint32_t pageId = page->getPageId();
    auto it = frameTable_.find(pageId);
    
    if (it != frameTable_.end()) {
        // 页面已在缓冲池中，更新内容
        it->second->page = page;
        it->second->isDirty = true;
        moveToFront(pageId);
        return true;
    }
    
    // 页面不在缓冲池中，需要添加
    if (frameTable_.size() >= poolSize_) {
        if (!evictPage()) {
            return false;
        }
    }
    
    auto frame = std::make_shared<BufferFrame>(page, pageId);
    frame->isDirty = true;
    frameTable_[pageId] = frame;
    addToFront(pageId);
    stats_.usedFrames++;
    
    return true;
}

bool BufferPool::flushPage(uint32_t pageId) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = frameTable_.find(pageId);
    if (it == frameTable_.end()) {
        return false;
    }
    
    auto frame = it->second;
    if (frame->isDirty) {
        // 这里应该写回磁盘，暂时只是标记为非脏页
        if (writeBackPage(frame)) {
            frame->isDirty = false;
            return true;
        }
        return false;
    }
    
    return true; // 非脏页，无需写回
}

void BufferPool::flushAllPages() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    for (auto& pair : frameTable_) {
        auto frame = pair.second;
        if (frame->isDirty) {
            writeBackPage(frame);
            frame->isDirty = false;
        }
    }
}

bool BufferPool::pinPage(uint32_t pageId) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = frameTable_.find(pageId);
    if (it == frameTable_.end()) {
        return false;
    }
    
    auto frame = it->second;
    frame->pinCount++;
    frame->isPinned = true;
    
    return true;
}

bool BufferPool::unpinPage(uint32_t pageId) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = frameTable_.find(pageId);
    if (it == frameTable_.end()) {
        return false;
    }
    
    auto frame = it->second;
    if (frame->pinCount > 0) {
        frame->pinCount--;
        if (frame->pinCount == 0) {
            frame->isPinned = false;
        }
    }
    
    return true;
}

bool BufferPool::evictPage() {
    // 查找可以淘汰的页面
    auto victimFrame = findVictimFrame();
    if (!victimFrame) {
        return false; // 没有可淘汰的页面
    }
    
    uint32_t victimPageId = victimFrame->pageId;
    
    // 如果是脏页，需要写回磁盘
    if (victimFrame->isDirty) {
        if (!writeBackPage(victimFrame)) {
            std::cerr << "Failed to write back dirty page " << victimPageId << std::endl;
            return false;
        }
    }
    
    // 从缓冲池中移除
    frameTable_.erase(victimPageId);
    removeFromLRU(victimPageId);
    stats_.usedFrames--;
    stats_.evictionCount++;
    
    return true;
}

void BufferPool::clearPool() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    flushAllPages();
    frameTable_.clear();
    lruList_.clear();
    lruIterators_.clear();
    stats_.usedFrames = 0;
}

const BufferPoolStats& BufferPool::getStats() const {
    return stats_;
}

void BufferPool::resetStats() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    stats_.hitCount = 0;
    stats_.missCount = 0;
    stats_.evictionCount = 0;
}

void BufferPool::printStats() const {
    std::cout << "Buffer Pool Statistics:" << std::endl;
    std::cout << "  Total Frames: " << stats_.totalFrames << std::endl;
    std::cout << "  Used Frames: " << stats_.usedFrames << std::endl;
    std::cout << "  Hit Count: " << stats_.hitCount << std::endl;
    std::cout << "  Miss Count: " << stats_.missCount << std::endl;
    std::cout << "  Eviction Count: " << stats_.evictionCount << std::endl;
    std::cout << "  Hit Ratio: " << std::fixed << std::setprecision(2) 
              << (stats_.getHitRatio() * 100) << "%" << std::endl;
}

void BufferPool::printPoolStatus() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::cout << "Buffer Pool Status:" << std::endl;
    std::cout << "  Pool Size: " << poolSize_ << std::endl;
    std::cout << "  Used Frames: " << frameTable_.size() << std::endl;
    
    std::cout << "  Pages in pool: ";
    for (const auto& pair : frameTable_) {
        const auto& frame = pair.second;
        std::cout << pair.first;
        if (frame->isDirty) std::cout << "(D)";
        if (frame->isPinned) std::cout << "(P" << frame->pinCount << ")";
        std::cout << " ";
    }
    std::cout << std::endl;
    
    std::cout << "  LRU order: ";
    for (auto pageId : lruList_) {
        std::cout << pageId << " ";
    }
    std::cout << std::endl;
}

bool BufferPool::isPageInPool(uint32_t pageId) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return frameTable_.find(pageId) != frameTable_.end();
}

void BufferPool::moveToFront(uint32_t pageId) {
    removeFromLRU(pageId);
    addToFront(pageId);
}

void BufferPool::removeFromLRU(uint32_t pageId) {
    auto it = lruIterators_.find(pageId);
    if (it != lruIterators_.end()) {
        lruList_.erase(it->second);
        lruIterators_.erase(it);
    }
}

void BufferPool::addToFront(uint32_t pageId) {
    lruList_.push_front(pageId);
    lruIterators_[pageId] = lruList_.begin();
}

std::shared_ptr<BufferFrame> BufferPool::findVictimFrame() {
    // 从LRU链表后端开始查找未固定的页面
    for (auto it = lruList_.rbegin(); it != lruList_.rend(); ++it) {
        uint32_t pageId = *it;
        auto frameIt = frameTable_.find(pageId);
        if (frameIt != frameTable_.end()) {
            auto frame = frameIt->second;
            if (!frame->isPinned) {
                return frame;
            }
        }
    }
    
    return nullptr; // 没有找到可淘汰的页面
}

bool BufferPool::writeBackPage(std::shared_ptr<BufferFrame> frame) {
    // 这里应该实现实际的磁盘写入操作
    // 暂时只是模拟写回成功
    std::cout << "Writing back page " << frame->pageId << " to disk" << std::endl;
    return true;
}
