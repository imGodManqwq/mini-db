#pragma once
#include "Page.h"
#include <unordered_map>
#include <list>
#include <memory>
#include <mutex>

// 缓冲池帧结构
struct BufferFrame {
    std::shared_ptr<Page> page;
    uint32_t pageId;
    bool isDirty;           // 脏页标记
    bool isPinned;          // 是否被固定（正在使用）
    int pinCount;           // 引用计数
    
    BufferFrame() : pageId(0), isDirty(false), isPinned(false), pinCount(0) {}
    BufferFrame(std::shared_ptr<Page> p, uint32_t id) 
        : page(p), pageId(id), isDirty(false), isPinned(false), pinCount(0) {}
};

// 缓冲池统计信息
struct BufferPoolStats {
    size_t totalFrames;     // 总帧数
    size_t usedFrames;      // 已使用帧数
    size_t hitCount;        // 命中次数
    size_t missCount;       // 未命中次数
    size_t evictionCount;   // 淘汰次数
    
    BufferPoolStats() : totalFrames(0), usedFrames(0), hitCount(0), missCount(0), evictionCount(0) {}
    
    double getHitRatio() const {
        size_t total = hitCount + missCount;
        return total > 0 ? static_cast<double>(hitCount) / total : 0.0;
    }
};

class BufferPool {
public:
    explicit BufferPool(size_t poolSize = 128); // 默认128帧
    ~BufferPool();
    
    // 页面操作
    std::shared_ptr<Page> getPage(uint32_t pageId);
    bool putPage(std::shared_ptr<Page> page);
    bool flushPage(uint32_t pageId);
    void flushAllPages();
    
    // 页面固定/解除固定
    bool pinPage(uint32_t pageId);
    bool unpinPage(uint32_t pageId);
    
    // 缓冲池管理
    bool evictPage();
    void clearPool();
    
    // 统计信息
    const BufferPoolStats& getStats() const;
    void resetStats();
    void printStats() const;
    
    // 调试功能
    void printPoolStatus() const;
    bool isPageInPool(uint32_t pageId) const;
    
private:
    size_t poolSize_;
    std::unordered_map<uint32_t, std::shared_ptr<BufferFrame>> frameTable_; // 页面ID到帧的映射
    std::list<uint32_t> lruList_;       // LRU链表，存储页面ID
    std::unordered_map<uint32_t, std::list<uint32_t>::iterator> lruIterators_; // 页面ID到LRU迭代器的映射
    
    BufferPoolStats stats_;
    mutable std::mutex mutex_;          // 线程安全
    
    // LRU操作
    void moveToFront(uint32_t pageId);
    void removeFromLRU(uint32_t pageId);
    void addToFront(uint32_t pageId);
    
    // 内部辅助方法
    std::shared_ptr<BufferFrame> findVictimFrame();
    bool writeBackPage(std::shared_ptr<BufferFrame> frame);
};
