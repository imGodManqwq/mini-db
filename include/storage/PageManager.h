#pragma once
#include "Page.h"
#include "BufferPool.h"
#include <unordered_map>
#include <memory>
#include <string>
#include <fstream>
#include <vector>

class PageManager {
public:
    explicit PageManager(const std::string& dbFileName, size_t bufferPoolSize = 128);
    ~PageManager();
    
    // 页面分配和释放
    uint32_t allocatePage(PageType type = PageType::DATA_PAGE);
    void deallocatePage(uint32_t pageId);
    
    // 页面读写
    std::shared_ptr<Page> getPage(uint32_t pageId);
    bool writePage(std::shared_ptr<Page> page);
    bool flushPage(uint32_t pageId);
    void flushAllPages();
    
    // 页面管理
    bool pageExists(uint32_t pageId) const;
    size_t getTotalPages() const;
    size_t getFreePages() const;
    
    // 持久化
    bool loadFromDisk();
    bool saveToDisk();
    
    // 缓冲池操作
    const BufferPoolStats& getBufferPoolStats() const;
    void printBufferPoolStats() const;
    void resetBufferPoolStats();
    
    // 统计信息
    void printStatistics() const;
    
private:
    std::string dbFileName_;
    std::fstream dbFile_;
    uint32_t nextPageId_;
    std::vector<bool> freePageBitmap_;  // 空闲页位图
    std::unique_ptr<BufferPool> bufferPool_; // 缓冲池
    
    // 文件操作
    bool openFile();
    void closeFile();
    bool readPageFromDisk(uint32_t pageId, std::vector<uint8_t>& data);
    bool writePageToDisk(uint32_t pageId, const std::vector<uint8_t>& data);
    
    // 位图操作
    void markPageUsed(uint32_t pageId);
    void markPageFree(uint32_t pageId);
    uint32_t findFreePageId();
};
