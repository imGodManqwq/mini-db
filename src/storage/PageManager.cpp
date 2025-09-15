#include "../../include/storage/PageManager.h"
#include <iostream>
#include <algorithm>
#include <stdexcept>

PageManager::PageManager(const std::string& dbFileName, size_t bufferPoolSize) 
    : dbFileName_(dbFileName), nextPageId_(1) {
    freePageBitmap_.resize(1000, true); // 初始支持1000页
    freePageBitmap_[0] = false; // 页面ID从1开始
    bufferPool_ = std::make_unique<BufferPool>(bufferPoolSize);
    openFile();
}

PageManager::~PageManager() {
    flushAllPages();
    closeFile();
}

uint32_t PageManager::allocatePage(PageType type) {
    uint32_t pageId = findFreePageId();
    if (pageId == 0) {
        // 扩展位图
        size_t oldSize = freePageBitmap_.size();
        freePageBitmap_.resize(oldSize * 2, true);
        pageId = oldSize;
    }
    
    markPageUsed(pageId);
    
    // 创建新页面
    auto page = std::make_shared<Page>(pageId, type);
    bufferPool_->putPage(page);
    
    if (pageId >= nextPageId_) {
        nextPageId_ = pageId + 1;
    }
    
    return pageId;
}

void PageManager::deallocatePage(uint32_t pageId) {
    if (pageId == 0 || pageId >= freePageBitmap_.size()) {
        return;
    }
    
    markPageFree(pageId);
    
    // 从缓冲池中刷新并移除（如果存在）
    if (bufferPool_->isPageInPool(pageId)) {
        bufferPool_->flushPage(pageId);
    }
}

std::shared_ptr<Page> PageManager::getPage(uint32_t pageId) {
    if (pageId == 0) {
        return nullptr;
    }
    
    // 先从缓冲池中获取
    auto page = bufferPool_->getPage(pageId);
    if (page) {
        return page;
    }
    
    // 从磁盘加载
    std::vector<uint8_t> data;
    if (readPageFromDisk(pageId, data)) {
        auto diskPage = Page::deserialize(data);
        if (diskPage) {
            auto sharedPage = std::shared_ptr<Page>(diskPage.release());
            bufferPool_->putPage(sharedPage);
            return sharedPage;
        }
    }
    
    return nullptr;
}

bool PageManager::writePage(std::shared_ptr<Page> page) {
    if (!page) {
        return false;
    }
    
    uint32_t pageId = page->getPageId();
    
    // 放入缓冲池
    bufferPool_->putPage(page);
    
    // 写入磁盘
    auto data = page->serialize();
    return writePageToDisk(pageId, data);
}

bool PageManager::flushPage(uint32_t pageId) {
    return bufferPool_->flushPage(pageId);
}

void PageManager::flushAllPages() {
    bufferPool_->flushAllPages();
}

bool PageManager::pageExists(uint32_t pageId) const {
    return pageId > 0 && 
           pageId < freePageBitmap_.size() && 
           !freePageBitmap_[pageId];
}

size_t PageManager::getTotalPages() const {
    return std::count(freePageBitmap_.begin(), freePageBitmap_.end(), false);
}

size_t PageManager::getFreePages() const {
    return std::count(freePageBitmap_.begin(), freePageBitmap_.end(), true);
}

bool PageManager::loadFromDisk() {
    if (!dbFile_.is_open()) {
        return false;
    }
    
    // 读取元数据（简化实现）
    dbFile_.seekg(0, std::ios::beg);
    
    // 这里可以实现更复杂的元数据加载逻辑
    return true;
}

bool PageManager::saveToDisk() {
    flushAllPages();
    
    if (dbFile_.is_open()) {
        dbFile_.flush();
        return true;
    }
    return false;
}

const BufferPoolStats& PageManager::getBufferPoolStats() const {
    return bufferPool_->getStats();
}

void PageManager::printBufferPoolStats() const {
    bufferPool_->printStats();
}

void PageManager::resetBufferPoolStats() {
    bufferPool_->resetStats();
}

void PageManager::printStatistics() const {
    std::cout << "PageManager Statistics:" << std::endl;
    std::cout << "  Database file: " << dbFileName_ << std::endl;
    std::cout << "  Total pages: " << getTotalPages() << std::endl;
    std::cout << "  Free pages: " << getFreePages() << std::endl;
    std::cout << "  Next page ID: " << nextPageId_ << std::endl;
    
    std::cout << std::endl;
    printBufferPoolStats();
}

bool PageManager::openFile() {
    dbFile_.open(dbFileName_, std::ios::in | std::ios::out | std::ios::binary);
    
    if (!dbFile_.is_open()) {
        // 文件不存在，创建新文件
        dbFile_.open(dbFileName_, std::ios::out | std::ios::binary);
        if (dbFile_.is_open()) {
            dbFile_.close();
            dbFile_.open(dbFileName_, std::ios::in | std::ios::out | std::ios::binary);
        }
    }
    
    return dbFile_.is_open();
}

void PageManager::closeFile() {
    if (dbFile_.is_open()) {
        dbFile_.close();
    }
}

bool PageManager::readPageFromDisk(uint32_t pageId, std::vector<uint8_t>& data) {
    if (!dbFile_.is_open()) {
        return false;
    }
    
    std::streampos pos = static_cast<std::streampos>(pageId - 1) * PAGE_SIZE;
    dbFile_.seekg(pos);
    
    data.resize(PAGE_SIZE);
    dbFile_.read(reinterpret_cast<char*>(data.data()), PAGE_SIZE);
    
    return dbFile_.gcount() == PAGE_SIZE;
}

bool PageManager::writePageToDisk(uint32_t pageId, const std::vector<uint8_t>& data) {
    if (!dbFile_.is_open() || data.size() != PAGE_SIZE) {
        return false;
    }
    
    std::streampos pos = static_cast<std::streampos>(pageId - 1) * PAGE_SIZE;
    dbFile_.seekp(pos);
    dbFile_.write(reinterpret_cast<const char*>(data.data()), PAGE_SIZE);
    dbFile_.flush();
    
    return dbFile_.good();
}

void PageManager::markPageUsed(uint32_t pageId) {
    if (pageId < freePageBitmap_.size()) {
        freePageBitmap_[pageId] = false;
    }
}

void PageManager::markPageFree(uint32_t pageId) {
    if (pageId < freePageBitmap_.size()) {
        freePageBitmap_[pageId] = true;
    }
}

uint32_t PageManager::findFreePageId() {
    for (size_t i = 1; i < freePageBitmap_.size(); ++i) {
        if (freePageBitmap_[i]) {
            return static_cast<uint32_t>(i);
        }
    }
    return 0; // 没有找到空闲页面
}
