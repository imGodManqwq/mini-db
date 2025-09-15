#include "../../include/storage/Page.h"
#include <iostream>
#include <cstring>
#include <algorithm>
#include <stdexcept>
#include <climits>

Page::Page(uint32_t pageId, PageType type) {
    header_.pageId = pageId;
    header_.pageType = type;
    header_.slotCount = 0;
    header_.freeSpaceOffset = PAGE_HEADER_SIZE;
    header_.freeSpaceSize = PAGE_DATA_SIZE;
    header_.checksum = 0;
    header_.lsn = 0;
    
    data_.resize(PAGE_SIZE);
    initializePage();
}

uint32_t Page::getPageId() const {
    return header_.pageId;
}

PageType Page::getPageType() const {
    return header_.pageType;
}

bool Page::insertRecord(const std::string& record) {
    size_t recordSize = record.size() + sizeof(uint16_t); // 记录 + 长度字段
    
    if (!hasSpace(recordSize)) {
        return false;
    }
    
    // 找到空闲槽位
    uint16_t slotId = findFreeSlot();
    
    // 写入记录
    uint16_t offset = header_.freeSpaceOffset;
    
    // 写入记录长度
    *reinterpret_cast<uint16_t*>(&data_[offset]) = static_cast<uint16_t>(record.size());
    offset += sizeof(uint16_t);
    
    // 写入记录数据
    std::memcpy(&data_[offset], record.data(), record.size());
    
    // 更新槽位目录
    if (slotId >= slots_.size()) {
        slots_.resize(slotId + 1, 0);
    }
    slots_[slotId] = header_.freeSpaceOffset;
    
    // 更新页头
<<<<<<< HEAD
    header_.freeSpaceOffset += recordSize;
    header_.freeSpaceSize -= recordSize;
=======
    header_.freeSpaceOffset += static_cast<uint16_t>(recordSize);
    header_.freeSpaceSize -= static_cast<uint16_t>(recordSize);
>>>>>>> origin/storage
    if (slotId >= header_.slotCount) {
        header_.slotCount = slotId + 1;
    }
    
    updateChecksum();
    return true;
}

uint16_t Page::insertRecordAndReturnSlot(const std::string& record) {
    size_t recordSize = record.size() + sizeof(uint16_t); // 记录 + 长度字段
    
    if (!hasSpace(recordSize)) {
        return UINT16_MAX; // 表示失败
    }
    
    // 找到空闲槽位
    uint16_t slotId = findFreeSlot();
    
    // 写入记录
    uint16_t offset = header_.freeSpaceOffset;
    
    // 写入记录长度
    *reinterpret_cast<uint16_t*>(&data_[offset]) = static_cast<uint16_t>(record.size());
    offset += sizeof(uint16_t);
    
    // 写入记录数据
    std::memcpy(&data_[offset], record.data(), record.size());
    
    // 更新槽位目录
    if (slotId >= slots_.size()) {
        slots_.resize(slotId + 1, 0);
    }
    slots_[slotId] = header_.freeSpaceOffset;
    
    // 更新页头
<<<<<<< HEAD
    header_.freeSpaceOffset += recordSize;
    header_.freeSpaceSize -= recordSize;
=======
    header_.freeSpaceOffset += static_cast<uint16_t>(recordSize);
    header_.freeSpaceSize -= static_cast<uint16_t>(recordSize);
>>>>>>> origin/storage
    if (slotId >= header_.slotCount) {
        header_.slotCount = slotId + 1;
    }
    
    updateChecksum();
    return slotId;
}

std::string Page::getRecord(uint16_t slotId) const {
    if (slotId >= slots_.size() || slots_[slotId] == 0) {
        return "";
    }
    
    uint16_t offset = slots_[slotId];
    uint16_t recordSize = *reinterpret_cast<const uint16_t*>(&data_[offset]);
    offset += sizeof(uint16_t);
    
    return std::string(reinterpret_cast<const char*>(&data_[offset]), recordSize);
}

bool Page::deleteRecord(uint16_t slotId) {
    if (slotId >= slots_.size() || slots_[slotId] == 0) {
        return false;
    }
    
    // 标记槽位为空
    slots_[slotId] = 0;
    
    // 压缩页面（简单实现：重新组织所有记录）
    compactPage();
    
    updateChecksum();
    return true;
}

bool Page::updateRecord(uint16_t slotId, const std::string& newRecord) {
    if (!deleteRecord(slotId)) {
        return false;
    }
    return insertRecord(newRecord);
}

size_t Page::getFreeSpace() const {
    return header_.freeSpaceSize;
}

uint16_t Page::getSlotCount() const {
    return header_.slotCount;
}

bool Page::hasSpace(size_t recordSize) const {
    return header_.freeSpaceSize >= recordSize + sizeof(uint16_t);
}

std::vector<uint8_t> Page::serialize() const {
    std::vector<uint8_t> result(PAGE_SIZE);
    
    // 序列化页头
    std::memcpy(result.data(), &header_, sizeof(PageHeader));
    
    // 序列化数据
    std::memcpy(result.data() + PAGE_HEADER_SIZE, 
                data_.data() + PAGE_HEADER_SIZE, 
                PAGE_DATA_SIZE);
    
    return result;
}

std::unique_ptr<Page> Page::deserialize(const std::vector<uint8_t>& data) {
    if (data.size() != PAGE_SIZE) {
        return nullptr;
    }
    
    // 读取页头
    PageHeader header;
    std::memcpy(&header, data.data(), sizeof(PageHeader));
    
    auto page = std::make_unique<Page>(header.pageId, header.pageType);
    page->header_ = header;
    
    // 读取数据
    std::memcpy(page->data_.data(), data.data(), PAGE_SIZE);
    
    // 重建槽位目录
    page->slots_.resize(header.slotCount);
    for (uint16_t i = 0; i < header.slotCount; ++i) {
        // 这里简化处理，实际应该从页面数据中重建槽位目录
        page->slots_[i] = 0;
    }
    
    return page;
}

bool Page::isValid() const {
    return calculateChecksum() == header_.checksum;
}

void Page::updateChecksum() {
    header_.checksum = calculateChecksum();
}

void Page::printPageInfo() const {
    std::cout << "Page Info:" << std::endl;
    std::cout << "  Page ID: " << header_.pageId << std::endl;
    std::cout << "  Page Type: " << static_cast<int>(header_.pageType) << std::endl;
    std::cout << "  Slot Count: " << header_.slotCount << std::endl;
    std::cout << "  Free Space: " << header_.freeSpaceSize << " bytes" << std::endl;
    std::cout << "  Free Space Offset: " << header_.freeSpaceOffset << std::endl;
}

void Page::initializePage() {
    // 清零整个页面
    std::fill(data_.begin(), data_.end(), 0);
    
    // 写入页头
    std::memcpy(data_.data(), &header_, sizeof(PageHeader));
    
    updateChecksum();
}

uint32_t Page::calculateChecksum() const {
    // 简单的校验和计算（排除校验和字段本身）
    uint32_t sum = 0;
    const uint8_t* ptr = data_.data();
    
    // 校验和字段前的数据
    size_t checksumOffset = offsetof(PageHeader, checksum);
    for (size_t i = 0; i < checksumOffset; ++i) {
        sum += ptr[i];
    }
    
    // 校验和字段后的数据
    for (size_t i = checksumOffset + sizeof(uint32_t); i < PAGE_SIZE; ++i) {
        sum += ptr[i];
    }
    
    return sum;
}

uint16_t Page::findFreeSlot() const {
    for (uint16_t i = 0; i < slots_.size(); ++i) {
        if (slots_[i] == 0) {
            return i;
        }
    }
<<<<<<< HEAD
    return slots_.size();
=======
    return static_cast<uint16_t>(slots_.size());
>>>>>>> origin/storage
}

void Page::compactPage() {
    // 简单的页面压缩：重新组织所有有效记录
    std::vector<std::string> validRecords;
    
    // 收集所有有效记录
    for (uint16_t i = 0; i < slots_.size(); ++i) {
        if (slots_[i] != 0) {
            validRecords.push_back(getRecord(i));
        }
    }
    
    // 重新初始化页面
    header_.slotCount = 0;
    header_.freeSpaceOffset = PAGE_HEADER_SIZE;
    header_.freeSpaceSize = PAGE_DATA_SIZE;
    slots_.clear();
    
    // 重新插入所有记录
    for (const auto& record : validRecords) {
        insertRecord(record);
    }
}
