#pragma once
#include <cstdint>
#include <vector>
#include <string>
#include <memory>

// 页的大小定义（4KB）
constexpr size_t PAGE_SIZE = 4096;
constexpr size_t PAGE_HEADER_SIZE = 24; // 页头大小
constexpr size_t PAGE_DATA_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE;

// 页类型枚举
enum class PageType : uint8_t {
    DATA_PAGE = 0,      // 数据页
    INDEX_PAGE = 1,     // 索引页
    META_PAGE = 2       // 元数据页
};

// 页头结构
struct PageHeader {
    uint32_t pageId;           // 页ID
    PageType pageType;         // 页类型
    uint16_t slotCount;        // 槽位数量
    uint16_t freeSpaceOffset;  // 空闲空间偏移
    uint16_t freeSpaceSize;    // 空闲空间大小
    uint32_t checksum;         // 校验和
    uint64_t lsn;              // 日志序列号（用于恢复）
};

class Page {
public:
    explicit Page(uint32_t pageId, PageType type = PageType::DATA_PAGE);
    
    // 页基本信息
    uint32_t getPageId() const;
    PageType getPageType() const;
    
    // 数据操作
    bool insertRecord(const std::string& record);
    uint16_t insertRecordAndReturnSlot(const std::string& record);  // 返回分配的槽位ID
    std::string getRecord(uint16_t slotId) const;
    bool deleteRecord(uint16_t slotId);
    bool updateRecord(uint16_t slotId, const std::string& newRecord);
    
    // 空间管理
    size_t getFreeSpace() const;
    uint16_t getSlotCount() const;
    bool hasSpace(size_t recordSize) const;
    
    // 序列化和反序列化
    std::vector<uint8_t> serialize() const;
    static std::unique_ptr<Page> deserialize(const std::vector<uint8_t>& data);
    
    // 页验证
    bool isValid() const;
    void updateChecksum();
    
    // 调试功能
    void printPageInfo() const;
    
private:
    PageHeader header_;
    std::vector<uint8_t> data_;  // 页数据
    std::vector<uint16_t> slots_; // 槽位目录（记录偏移）
    
    void initializePage();
    uint32_t calculateChecksum() const;
    uint16_t findFreeSlot() const;
    void compactPage();
};
