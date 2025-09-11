#pragma once
#include "Row.h"
#include <vector>
#include <memory>
#include <functional>

// B+树节点类型
enum class NodeType {
    LEAF,       // 叶子节点
    INTERNAL    // 内部节点
};

// B+树节点基类
class BPlusTreeNode {
public:
    NodeType nodeType;
    bool isRoot;
    int keyCount;
    std::vector<Value> keys;
    BPlusTreeNode* parent;
    
    BPlusTreeNode(NodeType type, int maxKeys);
    virtual ~BPlusTreeNode() = default;
    
    bool isFull() const;
    bool isUnderflow() const;
    virtual void insertKey(const Value& key, int index) = 0;
    virtual void removeKey(int index) = 0;
    
protected:
    int maxKeys_;
    int minKeys_;
};

// B+树叶子节点
class BPlusTreeLeafNode : public BPlusTreeNode {
public:
    std::vector<uint32_t> recordIds;  // 指向记录的ID
    BPlusTreeLeafNode* next;          // 指向下一个叶子节点
    BPlusTreeLeafNode* prev;          // 指向前一个叶子节点
    
    explicit BPlusTreeLeafNode(int maxKeys);
    ~BPlusTreeLeafNode() override = default;
    
    void insertKey(const Value& key, int index) override;
    void removeKey(int index) override;
    void insertRecord(const Value& key, uint32_t recordId);
    bool removeRecord(const Value& key, uint32_t recordId);
    std::vector<uint32_t> findRecords(const Value& key) const;
    
    // 范围查询
    std::vector<uint32_t> findRecordsInRange(const Value& startKey, const Value& endKey) const;
};

// B+树内部节点
class BPlusTreeInternalNode : public BPlusTreeNode {
public:
    std::vector<BPlusTreeNode*> children;
    
    explicit BPlusTreeInternalNode(int maxKeys);
    ~BPlusTreeInternalNode() override;
    
    void insertKey(const Value& key, int index) override;
    void removeKey(int index) override;
    void insertChild(BPlusTreeNode* child, int index);
    void removeChild(int index);
    BPlusTreeNode* findChild(const Value& key) const;
};

// B+树索引类
class BPlusTree {
public:
    explicit BPlusTree(int order = 128); // 默认阶数128
    ~BPlusTree();
    
    // 基本操作
    bool insert(const Value& key, uint32_t recordId);
    bool remove(const Value& key, uint32_t recordId);
    std::vector<uint32_t> search(const Value& key) const;
    
    // 范围查询
    std::vector<uint32_t> rangeSearch(const Value& startKey, const Value& endKey) const;
    
    // 树结构操作
    void clear();
    bool isEmpty() const;
    int getHeight() const;
    size_t getNodeCount() const;
    
    // 调试和统计
    void printTree() const;
    void validateTree() const;
    
private:
    BPlusTreeNode* root_;
    int order_;           // B+树的阶数
    int maxKeys_;         // 每个节点最大键数
    int minKeys_;         // 每个节点最小键数
    size_t nodeCount_;    // 节点总数
    
    // 内部操作
    BPlusTreeLeafNode* findLeafNode(const Value& key) const;
    void insertIntoParent(BPlusTreeNode* left, const Value& key, BPlusTreeNode* right);
    void deleteEntry(BPlusTreeNode* node, const Value& key, BPlusTreeNode* pointer = nullptr);
    
    // 节点分裂和合并
    void splitLeafNode(BPlusTreeLeafNode* leaf, const Value& key, uint32_t recordId);
    void splitInternalNode(BPlusTreeInternalNode* internal, const Value& key, BPlusTreeNode* child);
    void splitInternalNodeWithData(BPlusTreeInternalNode* internal, 
                                  const std::vector<Value>& keys, 
                                  const std::vector<BPlusTreeNode*>& children);
    void mergeLeafNodes(BPlusTreeLeafNode* left, BPlusTreeLeafNode* right);
    void mergeInternalNodes(BPlusTreeInternalNode* left, BPlusTreeInternalNode* right);
    
    // 重新分布
    bool redistributeLeafNodes(BPlusTreeLeafNode* left, BPlusTreeLeafNode* right);
    bool redistributeInternalNodes(BPlusTreeInternalNode* left, BPlusTreeInternalNode* right);
    
    // 辅助函数
    int compareValues(const Value& a, const Value& b) const;
    void printNode(BPlusTreeNode* node, int level) const;
    void validateNode(BPlusTreeNode* node) const;
    void deleteSubtree(BPlusTreeNode* node);
};
