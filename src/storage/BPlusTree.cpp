#include "../../include/storage/BPlusTree.h"
#include <iostream>
#include <algorithm>
#include <cassert>

// BPlusTreeNode 实现
BPlusTreeNode::BPlusTreeNode(NodeType type, int maxKeys) 
    : nodeType(type), isRoot(false), keyCount(0), parent(nullptr), maxKeys_(maxKeys) {
    minKeys_ = (maxKeys + 1) / 2 - 1; // 向下取整
    keys.reserve(maxKeys);
}

bool BPlusTreeNode::isFull() const {
    return keyCount >= maxKeys_;
}

bool BPlusTreeNode::isUnderflow() const {
    return keyCount < minKeys_;
}

// BPlusTreeLeafNode 实现
BPlusTreeLeafNode::BPlusTreeLeafNode(int maxKeys) 
    : BPlusTreeNode(NodeType::LEAF, maxKeys), next(nullptr), prev(nullptr) {
    recordIds.reserve(maxKeys);
}

void BPlusTreeLeafNode::insertKey(const Value& key, int index) {
    keys.insert(keys.begin() + index, key);
    keyCount++;
}

void BPlusTreeLeafNode::removeKey(int index) {
    if (index >= 0 && index < keyCount) {
        keys.erase(keys.begin() + index);
        recordIds.erase(recordIds.begin() + index);
        keyCount--;
    }
}

void BPlusTreeLeafNode::insertRecord(const Value& key, uint32_t recordId) {
    // 找到插入位置
    int pos = 0;
    while (pos < keyCount) {
        // 这里简化比较，实际需要根据Value类型进行比较
        bool shouldInsert = std::visit([&key](const auto& a) -> bool {
            return std::visit([&a](const auto& b) -> bool {
                using T1 = std::decay_t<decltype(a)>;
                using T2 = std::decay_t<decltype(b)>;
                if constexpr (std::is_same_v<T1, T2>) {
                    return b >= a;
                } else {
                    return false; // 不同类型不比较
                }
            }, key);
        }, keys[pos]);
        
        if (shouldInsert) break;
        pos++;
    }
    
    keys.insert(keys.begin() + pos, key);
    recordIds.insert(recordIds.begin() + pos, recordId);
    keyCount++;
}

bool BPlusTreeLeafNode::removeRecord(const Value& key, uint32_t recordId) {
    for (int i = 0; i < keyCount; ++i) {
        if (keys[i] == key && recordIds[i] == recordId) {
            removeKey(i);
            return true;
        }
    }
    return false;
}

std::vector<uint32_t> BPlusTreeLeafNode::findRecords(const Value& key) const {
    std::vector<uint32_t> result;
    for (int i = 0; i < keyCount; ++i) {
        if (keys[i] == key) {
            result.push_back(recordIds[i]);
        }
    }
    return result;
}

std::vector<uint32_t> BPlusTreeLeafNode::findRecordsInRange(const Value& startKey, const Value& endKey) const {
    std::vector<uint32_t> result;
    for (int i = 0; i < keyCount; ++i) {
        // 简化范围比较
        bool inRange = std::visit([&startKey, &endKey](const auto& keyVal) -> bool {
            return std::visit([&keyVal, &endKey](const auto& start) -> bool {
                return std::visit([&keyVal, &start](const auto& end) -> bool {
                    using T = std::decay_t<decltype(keyVal)>;
                    using T1 = std::decay_t<decltype(start)>;
                    using T2 = std::decay_t<decltype(end)>;
                    if constexpr (std::is_same_v<T, T1> && std::is_same_v<T, T2>) {
                        return keyVal >= start && keyVal <= end;
                    }
                    return false;
                }, endKey);
            }, startKey);
        }, keys[i]);
        
        if (inRange) {
            result.push_back(recordIds[i]);
        }
    }
    return result;
}

// BPlusTreeInternalNode 实现
BPlusTreeInternalNode::BPlusTreeInternalNode(int maxKeys) 
    : BPlusTreeNode(NodeType::INTERNAL, maxKeys) {
    children.reserve(maxKeys + 1);
}

BPlusTreeInternalNode::~BPlusTreeInternalNode() {
    // 不在这里删除子节点，由BPlusTree统一管理
}

void BPlusTreeInternalNode::insertKey(const Value& key, int index) {
    keys.insert(keys.begin() + index, key);
    keyCount++;
}

void BPlusTreeInternalNode::removeKey(int index) {
    if (index >= 0 && index < keyCount) {
        keys.erase(keys.begin() + index);
        keyCount--;
    }
}

void BPlusTreeInternalNode::insertChild(BPlusTreeNode* child, int index) {
    children.insert(children.begin() + index, child);
    if (child) {
        child->parent = this;
    }
}

void BPlusTreeInternalNode::removeChild(int index) {
    if (index >= 0 && index < static_cast<int>(children.size())) {
        if (children[index]) {
            children[index]->parent = nullptr;
        }
        children.erase(children.begin() + index);
    }
}

BPlusTreeNode* BPlusTreeInternalNode::findChild(const Value& key) const {
    int pos = 0;
    while (pos < keyCount) {
        bool keyLessEqual = std::visit([&key](const auto& a) -> bool {
            return std::visit([&a](const auto& b) -> bool {
                using T1 = std::decay_t<decltype(a)>;
                using T2 = std::decay_t<decltype(b)>;
                if constexpr (std::is_same_v<T1, T2>) {
                    return b <= a;
                } else {
                    return false;
                }
            }, key);
        }, keys[pos]);
        
        if (keyLessEqual) break;
        pos++;
    }
    
    if (pos < static_cast<int>(children.size())) {
        return children[pos];
    }
    return nullptr;
}

// BPlusTree 实现
BPlusTree::BPlusTree(int order) : root_(nullptr), order_(order), nodeCount_(0) {
    maxKeys_ = order - 1;
    minKeys_ = (order + 1) / 2 - 1;
}

BPlusTree::~BPlusTree() {
    clear();
}

bool BPlusTree::insert(const Value& key, uint32_t recordId) {
    if (!root_) {
        // 创建根节点（叶子节点）
        root_ = new BPlusTreeLeafNode(maxKeys_);
        root_->isRoot = true;
        nodeCount_++;
    }
    
    BPlusTreeLeafNode* leaf = findLeafNode(key);
    if (!leaf) return false;
    
    // 如果叶子节点未满，直接插入
    if (!leaf->isFull()) {
        leaf->insertRecord(key, recordId);
        return true;
    }
    
    // 叶子节点已满，需要分裂
    splitLeafNode(leaf, key, recordId);
    return true;
}

bool BPlusTree::remove(const Value& key, uint32_t recordId) {
    if (!root_) return false;
    
    BPlusTreeLeafNode* leaf = findLeafNode(key);
    if (!leaf) return false;
    
    if (!leaf->removeRecord(key, recordId)) {
        return false; // 记录不存在
    }
    
    // 检查是否需要重新平衡
    if (leaf->isUnderflow() && !leaf->isRoot) {
        deleteEntry(leaf, key);
    }
    
    return true;
}

std::vector<uint32_t> BPlusTree::search(const Value& key) const {
    if (!root_) return {};
    
    BPlusTreeLeafNode* leaf = findLeafNode(key);
    if (!leaf) return {};
    
    return leaf->findRecords(key);
}

std::vector<uint32_t> BPlusTree::rangeSearch(const Value& startKey, const Value& endKey) const {
    std::vector<uint32_t> result;
    if (!root_) return result;
    
    BPlusTreeLeafNode* leaf = findLeafNode(startKey);
    if (!leaf) return result;
    
    // 从找到的叶子节点开始，遍历所有相关的叶子节点
    while (leaf) {
        auto records = leaf->findRecordsInRange(startKey, endKey);
        result.insert(result.end(), records.begin(), records.end());
        
        // 检查是否需要继续到下一个叶子节点
        if (leaf->keyCount > 0) {
            bool shouldContinue = std::visit([&endKey](const auto& lastKey) -> bool {
                return std::visit([&lastKey](const auto& end) -> bool {
                    using T1 = std::decay_t<decltype(lastKey)>;
                    using T2 = std::decay_t<decltype(end)>;
                    if constexpr (std::is_same_v<T1, T2>) {
                        return lastKey < end;
                    }
                    return false;
                }, endKey);
            }, leaf->keys[leaf->keyCount - 1]);
            
            if (!shouldContinue) break;
        }
        
        leaf = leaf->next;
    }
    
    return result;
}

void BPlusTree::clear() {
    if (root_) {
        deleteSubtree(root_);
        root_ = nullptr;
        nodeCount_ = 0;
    }
}

bool BPlusTree::isEmpty() const {
    return root_ == nullptr;
}

int BPlusTree::getHeight() const {
    if (!root_) return 0;
    
    int height = 1;
    BPlusTreeNode* current = root_;
    while (current->nodeType == NodeType::INTERNAL) {
        auto internal = static_cast<BPlusTreeInternalNode*>(current);
        current = internal->children[0];
        height++;
    }
    
    return height;
}

size_t BPlusTree::getNodeCount() const {
    return nodeCount_;
}

void BPlusTree::printTree() const {
    if (!root_) {
        std::cout << "Empty tree" << std::endl;
        return;
    }
    
    std::cout << "B+ Tree Structure:" << std::endl;
    printNode(root_, 0);
}

BPlusTreeLeafNode* BPlusTree::findLeafNode(const Value& key) const {
    if (!root_) return nullptr;
    
    BPlusTreeNode* current = root_;
    while (current->nodeType == NodeType::INTERNAL) {
        auto internal = static_cast<BPlusTreeInternalNode*>(current);
        current = internal->findChild(key);
        if (!current) return nullptr;
    }
    
    return static_cast<BPlusTreeLeafNode*>(current);
}

void BPlusTree::splitLeafNode(BPlusTreeLeafNode* leaf, const Value& key, uint32_t recordId) {
    // 创建新的叶子节点
    auto newLeaf = new BPlusTreeLeafNode(maxKeys_);
    nodeCount_++;
    
    // 临时存储所有键值对
    std::vector<Value> tempKeys = leaf->keys;
    std::vector<uint32_t> tempRecordIds = leaf->recordIds;
    
    // 插入新记录到临时数组
    int pos = 0;
    while (pos < leaf->keyCount) {
        bool shouldInsert = std::visit([&key](const auto& a) -> bool {
            return std::visit([&a](const auto& b) -> bool {
                using T1 = std::decay_t<decltype(a)>;
                using T2 = std::decay_t<decltype(b)>;
                if constexpr (std::is_same_v<T1, T2>) {
                    return b >= a;
                } else {
                    return false;
                }
            }, key);
        }, tempKeys[pos]);
        
        if (shouldInsert) break;
        pos++;
    }
    
    tempKeys.insert(tempKeys.begin() + pos, key);
    tempRecordIds.insert(tempRecordIds.begin() + pos, recordId);
    
    // 计算分割点
    int splitPoint = (tempKeys.size() + 1) / 2;
    
    // 清空原叶子节点并重新分配
    leaf->keys.clear();
    leaf->recordIds.clear();
    leaf->keyCount = 0;
    
    // 前半部分留在原节点
    for (int i = 0; i < splitPoint; ++i) {
        leaf->keys.push_back(tempKeys[i]);
        leaf->recordIds.push_back(tempRecordIds[i]);
        leaf->keyCount++;
    }
    
    // 后半部分放入新节点
    for (int i = splitPoint; i < static_cast<int>(tempKeys.size()); ++i) {
        newLeaf->keys.push_back(tempKeys[i]);
        newLeaf->recordIds.push_back(tempRecordIds[i]);
        newLeaf->keyCount++;
    }
    
    // 更新链接
    newLeaf->next = leaf->next;
    if (leaf->next) {
        leaf->next->prev = newLeaf;
    }
    leaf->next = newLeaf;
    newLeaf->prev = leaf;
    
    // 向父节点插入新的键
    Value splitKey = newLeaf->keys[0];
    insertIntoParent(leaf, splitKey, newLeaf);
}

void BPlusTree::insertIntoParent(BPlusTreeNode* left, const Value& key, BPlusTreeNode* right) {
    if (left->isRoot) {
        // 创建新的根节点
        auto newRoot = new BPlusTreeInternalNode(maxKeys_);
        newRoot->isRoot = true;
        newRoot->insertKey(key, 0);
        newRoot->insertChild(left, 0);
        newRoot->insertChild(right, 1);
        
        left->isRoot = false;
        left->parent = newRoot;
        right->parent = newRoot;
        
        root_ = newRoot;
        nodeCount_++;
        return;
    }
    
    auto parent = static_cast<BPlusTreeInternalNode*>(left->parent);
    if (!parent->isFull()) {
        // 父节点未满，直接插入
        int pos = 0;
        while (pos < parent->keyCount) {
            bool shouldInsert = std::visit([&key](const auto& a) -> bool {
                return std::visit([&a](const auto& b) -> bool {
                    using T1 = std::decay_t<decltype(a)>;
                    using T2 = std::decay_t<decltype(b)>;
                    if constexpr (std::is_same_v<T1, T2>) {
                        return b >= a;
                    } else {
                        return false;
                    }
                }, key);
            }, parent->keys[pos]);
            
            if (shouldInsert) break;
            pos++;
        }
        
        parent->insertKey(key, pos);
        parent->insertChild(right, pos + 1);
        right->parent = parent;
    } else {
        // 父节点已满，需要分裂
        splitInternalNode(parent, key, right);
    }
}

void BPlusTree::splitInternalNode(BPlusTreeInternalNode* internal, const Value& key, BPlusTreeNode* child) {
    // 创建新的内部节点
    auto newInternal = new BPlusTreeInternalNode(maxKeys_);
    nodeCount_++;
    
    // 临时存储所有键和子节点
    std::vector<Value> tempKeys = internal->keys;
    std::vector<BPlusTreeNode*> tempChildren = internal->children;
    
    // 找到插入位置
    int pos = 0;
    while (pos < internal->keyCount) {
        bool shouldInsert = std::visit([&key](const auto& a) -> bool {
            return std::visit([&a](const auto& b) -> bool {
                using T1 = std::decay_t<decltype(a)>;
                using T2 = std::decay_t<decltype(b)>;
                if constexpr (std::is_same_v<T1, T2>) {
                    return b >= a;
                } else {
                    return false;
                }
            }, key);
        }, tempKeys[pos]);
        
        if (shouldInsert) break;
        pos++;
    }
    
    tempKeys.insert(tempKeys.begin() + pos, key);
    tempChildren.insert(tempChildren.begin() + pos + 1, child);
    
    // 计算分割点
    int splitPoint = tempKeys.size() / 2;
    Value splitKey = tempKeys[splitPoint];
    
    // 清空原节点
    internal->keys.clear();
    internal->children.clear();
    internal->keyCount = 0;
    
    // 前半部分留在原节点
    for (int i = 0; i < splitPoint; ++i) {
        internal->keys.push_back(tempKeys[i]);
        internal->keyCount++;
    }
    for (int i = 0; i <= splitPoint; ++i) {
        internal->children.push_back(tempChildren[i]);
        tempChildren[i]->parent = internal;
    }
    
    // 后半部分放入新节点
    for (int i = splitPoint + 1; i < static_cast<int>(tempKeys.size()); ++i) {
        newInternal->keys.push_back(tempKeys[i]);
        newInternal->keyCount++;
    }
    for (int i = splitPoint + 1; i < static_cast<int>(tempChildren.size()); ++i) {
        newInternal->children.push_back(tempChildren[i]);
        tempChildren[i]->parent = newInternal;
    }
    
    // 向父节点插入分割键
    insertIntoParent(internal, splitKey, newInternal);
}

void BPlusTree::deleteEntry(BPlusTreeNode* node, const Value& key, BPlusTreeNode* pointer) {
    // 简化实现：这里只处理基本的删除逻辑
    // 实际实现需要处理节点合并和重新分布
}

int BPlusTree::compareValues(const Value& a, const Value& b) const {
    return std::visit([](const auto& x, const auto& y) -> int {
        using T1 = std::decay_t<decltype(x)>;
        using T2 = std::decay_t<decltype(y)>;
        if constexpr (std::is_same_v<T1, T2>) {
            if (x < y) return -1;
            if (x > y) return 1;
            return 0;
        } else {
            return 0; // 不同类型认为相等
        }
    }, a, b);
}

void BPlusTree::printNode(BPlusTreeNode* node, int level) const {
    if (!node) return;
    
    std::string indent(level * 2, ' ');
    std::cout << indent << "Node (";
    std::cout << (node->nodeType == NodeType::LEAF ? "Leaf" : "Internal");
    std::cout << ", Keys: " << node->keyCount << "): ";
    
    for (int i = 0; i < node->keyCount; ++i) {
        std::visit([](const auto& val) {
            std::cout << val << " ";
        }, node->keys[i]);
    }
    std::cout << std::endl;
    
    if (node->nodeType == NodeType::INTERNAL) {
        auto internal = static_cast<BPlusTreeInternalNode*>(node);
        for (auto child : internal->children) {
            printNode(child, level + 1);
        }
    }
}

void BPlusTree::validateTree() const {
    if (root_) {
        validateNode(root_);
    }
}

void BPlusTree::validateNode(BPlusTreeNode* node) const {
    // 简化的验证逻辑
    assert(node->keyCount >= 0);
    assert(node->keyCount <= maxKeys_);
    
    if (!node->isRoot) {
        assert(node->keyCount >= minKeys_);
    }
}

void BPlusTree::deleteSubtree(BPlusTreeNode* node) {
    if (!node) return;
    
    if (node->nodeType == NodeType::INTERNAL) {
        auto internal = static_cast<BPlusTreeInternalNode*>(node);
        for (auto child : internal->children) {
            deleteSubtree(child);
        }
    }
    
    delete node;
}
