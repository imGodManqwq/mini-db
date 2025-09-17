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
        // 比较当前键和要插入的键，找到正确的插入位置
        bool keyIsSmaller = std::visit([&key](const auto& currentKey) -> bool {
            return std::visit([&currentKey](const auto& newKey) -> bool {
                using T1 = std::decay_t<decltype(currentKey)>;
                using T2 = std::decay_t<decltype(newKey)>;
                if constexpr (std::is_same_v<T1, T2>) {
                    return newKey < currentKey; // 新键小于当前键，继续查找
                } else {
                    return false; // 不同类型不比较
                }
            }, key);
        }, keys[pos]);
        
        if (!keyIsSmaller) break; // 找到插入位置
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
        // 改进的范围比较，支持数值类型之间的转换
        bool inRange = std::visit([&startKey, &endKey](const auto& keyVal) -> bool {
            return std::visit([&keyVal, &endKey](const auto& start) -> bool {
                return std::visit([&keyVal, &start](const auto& end) -> bool {
                    using T = std::decay_t<decltype(keyVal)>;
                    using T1 = std::decay_t<decltype(start)>;
                    using T2 = std::decay_t<decltype(end)>;
                    
                    // 支持数值类型之间的比较
                    if constexpr (std::is_arithmetic_v<T> && std::is_arithmetic_v<T1> && std::is_arithmetic_v<T2>) {
                        double key = static_cast<double>(keyVal);
                        double startVal = static_cast<double>(start);
                        double endVal = static_cast<double>(end);
                        return key >= startVal && key <= endVal;
                    }
                    // 字符串类型的比较
                    else if constexpr (std::is_same_v<T, std::string> && std::is_same_v<T1, std::string> && std::is_same_v<T2, std::string>) {
                        return keyVal >= start && keyVal <= end;
                    }
                    // 相同类型的比较
                    else if constexpr (std::is_same_v<T, T1> && std::is_same_v<T, T2>) {
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
    // 在B+树内部节点中，keys[i]是子节点children[i+1]的最小值
    // 如果key < keys[pos]，则应该去左边的子节点children[pos]
    while (pos < keyCount) {
        bool keyLessThanCurrent = std::visit([&key](const auto& currentKey) -> bool {
            return std::visit([&currentKey](const auto& searchKey) -> bool {
                using T1 = std::decay_t<decltype(currentKey)>;
                using T2 = std::decay_t<decltype(searchKey)>;
                if constexpr (std::is_same_v<T1, T2>) {
                    return searchKey < currentKey;
                } else {
                    return false;
                }
            }, key);
        }, keys[pos]);
        
        if (keyLessThanCurrent) break;
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
    
    // 收集所有键值对（包括新的键值对）
    std::vector<std::pair<Value, uint32_t>> allEntries;
    
    // 添加现有的键值对
    for (int i = 0; i < leaf->keyCount; ++i) {
        allEntries.emplace_back(leaf->keys[i], leaf->recordIds[i]);
    }
    
    // 添加新的键值对
    allEntries.emplace_back(key, recordId);
    
    // 按键排序所有条目
    std::sort(allEntries.begin(), allEntries.end(), 
        [this](const std::pair<Value, uint32_t>& a, const std::pair<Value, uint32_t>& b) {
            return compareValues(a.first, b.first) < 0;
        });
    
    // 计算分割点
    int splitPoint = (allEntries.size() + 1) / 2;
    
    // 清空原叶子节点并重新分配
    leaf->keys.clear();
    leaf->recordIds.clear();
    leaf->keyCount = 0;
    
    // 前半部分留在原节点
    for (int i = 0; i < splitPoint; ++i) {
        leaf->keys.push_back(allEntries[i].first);
        leaf->recordIds.push_back(allEntries[i].second);
        leaf->keyCount++;
    }
    
    // 后半部分放入新节点
    for (int i = splitPoint; i < static_cast<int>(allEntries.size()); ++i) {
        newLeaf->keys.push_back(allEntries[i].first);
        newLeaf->recordIds.push_back(allEntries[i].second);
        newLeaf->keyCount++;
    }
    
    // 更新链接
    newLeaf->next = leaf->next;
    if (leaf->next) {
        leaf->next->prev = newLeaf;
    }
    leaf->next = newLeaf;
    newLeaf->prev = leaf;
    
    // 向父节点插入新的键（新叶子节点的第一个键）
    Value splitKey = newLeaf->keys[0];
    insertIntoParent(leaf, splitKey, newLeaf);
}

void BPlusTree::insertIntoParent(BPlusTreeNode* left, const Value& key, BPlusTreeNode* right) {
    if (left->isRoot) {
        // 创建新的根节点
        auto newRoot = new BPlusTreeInternalNode(maxKeys_);
        newRoot->isRoot = true;
        
        // 设置键和子节点
        newRoot->keys.push_back(key);
        newRoot->keyCount = 1;
        newRoot->children.push_back(left);
        newRoot->children.push_back(right);
        
        left->isRoot = false;
        left->parent = newRoot;
        right->parent = newRoot;
        
        root_ = newRoot;
        nodeCount_++;
        return;
    }
    
    auto parent = static_cast<BPlusTreeInternalNode*>(left->parent);
    
    // 收集所有现有的键和子节点，加上新的键和子节点
    std::vector<Value> allKeys = parent->keys;
    std::vector<BPlusTreeNode*> allChildren = parent->children;
    
    // 找到正确的插入位置
    int insertPos = 0;
    for (int i = 0; i < parent->keyCount; ++i) {
        if (compareValues(key, parent->keys[i]) < 0) {
            break;
        }
        insertPos++;
    }
    
    // 插入新键和右子节点
    allKeys.insert(allKeys.begin() + insertPos, key);
    allChildren.insert(allChildren.begin() + insertPos + 1, right);
    
    if (allKeys.size() <= static_cast<size_t>(maxKeys_)) {
        // 父节点未满，直接更新
        parent->keys = allKeys;
        parent->children = allChildren;
        parent->keyCount = allKeys.size();
        right->parent = parent;
    } else {
        // 父节点已满，需要分裂
        splitInternalNodeWithData(parent, allKeys, allChildren);
    }
}

void BPlusTree::splitInternalNode(BPlusTreeInternalNode* internal, const Value& key, BPlusTreeNode* child) {
    // 这个方法现在仅作为向后兼容
    // 实际逻辑在insertIntoParent中处理
}

void BPlusTree::splitInternalNodeWithData(BPlusTreeInternalNode* internal, 
                                         const std::vector<Value>& keys, 
                                         const std::vector<BPlusTreeNode*>& children) {
    // 创建新的内部节点
    auto newInternal = new BPlusTreeInternalNode(maxKeys_);
    nodeCount_++;
    
    // 计算分割点
    int splitPoint = keys.size() / 2;
    Value splitKey = keys[splitPoint];
    
    // 清空原节点
    internal->keys.clear();
    internal->children.clear();
    internal->keyCount = 0;
    
    // 前半部分留在原节点
    for (int i = 0; i < splitPoint; ++i) {
        internal->keys.push_back(keys[i]);
        internal->keyCount++;
    }
    for (int i = 0; i <= splitPoint; ++i) {
        internal->children.push_back(children[i]);
        children[i]->parent = internal;
    }
    
    // 后半部分放入新节点
    for (int i = splitPoint + 1; i < static_cast<int>(keys.size()); ++i) {
        newInternal->keys.push_back(keys[i]);
        newInternal->keyCount++;
    }
    for (int i = splitPoint + 1; i < static_cast<int>(children.size()); ++i) {
        newInternal->children.push_back(children[i]);
        children[i]->parent = newInternal;
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
