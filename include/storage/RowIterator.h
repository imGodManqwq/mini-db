#pragma once
#include "Row.h"
#include <vector>
#include <memory>

class RowIterator {
public:
    RowIterator(const std::vector<Row>* rows, size_t position = 0);
    
    // 迭代器操作
    Row& operator*();
    const Row& operator*() const;
    Row* operator->();
    const Row* operator->() const;
    RowIterator& operator++();
    RowIterator operator++(int);
    
    // 比较操作
    bool operator==(const RowIterator& other) const;
    bool operator!=(const RowIterator& other) const;
    
    // 检查是否到达末尾
    bool hasNext() const;
    
    // 获取当前位置
    size_t getPosition() const;
    
private:
    const std::vector<Row>* rows_;
    size_t position_;
};
