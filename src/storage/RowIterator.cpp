#include "../../include/storage/RowIterator.h"
#include <stdexcept>

RowIterator::RowIterator(const std::vector<Row>* rows, size_t position) 
    : rows_(rows), position_(position) {}

Row& RowIterator::operator*() {
    if (!rows_ || position_ >= rows_->size()) {
        throw std::out_of_range("Iterator out of range");
    }
    return const_cast<Row&>((*rows_)[position_]);
}

const Row& RowIterator::operator*() const {
    if (!rows_ || position_ >= rows_->size()) {
        throw std::out_of_range("Iterator out of range");
    }
    return (*rows_)[position_];
}

Row* RowIterator::operator->() {
    if (!rows_ || position_ >= rows_->size()) {
        throw std::out_of_range("Iterator out of range");
    }
    return const_cast<Row*>(&(*rows_)[position_]);
}

const Row* RowIterator::operator->() const {
    if (!rows_ || position_ >= rows_->size()) {
        throw std::out_of_range("Iterator out of range");
    }
    return &(*rows_)[position_];
}

RowIterator& RowIterator::operator++() {
    ++position_;
    return *this;
}

RowIterator RowIterator::operator++(int) {
    RowIterator temp = *this;
    ++position_;
    return temp;
}

bool RowIterator::operator==(const RowIterator& other) const {
    return rows_ == other.rows_ && position_ == other.position_;
}

bool RowIterator::operator!=(const RowIterator& other) const {
    return !(*this == other);
}

bool RowIterator::hasNext() const {
    return rows_ && position_ < rows_->size();
}

size_t RowIterator::getPosition() const {
    return position_;
}
