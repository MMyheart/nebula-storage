/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_TOPKNODE_H_
#define STORAGE_EXEC_TOPKNODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"

namespace nebula {
namespace storage {


template<class T>
class TopKHeap final {
public:
    TopKHeap(int heapSize, std::function<bool(T, T)> comparator)
        : heapSize_(heapSize)
        , comparator_(std::move(comparator)) {
        v_.reserve(heapSize);
    }
    ~TopKHeap() = default;

    void push(T data) {
        if (v_.size() < static_cast<size_t>(heapSize_)) {
            v_.push_back(data);
            adjustUp(v_.size() - 1);
            return;
        }
        if (comparator_(data, v_[0])) {
            v_[0] = data;
            adjustDown(0);
        }
    }

    std::vector<T> moveTopK() {
        return std::move(v_);
    }

private:
    void adjustDown(size_t parent) {
        size_t child = parent * 2 + 1;
        size_t size = v_.size();
        while (child < size) {
            if (child + 1 < size && comparator_(v_[child], v_[child + 1])) {
                child += 1;
            }
            if (!comparator_(v_[parent], v_[child])) {
                return;
            }
            std::swap(v_[parent], v_[child]);
            parent = child;
            child = parent * 2 + 1;
        }
    }
    void adjustUp(size_t child) {
        size_t parent = (child - 1) >> 1;
        while (0 != child) {
            if (!comparator_(v_[parent], v_[child])) {
                return;
            }
            std::swap(v_[parent], v_[child]);
            child = parent;
            parent = (child - 1) >> 1;
        }
    }

private:
    int heapSize_;
    std::vector<T> v_;
    std::function<bool(T, T)> comparator_;
};

// TopKNode will return a DataSet with fixed size
template<class T>
class TopKNode final : public RelNode<T> {
public:
    using RelNode<T>::execute;

    TopKNode(nebula::DataSet* resultSet,
             std::vector<cpp2::OrderBy> orderBy,
             int limit)
        : resultSet_(resultSet)
        , orderBy_(orderBy)
        , limit_(limit) {}

    nebula::cpp2::ErrorCode execute(PartitionID partId) override {
        auto ret = RelNode<T>::execute(partId);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }
        resultSet_->rows = topK(resultSet_->rows, orderBy_, limit_);
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    static std::vector<Row> topK(std::vector<Row>& rows,
                                 std::vector<cpp2::OrderBy> orderBy, int k) {
        auto comparator = [orderBy, k] (const Row &lhs, const Row &rhs) {
            for (auto &item : orderBy) {
                auto index = item.get_pos();
                auto orderType = item.get_direction();
                if (lhs[index] == rhs[index]) {
                    continue;
                }
                if (orderType == cpp2::OrderDirection::ASCENDING) {
                    return lhs[index] < rhs[index];
                } else if (orderType == cpp2::OrderDirection::DESCENDING) {
                    return lhs[index] > rhs[index];
                }
            }
            return false;
        };
        TopKHeap<Row> topKHeap(k, std::move(comparator));
        for (auto row : rows) {
            topKHeap.push(row);
        }
        return topKHeap.moveTopK();
    }

private:
    nebula::DataSet*            resultSet_;
    std::vector<cpp2::OrderBy>  orderBy_{};
    int                         limit_{-1};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_TOPKNODE_H_
