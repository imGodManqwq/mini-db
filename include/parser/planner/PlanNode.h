#pragma once
#include <string>
#include <vector>
#include <memory>
#include <optional>

// 执行计划节点基类
class PlanNode {
public:
    virtual ~PlanNode() = default;
    virtual std::string name() const = 0;
};

// CREATE TABLE
class CreateTablePlan : public PlanNode {
public:
    std::string tableName;
    std::vector<std::pair<std::string,std::string>> columns; // 列名, 类型
    CreateTablePlan(const std::string& name,
                    const std::vector<std::pair<std::string,std::string>>& cols)
        : tableName(name), columns(cols) {}
    std::string name() const override { return "CreateTable"; }
};

// INSERT
class InsertPlan : public PlanNode {
public:
    std::string tableName;
    std::vector<std::string> columns;
    std::vector<std::string> values;
    InsertPlan(const std::string& t,
               const std::vector<std::string>& c,
               const std::vector<std::string>& v)
        : tableName(t), columns(c), values(v) {}
    std::string name() const override { return "Insert"; }
};

// SEQ SCAN（全表扫描）
class SeqScanPlan : public PlanNode {
public:
    std::string tableName;
    SeqScanPlan(const std::string& t) : tableName(t) {}
    std::string name() const override { return "SeqScan"; }
};

// FILTER（WHERE 条件）
class FilterPlan : public PlanNode {
public:
    std::shared_ptr<PlanNode> child;
    std::string column;
    std::string op;     // >, <, =, >=, <=
    std::string value;  // 比较值
    FilterPlan(std::shared_ptr<PlanNode> c,
               const std::string& col,
               const std::string& o,
               const std::string& val)
        : child(c), column(col), op(o), value(val) {}
    std::string name() const override { return "Filter"; }
};

// PROJECT（列选择）
class ProjectPlan : public PlanNode {
public:
    std::shared_ptr<PlanNode> child;
    std::vector<std::string> columns;
    ProjectPlan(std::shared_ptr<PlanNode> c,
                const std::vector<std::string>& cols)
        : child(c), columns(cols) {}
    std::string name() const override { return "Project"; }
};

// DELETE
class DeletePlan : public PlanNode {
public:
    std::string tableName;
    std::optional<std::string> whereColumn;
    std::optional<std::string> whereOp;
    std::optional<std::string> whereValue;
    DeletePlan(const std::string& t,
               std::optional<std::string> c = std::nullopt,
               std::optional<std::string> o = std::nullopt,
               std::optional<std::string> v = std::nullopt)
        : tableName(t), whereColumn(c), whereOp(o), whereValue(v) {}
    std::string name() const override { return "Delete"; }
};