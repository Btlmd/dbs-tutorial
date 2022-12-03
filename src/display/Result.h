//
// Created by lambda on 22-11-18.
//

#ifndef DBS_TUTORIAL_RESULT_H
#define DBS_TUTORIAL_RESULT_H

#include <string>
#include <fmt/core.h>
#include <vector>
#include <memory>

#include <defines.h>

#include <record/Record.h>
#include <tabulate/table.hpp>

class Result {
public:
    virtual std::string ToString() = 0;
    virtual ~Result() = default;
    virtual void SetRuntime(double runtime);
    virtual void AddInfo(const std::string &info);
protected:
    std::vector<std::string> infos;
    double runtime{-1};
};

class TableResult : public Result {
public:
    TableResult(std::vector<std::string> headers, std::vector<std::vector<std::string>> rows) :
            headers{std::move(headers)}, records{std::move(rows)}, table{nullptr}, record_list{nullptr} {}

    TableResult(std::vector<std::string> headers, const RecordList &record_list);

    std::string ToString() override;

    typedef std::vector<variant<std::string, const char *, tabulate::Table>> Row;

    static Row ToRow(std::vector<std::string> los);

    std::vector<std::string> headers;
    std::vector<std::vector<std::string>> records;
    std::shared_ptr<RecordList> record_list;
    std::shared_ptr<tabulate::Table> table;
};

class TextResult : public Result {
public:
    explicit TextResult(std::string message) : Result{}, text{std::move(message)} {}

    std::string ToString() override;

    std::string text;
};

// The results of a `program`
typedef std::vector<std::shared_ptr<Result>> ResultList;

#endif //DBS_TUTORIAL_RESULT_H
