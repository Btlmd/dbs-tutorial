//
// Created by lambda on 22-11-18.
//

#include "Result.h"

#include <fmt/core.h>
#include <fmt/color.h>

#include <tabulate/tabulate.hpp>

#include <algorithm>

#include <record/Field.h>

std::string TextResult::ToString() {
    return text;
}

TableResult::TableResult(std::vector<std::string> headers, const RecordList &record_list) :
        headers{std::move(headers)}, table{nullptr}, record_list{std::make_shared<RecordList>(record_list)} {
    for (const auto &record: record_list) {
        records.emplace_back(record->ToString());
    }
}

TableResult::Row TableResult::ToRow(std::vector<std::string> los) {
    Row buffer;
    buffer.insert(buffer.end(), std::make_move_iterator(los.begin()), std::make_move_iterator(los.end()));
    return std::move(buffer);
}

std::string TableResult::ToString() {
    std::string output;
    if (records.empty()) {
        output = "Empty Set";
    } else {
        // create table
        table = std::make_shared<tabulate::Table>();

        if (headers.empty()) {
            for (auto &record: records) {
                table->add_row(ToRow(record));
            }

            // table format
            auto record_len{records.size()};
            if (record_len > 1) {  // range [0, record_len - 1]
                table->row(0).format().border_bottom("").corner_bottom_left("").corner_bottom_right("");
                for (int i{1}; i < record_len - 1; ++i) {
                    table->row(i).format().border_top("").border_bottom("").corner("");
                }
                table->row(record_len - 1).format().border_top("").corner_top_left("").corner_top_right("");
            }
        } else {
            table->add_row(ToRow(headers));
            for (auto &record: records) {
                table->add_row(ToRow(record));
            }

            // table format
            auto record_len{records.size()};
            if (record_len > 1) {  // range [1, record_len]
                table->row(1).format().border_bottom("").corner_bottom_left("").corner_bottom_right("");
                for (int i{2}; i < record_len; ++i) {
                    table->row(i).format().border_top("").border_bottom("").corner("");
                }
                table->row(record_len).format().border_top("").corner_top_left("").corner_top_right("");
            }
        }

        output = table->str();
        AddInfo(fmt::format("{} row(s) in set", records.size()), true);
    }
    return output;
}

void Result::SetRuntime(double _runtime) {
    runtime = _runtime;
}

void Result::AddInfo(const std::string &info, bool head) {
    if (head) {
        infos.insert(infos.begin(), info);
    } else {
        infos.push_back(info);
    }
}

std::string Result::Display() {
    std::string ret;
    ret += ToString() + '\n';
    for (const auto &info: infos) {
        ret += info + '\n';
    }
    ret += fmt::format("({:.2f} sec)", runtime) + '\n';
    return ret;
}
