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

TableResult::TableResult(const std::vector<std::string> &headers, const RecordList &record_list) :
        headers{headers}, table{nullptr} {
    std::vector<std::string> buffer;
    for (const auto &record: record_list) {
        buffer.clear();
        std::transform(record.fields.cbegin(), record.fields.cend(), buffer.end(), [](Field *const &f) {
            return f->ToString();
        });
        records.emplace_back(std::move(buffer));
    }
}

TableResult::Row TableResult::MoveToRow(std::vector<std::string> &&los) {
    Row buffer;
    buffer.insert(buffer.end(), std::make_move_iterator(los.begin()), std::make_move_iterator(los.end()));
    return std::move(buffer);
}

std::string TableResult::ToString() {
    std::string output;
    if (records.empty()) {
        output = "Empty Set";
    } else {
        if (table) {
            return table->str();
        }

        // create table
        table = new tabulate::Table;
        table->add_row(MoveToRow(std::move(headers)));
        for (auto &record: records) {
            table->add_row(MoveToRow(std::move(record)));
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

        output = table->str() + "\n" + fmt::format("{} rows in set", records.size());
    }
    return output + fmt::format(" ({:.2f} sec)", runtime);
}

void Result::SetRuntime(double _runtime) {
    runtime = _runtime;
}
