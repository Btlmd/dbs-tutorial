//
// Created by lambda on 22-12-1.
//

#ifndef DBS_TUTORIAL_QUERYCOLUMN_H
#define DBS_TUTORIAL_QUERYCOLUMN_H

#include <defines.h>
#include <record/Field.h>

enum class ColumnType {
    BASIC = 0,
    COUNT = 1,
    MAX = 2,
    MIN = 3,
    AVG = 4,
    SUM = 5,
    COUNT_REC = 6
};

class Column {
public:
    ColumnType type;
    TableID table_id;
    std::shared_ptr<FieldMeta> field_meta;

    Column(TableID table_id, std::shared_ptr<FieldMeta> field_meta, ColumnType type = ColumnType::BASIC) :
            type{type}, table_id{table_id}, field_meta{std::move(field_meta)} {}

    bool operator==(const Column &rhs) const {
        return rhs.table_id == table_id && rhs.field_meta->field_id == field_meta->field_id;
    }
};


#endif //DBS_TUTORIAL_QUERYCOLUMN_H
