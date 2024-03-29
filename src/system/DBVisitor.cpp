//
// Created by lambda on 22-11-17.
//

#include "DBVisitor.h"

#include <defines.h>
#include <system/DBSystem.h>
#include <exception/OperationException.h>
#include <system/WhereConditions.h>
#include <system/Column.h>
#include <node/OpNode.h>
#include <node/ScanNode.h>
#include <node/NestedJoinNode.h>
#include <node/HashJoinNode.h>
#include <node/ProjectNode.h>
#include <node/AggregateNode.h>
#include <node/OffsetLimitNode.h>

#include <magic_enum.hpp>

#include <boost/range/adaptor/transformed.hpp>
#include <boost/algorithm/string/join.hpp>

#include <chrono>
#include <memory>
#include <tuple>
#include <memory>

antlrcpp::Any DBVisitor::visitCreate_db(SQLParser::Create_dbContext *ctx) {
    visitChildren(ctx);
    std::string db_name{ctx->Identifier()->getSymbol()->getText()};
    return system.CreateDatabase(db_name);
}

antlrcpp::Any DBVisitor::visitProgram(SQLParser::ProgramContext *ctx) {
    auto results{std::make_shared<ResultList>()};
    for (auto &child: ctx->children) {
        auto stmt_ctx{dynamic_cast<SQLParser::StatementContext *>(child)};
        if (stmt_ctx && !stmt_ctx->Annotation() && !stmt_ctx->Null()) {
            auto begin{std::chrono::high_resolution_clock::now()};
            antlrcpp::Any result{child->accept(this)};
            std::chrono::duration<double> elapse{std::chrono::high_resolution_clock::now() - begin};
            std::shared_ptr<Result> result_ptr;
            try {
                result_ptr = result.as<std::shared_ptr<Result>>();
            } catch (std::bad_cast &) {
                throw OperationError{"Unsupported Operation"};
            }
            result_ptr->SetRuntime(elapse.count());
            results->emplace_back(result_ptr);
        }
    }
    return results;
}

antlrcpp::Any DBVisitor::visitStatement(SQLParser::StatementContext *ctx) {
    return ctx->children[0]->accept(this);
}

antlrcpp::Any DBVisitor::visitShow_dbs(SQLParser::Show_dbsContext *ctx) {
    return system.ShowDatabases();
}

antlrcpp::Any DBVisitor::visitDrop_db(SQLParser::Drop_dbContext *ctx) {
    visitChildren(ctx);
    std::string db_name{ctx->Identifier()->getSymbol()->getText()};
    return system.DropDatabase(db_name);
}

antlrcpp::Any DBVisitor::visitCreate_table(SQLParser::Create_tableContext *ctx) {
    visitChildren(ctx);
    std::string table_name{ctx->Identifier()->getText()};
    auto field_ctx_list{ctx->field_list()->field()};

    std::optional<RawPrimaryKey> raw_pk;
    std::vector<RawForeignKey> raw_fks;
    std::vector<std::shared_ptr<FieldMeta>> field_meta;

    FieldID field_id_counter{0};

    for (const auto &field_ctx: field_ctx_list) {
        auto nf{dynamic_cast<SQLParser::Normal_fieldContext *>(field_ctx)};
        if (nf) {
            auto field_type_caster{magic_enum::enum_cast<FieldType>(nf->type_()->children[0]->getText())};
            assert(field_type_caster.has_value());
            auto field_type{field_type_caster.value()};
            auto not_null{nf->Null()};
            RecordSize max_size{-1};
            std::string field_name{nf->Identifier()->getText()};

            // max_len config
            if (field_type == FieldType::VARCHAR || field_type == FieldType::CHAR) {
                auto max_len_raw = std::stoi(nf->type_()->Integer()->getText());
                if (max_len_raw > RECORD_LEN_MAX) {
                    throw OperationError{"Column length too big for column `{} (max = {})`", field_name,
                                         RECORD_LEN_MAX};
                }
                max_size = max_len_raw;
            }

            auto new_field_meta{std::make_shared<FieldMeta>(
                    field_type,
                    field_name,
                    field_id_counter++,
                    max_size,
                    not_null != nullptr,
                    false,
                    nullptr
            )};


            // default value config
            if (nf->value()) {
                new_field_meta->has_default = true;
                new_field_meta->default_value = GetValue(nf->value(), new_field_meta);
            }

            field_meta.push_back(new_field_meta);
            continue;
        }

        auto fk{dynamic_cast<SQLParser::Foreign_key_fieldContext *>(field_ctx)};
        if (fk) {
            std::string fk_name;
            std::string reference_table_name;
            if (fk->Identifier().size() == 1) {
                fk_name = "";
                reference_table_name = fk->Identifier(0)->getText();
            } else {
                fk_name = fk->Identifier(0)->getText();
                reference_table_name = fk->Identifier(1)->getText();
            }
            std::vector<std::string> field_names;
            std::vector<std::string> reference_field_names;
            for (const auto &ident: fk->identifiers(0)->Identifier()) {
                field_names.emplace_back(ident->getText());
            }
            for (const auto &ident: fk->identifiers(1)->Identifier()) {
                reference_field_names.emplace_back(ident->getText());
            }
            raw_fks.emplace_back(fk_name, reference_table_name, field_names, reference_field_names);
            continue;
        }

        auto pk{dynamic_cast<SQLParser::Primary_key_fieldContext *>(field_ctx)};
        if (pk) {
            if (raw_pk) {
                throw OperationError{"Multiple primary keys defined"};
            }
            std::string pk_name;
            if (pk->Identifier()) {
                pk_name = pk->Identifier()->getText();
            }
            std::vector<std::string> field_names;
            for (const auto &ident: pk->identifiers()->Identifier()) {
                field_names.emplace_back(ident->getText());
            }
            raw_pk = RawPrimaryKey{pk_name, field_names};
            continue;
        }

        std::cout << field_ctx->getText() << std::endl;
    }


    return system.CreateTable(table_name, field_meta, raw_pk, raw_fks);
}

antlrcpp::Any DBVisitor::visitDrop_table(SQLParser::Drop_tableContext *ctx) {
    return system.DropTable(ctx->Identifier()->getText());
}

antlrcpp::Any DBVisitor::visitField_list(SQLParser::Field_listContext *ctx) {
    return SQLBaseVisitor::visitField_list(ctx);
}

antlrcpp::Any DBVisitor::visitUse_db(SQLParser::Use_dbContext *ctx) {
    return system.UseDatabase(ctx->Identifier()->getText());
}

antlrcpp::Any DBVisitor::visitInsert_into_table(SQLParser::Insert_into_tableContext *ctx) {
    visitChildren(ctx);
    auto table_name{ctx->Identifier()->getText()};
    auto table_id{system.GetTableID(table_name)};
    auto &table_meta{*system.GetTableMeta(table_id)};

    auto &field_meta{table_meta.field_meta};
    auto field_count{field_meta.Count()};
    auto &field_seq{field_meta.meta};
    auto insertions{ctx->value_lists()->value_list()};

    RecordList records;

    /**
     * Value Insertion for `INSERT` query without column specified
     */
    for (int row{0}; row < insertions.size(); ++row) {
        try {
            auto value_list{insertions[row]->value()};
            auto friendly_row{row + 1};  // adjust to human convention
            if (value_list.size() != field_count) {
                throw OperationError{"Column count doesn't match value count"};
            }
            std::vector<std::shared_ptr<Field>> fields;
            for (int i{0}; i < field_count; ++i) {
                fields.push_back(GetValue(value_list[i], field_seq[i]));
            }
            auto record{std::make_shared<Record>(std::move(fields))};
            records.push_back(record);
        } catch (OperationError &e) {
            throw OperationError{"At row {}, {}", row + 1, e.what()};
        }
    }
    return system.Insert(table_id, records);
}

antlrcpp::Any DBVisitor::visitDescribe_table(SQLParser::Describe_tableContext *ctx) {
    return system.DescribeTable(ctx->Identifier()->getText());
}

antlrcpp::Any DBVisitor::visitSelect_table(SQLParser::Select_tableContext *ctx) {
    // config `selected_tables`, note that other functions should keep it unchanged
    selected_tables_stack.push({});
    auto &selected_tables{selected_tables_stack.top()};

    for (const auto &ident_ctx: ctx->identifiers()->Identifier()) {
        auto table_id{system.GetTableID(ident_ctx->getText())};
        selected_tables.push_back(table_id);  // `selected_tables` follows the original order of user input
    }

//    DebugLog << "Selected tables" << boost::algorithm::join(
//                selected_tables | boost::adaptors::transformed(static_cast<std::string(*)(TableID)>(std::to_string)),
//                ", ");

    std::multimap<TableID, std::shared_ptr<FilterCondition>> filters;
    std::multimap<JoinPair, std::shared_ptr<JoinCondition>> joins;

    // get conditions
    if (ctx->where_and_clause()) {
        auto table_tree_any{ctx->where_and_clause()->accept(this)};
        auto [where_filters, where_joins]{
                table_tree_any.as<std::pair<std::multimap<TableID, std::shared_ptr<FilterCondition>>, std::multimap<JoinPair, std::shared_ptr<JoinCondition>>>>()};
        filters = where_filters;
        joins = where_joins;
    }

    // construct plan tree
    std::map<TableID, std::shared_ptr<OpNode>> table_scanners;
    std::shared_ptr<OpNode> root;

    // construct table scan nodes
    for (const auto &table_id: selected_tables) {
        std::shared_ptr<AndCondition> and_cond{nullptr};
        if (filters.count(table_id) > 0) {  // only add `AndCondition` where there is at least one condition
            and_cond = std::make_shared<AndCondition>(FilterConditionList{}, table_id);
            for (auto [i, end]{filters.equal_range(table_id)}; i != end; ++i) {
                and_cond->AddCondition(i->second);
            }
        }

        table_scanners.insert({table_id, system.GetScanNodeByCondition(table_id, and_cond)});
    }

    // join tables
    std::vector<TableID> join_sequence{selected_tables[0]};
    std::vector<FieldID> table_offset{0};  // corresponding to join_sequence
    if (selected_tables.size() > 1) {  // construct join nodes
        std::unordered_set<TableID> to_be_joined;
        for (TableID i{1}; i < selected_tables.size(); ++i) {  // mark [1: n-1] as "to be joined"
            to_be_joined.insert(selected_tables[i]);
        }

        // determine join order to use as many hash node as possible
        while (join_sequence.size() != selected_tables.size()) {
            // find a connected table
            bool found{false};
            for (auto joined_tid: join_sequence) {  // for all joined tables
                bool found_this_round{false};
                for (auto it1{to_be_joined.begin()}; it1 != to_be_joined.end(); ++it1) { // for all not-joined table
                    auto cond_tables{std::make_pair(joined_tid, *it1)};
                    auto rev_cond_tables{std::make_pair(*it1, joined_tid)};
                    auto [same_order_it, same_order_end]{joins.equal_range(cond_tables)};
                    auto [rev_order_it, rev_order_end]{joins.equal_range(rev_cond_tables)};
                    if (same_order_it != same_order_end || rev_order_it != rev_order_end) {
                        found = true;
                        found_this_round = true;
                        join_sequence.push_back(*it1);
                        to_be_joined.erase(it1);
                        break;
                    }
                }
                if (found_this_round) {
                    break;
                }
            }
            if (!found) {
                DebugLog << "[disconnected!]";
                // not a connected graph
                for (auto it2{to_be_joined.begin()}; it2 != to_be_joined.end(); ++it2) {
                    // all append
                    join_sequence.push_back(*it2);
                }
                to_be_joined.clear();
                break;
            }
        }
        assert(join_sequence.size() == selected_tables.size());
        assert(to_be_joined.empty());


        std::vector<std::shared_ptr<const TableMeta>> join_table_metas;
        for (const auto &table_id: join_sequence) {
            join_table_metas.push_back(system.GetTableMeta(table_id));
        }
        for (FieldID i{1}; i < join_sequence.size(); ++i) {
            table_offset[i] = table_offset[i - 1] + join_table_metas[i - 1]->field_meta.Count();
        }
        std::shared_ptr<OpNode> join_root{table_scanners[join_sequence[0]]};
        for (TableID i{1} /* index in join_sequence */; i < join_sequence.size(); ++i) {
            std::vector<std::shared_ptr<JoinCondition>> join_conds_i;
            for (TableID j{0}/* pointer to previous tables */; j < i; ++j) {
                auto cond_tables{std::make_pair(join_sequence[j], join_sequence[i])};
                auto rev_cond_tables{std::make_pair(cond_tables.second, cond_tables.first)};
                // same ordered conditions
                for (auto [it, end]{joins.equal_range(cond_tables)}; it != end; ++it) {
                    for (auto &cond: it->second->conditions) {  // offset the 1st table
                        std::get<0>(cond) += table_offset[j];
                    }
                    join_conds_i.push_back(it->second);
                }

                // reversed ordered conditions
                for (auto [it, end]{joins.equal_range(rev_cond_tables)}; it != end; ++it) {
                    it->second->Swap();
                    for (auto &cond: it->second->conditions) {  // offset the 1st table
                        std::get<0>(cond) += table_offset[j];
                    }
                    join_conds_i.push_back(it->second);
                }
            }
            auto join_cond{JoinCondition::Merge(join_conds_i, JoinPair{-1, join_sequence[i]})};
            // try construct hash join node
            auto it{std::find_if(join_cond->conditions.begin(), join_cond->conditions.end(),
                                 [](const JoinCond &jc) -> bool {
                                     return std::dynamic_pointer_cast<EqCmp>(std::get<2>(jc)) != nullptr;
                                 })};
            if (it != join_cond->conditions.end()) {  // use EqCmp to construct hash join
                auto hash_cond{std::make_shared<JoinCondition>(std::vector{*it}, join_cond->tables)};
                join_cond->conditions.erase(it);
                if (join_cond->conditions.empty()) {
                    join_cond = nullptr;
                }
                DebugLog << fmt::format("Hash Join @ table {}", i);
                join_root = std::make_shared<HashJoinNode>(join_root, table_scanners[join_sequence[i]], hash_cond,
                                                           join_cond);
            } else {  // does not exist EqCmp, use nested loop join
                DebugLog << fmt::format("Nested Join @ table {}", i);
                join_root = std::make_shared<NestedJoinNode>(join_root, table_scanners[join_sequence[i]], join_cond);
            }
        }
        root = join_root;
    } else {  // connect scan node directly
        assert(table_scanners.size() == 1);
        root = table_scanners.begin()->second;
    }

    // map from `table_id` to table position in sequence
    std::map<TableID, TableID> table_mapping;
    for (TableID i{0}; i < join_sequence.size(); ++i) {
        table_mapping.insert({join_sequence[i], i});
    }

    // transform selectors
    std::vector<std::string> header;
    std::vector<FieldID> target;
    std::vector<std::shared_ptr<Column>> columns;
    if (ctx->selectors()->children[0]->getText() == "*") {  // select all columns
        for (TableID i{0}; i < selected_tables.size(); ++i) {  // show in the order in which user selects tables
            auto table_pos{table_mapping[selected_tables[i]]};
            auto table_meta{system.GetTableMeta(selected_tables[i])};
            for (const auto &field: table_meta->field_meta.meta) {
                header.push_back(table_meta->table_name + "." + field->name);
                target.push_back(table_offset[table_pos] + field->field_id);
                columns.push_back(std::make_shared<Column>(selected_tables[i], field));
            }
        }
    } else {
        for (const auto &selector: ctx->selectors()->selector()) {
            header.push_back(selector->getText());
            auto col{selector->accept(this).as<std::shared_ptr<Column>>()};
            columns.push_back(col);
            if (col->type == ColumnType::COUNT_REC) {
                target.push_back(-1);
            } else {
                target.push_back(table_offset[table_mapping[col->table_id]] + col->field_meta->field_id);
            }
        }
    }

    assert(header.size() == columns.size());
    assert(header.size() == target.size());


    bool has_aggregator{std::any_of(columns.cbegin(), columns.cend(),
                                    [](const auto &col) { return col->type != ColumnType::BASIC; })};
    FieldID group_by_col{-1};
    std::shared_ptr<Column> group_by_ptr{nullptr};
    if (ctx->column()) {
        auto col{ctx->column()->accept(this).as<std::shared_ptr<Column>>()};
        auto table_id{col->table_id};
        auto field_meta{col->field_meta};
        group_by_col = table_offset[table_mapping[col->table_id]] + col->field_meta->field_id;
        group_by_ptr = col;
        // implies aggregating
        has_aggregator = true;
    }

    // when aggregator exists, basic column not quantified by GROUP BY is forbidden
    if (has_aggregator) {
        for (FieldID i{0}; i < columns.size(); ++i) {
            if (columns[i]->type == ColumnType::BASIC) {
                if (group_by_col >= 0 && *columns[i] == *group_by_ptr) {
                    continue;
                }
                throw OperationError{
                        "Expression #{} of SELECT list is not in GROUP BY clause and contains nonaggregated column `{}`",
                        i + 1,
                        header[i]
                };
            }
        }
    }

    // insert AggregateNode or ProjectNode
    if (has_aggregator) {
        root = std::make_shared<AggregateNode>(root, std::move(columns), group_by_col, std::move(target));
    } else {
        root = std::make_shared<ProjectNode>(root, std::move(target));
    }

    // insert OffsetLimitNode if possible
    if (ctx->Integer(0)) {
        auto limit_ctx{ctx->Integer(0)};
        auto offset_ctx{ctx->Integer(1)};
        auto limit_val{std::stoi(limit_ctx->getText())};
        if (limit_val < 0) {
            throw OperationError{"Invalid limit value {}", limit_val};
        }
        int offset_val{0};
        if (offset_ctx) {
            offset_val = std::stoi(offset_ctx->getText());
        }
        if (offset_val < 0) {
            throw OperationError{"Invalid offset value {}", offset_val};
        }
        root = std::make_shared<OffsetLimitNode>(root, limit_val, offset_val);
    }

    selected_tables_stack.pop();
    return std::make_pair(header, root);
}

antlrcpp::Any DBVisitor::visitSelect_table_(SQLParser::Select_table_Context *ctx) {
    const auto [header, root] {
            ctx->select_table()->accept(this).as<std::pair<std::vector<std::string>, std::shared_ptr<OpNode>>>()};
    return system.Select(header, root);
}

std::shared_ptr<Cmp> DBVisitor::ConvertOperator(SQLParser::Operator_Context *ctx) {
    if (ctx->EqualOrAssign()) {
        return std::make_shared<EqCmp>();
    }
    if (ctx->Less()) {
        return std::make_shared<LeCmp>();
    }
    if (ctx->Greater()) {
        return std::make_shared<GeCmp>();
    }
    if (ctx->LessEqual()) {
        return std::make_shared<LeqCmp>();
    }
    if (ctx->GreaterEqual()) {
        return std::make_shared<GeqCmp>();
    }
    if (ctx->NotEqual()) {
        return std::make_shared<NeqCmp>();
    }
    assert(false);
}

antlrcpp::Any DBVisitor::visitWhere_and_clause(SQLParser::Where_and_clauseContext *ctx) {
    std::multimap<TableID, std::shared_ptr<FilterCondition>> filters;
    std::multimap<JoinPair, std::shared_ptr<JoinCondition>> joins;

    // collect conditions
    for (const auto &clause: ctx->where_clause()) {
        auto condition{clause->accept(this).as<std::shared_ptr<Condition>>()};
        if (condition->type == ConditionType::FILTER) {
            auto fc{std::static_pointer_cast<FilterCondition>(condition)};
            filters.insert({fc->table_id, fc});
        } else if (condition->type == ConditionType::JOIN) {
            auto jc{std::static_pointer_cast<JoinCondition>(condition)};
            joins.insert({jc->tables, jc});
        } else {
            assert(false);
        }
    }

    return std::make_pair(filters, joins);
}

antlrcpp::Any DBVisitor::visitColumn(SQLParser::ColumnContext *ctx) {
    auto &selected_tables{selected_tables_stack.top()};
    auto table_name{ctx->Identifier(0)->getText()};
    auto field_name{ctx->Identifier(1)->getText()};
    auto table_id = system.GetTableID(table_name);
    if (std::find(selected_tables.cbegin(), selected_tables.cend(), table_id) == selected_tables.cend()) {
        throw OperationError{"Table {} is not selected", table_name};
    }

    const auto field_meta{system.GetTableMeta(table_id)->field_meta.ToMeta<OperationError>(
            field_name,
            "Table `{}` does not have column `{}`",
            table_name,
            field_name
    )};
    return std::make_shared<Column>(table_id, field_meta);
}

std::shared_ptr<Field>
DBVisitor::GetValue(SQLParser::ValueContext *ctx, const std::shared_ptr<FieldMeta> &selected_field,
                    bool ignore_max_size) {
    auto null_p{ctx->Null()};
    if (null_p) {
        return Field::MakeNull(selected_field->type, selected_field->max_size);
    }

    auto int_p{ctx->Integer()};
    if (int_p) {
        if (selected_field->type == FieldType::INT) {
            return std::make_shared<Int>(std::stoi(int_p->getText()));
        } else if (selected_field->type == FieldType::FLOAT) {
            return std::make_shared<Float>(std::stof(int_p->getText()));
        } else {
            auto type_name{magic_enum::enum_name(selected_field->type)};
            throw OperationError{
                    "Incompatible type; Expecting {} for column {}, got INT instead",
                    type_name,
                    selected_field->name
            };
        }
    }

    auto float_p{ctx->Float()};
    if (float_p) {
        if (selected_field->type == FieldType::FLOAT) {
            return std::make_shared<Float>(std::stof(float_p->getText()));
        } else {
            auto type_name{magic_enum::enum_name(selected_field->type)};
            throw OperationError{
                    "Incompatible type; Expecting {} for column {}, got FLOAT instead",
                    type_name,
                    selected_field->name
            };
        }
    }

//    auto date_p{ctx->Date()};
//    if (date_p) {
//        if (selected_field->type == FieldType::DATE) {
//            return std::make_shared<Date>(date_p->getText());
//        } else {
//            auto type_name{magic_enum::enum_name(selected_field->type)};
//            throw OperationError{
//                    "Incompatible type; Expecting {} for column {}, got DATE instead",
//                    type_name,
//                    selected_field->name
//            };
//        }
//    }

    auto str_p{ctx->String()};
    if (str_p) {
        std::string str_val{str_p->getText()};
        strip_quote(str_val);
        if (selected_field->type == FieldType::CHAR) {
            if (!ignore_max_size && str_val.size() > selected_field->max_size) {
                throw OperationError{"Data too long for column `{}`", selected_field->name};
            }
            return std::make_shared<Char>(str_val, selected_field->max_size);
        } else if (selected_field->type == FieldType::VARCHAR) {
            if (!ignore_max_size && str_val.size() > selected_field->max_size) {
                throw OperationError{"Data too long for column `{}`", selected_field->name,};
            }
            return std::make_shared<VarChar>(str_val);
        }
        if (selected_field->type == FieldType::DATE) {
            return std::make_shared<Date>(str_val);
        } else {
            auto type_name{magic_enum::enum_name(selected_field->type)};
            throw OperationError{
                    "Incompatible type; Expecting {} for column {}, got STRING instead",
                    type_name, selected_field->name
            };
        }
    }
    assert(false);
}

antlrcpp::Any DBVisitor::visitWhere_operator_expression(SQLParser::Where_operator_expressionContext *ctx) {
    auto col{ctx->column()->accept(this).as<std::shared_ptr<Column>>()};
    auto table_id{col->table_id};
    auto field_meta{col->field_meta};
    auto comparer{ConvertOperator(ctx->operator_())};
    if (ctx->expression()->value()) {
        auto value_field{GetValue(ctx->expression()->value(), field_meta, true)};
        return make_cond<ValueCompareCondition>(value_field, table_id, field_meta->field_id, comparer);
    }
    if (ctx->expression()->column()) {
        auto r_col{ctx->expression()->column()->accept(this).as<std::shared_ptr<Column>>()};
        if (field_meta->type != r_col->field_meta->type) {
            throw OperationError{
                    "Type mismatch in {}.{} and {}.{}",
                    system.GetTableMeta(col->table_id)->table_name,
                    col->field_meta->name,
                    system.GetTableMeta(r_col->table_id)->table_name,
                    r_col->field_meta->name
            };
        }
        if (r_col->table_id == table_id) {
            return make_cond<FieldCmpCondition>(table_id, field_meta->field_id, r_col->field_meta->field_id,
                                                comparer);
        } else {
            return make_cond<JoinCondition>(
                    std::vector<JoinCond>{{field_meta->field_id, r_col->field_meta->field_id, comparer}},
                    std::make_pair(table_id, r_col->table_id)
            );
        }
    }
    assert(false);
}

antlrcpp::Any DBVisitor::visitWhere_operator_select(SQLParser::Where_operator_selectContext *ctx) {
    auto col{ctx->column()->accept(this).as<std::shared_ptr<Column>>()};
    auto table_id{col->table_id};
    auto field_meta{col->field_meta};
    auto select_plan{ctx->select_table()->accept(this).as<SelectPlan>()};
    return make_cond<CompareSubQueryCondition>(
            select_plan, table_id, field_meta->field_id, ConvertOperator(ctx->operator_()), field_meta->type
    );
}

antlrcpp::Any DBVisitor::visitWhere_null(SQLParser::Where_nullContext *ctx) {
    auto col{ctx->column()->accept(this).as<std::shared_ptr<Column>>()};
    auto table_id{col->table_id};
    auto field_meta{col->field_meta};
    bool filter_not_null{ctx->children[2]->getText() == "NOT"};
    return make_cond<NullCompCondition>(table_id, field_meta->field_id, filter_not_null);
}

antlrcpp::Any DBVisitor::visitWhere_in_select(SQLParser::Where_in_selectContext *ctx) {
    auto col{ctx->column()->accept(this).as<std::shared_ptr<Column>>()};
    auto table_id{col->table_id};
    auto field_meta{col->field_meta};
    auto select_plan{ctx->select_table()->accept(this).as<SelectPlan>()};
    return make_cond<InSubQueryCondition>(select_plan, table_id, field_meta->field_id, field_meta->type);
}

antlrcpp::Any DBVisitor::visitWhere_like_string(SQLParser::Where_like_stringContext *ctx) {
    auto col{ctx->column()->accept(this).as<std::shared_ptr<Column>>()};
    auto table_id{col->table_id};
    auto field_meta{col->field_meta};
    auto sql_pattern{ctx->String()->getText()};
    strip_quote(sql_pattern);
    return make_cond<LikeCondition>(sql_pattern, table_id, field_meta->field_id);
}

antlrcpp::Any DBVisitor::visitWhere_in_list(SQLParser::Where_in_listContext *ctx) {
    auto col{ctx->column()->accept(this).as<std::shared_ptr<Column>>()};
    auto table_id{col->table_id};
    auto field_meta{col->field_meta};
    std::vector<std::shared_ptr<Field>> values;
    for (const auto &value_ctx: ctx->value_list()->value()) {
        values.push_back(GetValue(value_ctx, field_meta, true));
    }
    return make_cond<ValueInListCondition>(values, table_id, field_meta->field_id);
}

antlrcpp::Any DBVisitor::visitSelector(SQLParser::SelectorContext *ctx) {
    if (ctx->column()) {
        auto col{ctx->column()->accept(this).as<std::shared_ptr<Column>>()};
        auto ag{ctx->aggregator()};
        if (ag) {
            if (ag->Count()) {
                col->type = ColumnType::COUNT;
            } else if (ag->Average()) {
                col->type = ColumnType::AVG;
            } else if (ag->Max()) {
                col->type = ColumnType::MAX;
            } else if (ag->Min()) {
                col->type = ColumnType::MIN;
            } else if (ag->Sum()) {
                col->type = ColumnType::SUM;
            } else {
                assert(false);
            }
        }
        return col;
    }
    // `COUNT(*)`
    return std::make_shared<Column>(-1, nullptr, ColumnType::COUNT_REC);
}

antlrcpp::Any DBVisitor::visitDelete_from_table(SQLParser::Delete_from_tableContext *ctx) {
    auto table_name{ctx->Identifier()->getText()};
    auto table_id{system.GetTableID(table_name)};

    selected_tables_stack.push({});
    auto &selected_tables{selected_tables_stack.top()};

    selected_tables.clear();
    selected_tables.push_back(table_id);

    auto table_tree_any{ctx->where_and_clause()->accept(this)};
    auto &[filters, joins]{
            table_tree_any.as<
                    std::pair<
                            std::multimap<TableID, std::shared_ptr<FilterCondition>>,
                            std::multimap<JoinPair, std::shared_ptr<JoinCondition>>
                    >
            >()
    };
    assert(joins.empty());
    assert(filters.count(table_id) == filters.size());

    FilterConditionList filter_list;
    for (const auto &[_, cond]: filters) {
        filter_list.push_back(cond);
    }
    auto and_cond{std::make_shared<AndCondition>(std::move(filter_list), table_id)};

    selected_tables_stack.pop();
    return system.Delete(table_id, and_cond);
}

void DBVisitor::strip_quote(std::string &possibly_quoted) {
    if (possibly_quoted.empty()) {
        return;
    }
    auto begin{possibly_quoted.begin()};
    if (*begin == '\'') {
        possibly_quoted.erase(begin);
    }
    if (possibly_quoted.empty()) {
        return;
    }
    auto end{std::prev(possibly_quoted.end())};
    if (*end == '\'') {
        possibly_quoted.erase(end);
    }
}


antlrcpp::Any DBVisitor::visitUpdate_table(SQLParser::Update_tableContext *ctx) {
    auto table_name{ctx->Identifier()->getText()};
    auto table_id{system.GetTableID(table_name)};

    selected_tables_stack.push({});
    auto &selected_tables{selected_tables_stack.top()};

    selected_tables.clear();
    selected_tables.push_back(table_id);

    auto updates{ctx->set_clause()->accept(this)
                         .as<std::vector<std::pair<std::shared_ptr<FieldMeta>, std::shared_ptr<Field>>>>()};

    auto table_tree_any{ctx->where_and_clause()->accept(this)};
    auto &[filters, joins]{
            table_tree_any.as<
                    std::pair<
                            std::multimap<TableID, std::shared_ptr<FilterCondition>>,
                            std::multimap<JoinPair, std::shared_ptr<JoinCondition>>
                    >
            >()
    };
    assert(joins.empty());
    assert(filters.count(table_id) == filters.size());

    FilterConditionList filter_list;
    for (const auto &[_, cond]: filters) {
        filter_list.push_back(cond);
    }
    auto and_cond{std::make_shared<AndCondition>(std::move(filter_list), table_id)};

    selected_tables_stack.pop();

    return system.Update(table_id, updates, and_cond);
}

antlrcpp::Any DBVisitor::visitSet_clause(SQLParser::Set_clauseContext *ctx) {
    assert(ctx->Identifier().size() == ctx->value().size());
    auto tables{selected_tables_stack.top()};
    assert(tables.size() == 1);
    auto table_id{tables[0]};
    const auto &meta{system.GetTableMeta(table_id)};
    std::vector<std::pair<std::shared_ptr<FieldMeta>, std::shared_ptr<Field>>> updates;
    for (FieldID i{0}; i < ctx->Identifier().size(); ++i) {
        auto field_name{ctx->Identifier(i)->getText()};
        const auto field_meta{meta->field_meta.ToMeta<OperationError>(
                field_name,
                "Table `{}` does not have column `{}`",
                meta->table_name,
                field_name
        )};
        updates.emplace_back(field_meta, GetValue(ctx->value(i), field_meta, false));
    }
    return updates;
}

antlrcpp::Any DBVisitor::visitAlter_add_index(SQLParser::Alter_add_indexContext *ctx) {
    visitChildren(ctx);
    auto table_name{ctx->Identifier()->getText()};

    std::vector<std::string> indexed_fields;
    for (auto field: ctx->identifiers()->Identifier()) {
        indexed_fields.push_back(field->getText());
    }

    return system.AddIndex(table_name, indexed_fields);
}


antlrcpp::Any DBVisitor::visitAlter_drop_index(SQLParser::Alter_drop_indexContext *ctx) {
    visitChildren(ctx);
    auto table_name{ctx->Identifier()->getText()};

    std::vector<std::string> indexed_fields;
    for (auto field: ctx->identifiers()->Identifier()) {
        indexed_fields.push_back(field->getText());
    }

    return system.DropIndex(table_name, indexed_fields);
}


antlrcpp::Any DBVisitor::visitShow_tables(SQLParser::Show_tablesContext *ctx) {
    return system.ShowTables();
}

antlrcpp::Any DBVisitor::visitAlter_table_drop_pk(SQLParser::Alter_table_drop_pkContext *ctx) {
    std::string pk_name;
    if (ctx->Identifier().size() == 2) {
        pk_name = ctx->Identifier(1)->getText();
    }
    auto table_name{ctx->Identifier(0)->getText()};
    return system.DropPrimaryKey(table_name, pk_name);
}

antlrcpp::Any DBVisitor::visitAlter_table_drop_foreign_key(SQLParser::Alter_table_drop_foreign_keyContext *ctx) {
    auto table_name{ctx->Identifier(0)->getText()};
    auto fk_name{ctx->Identifier(1)->getText()};
    return system.DropForeignKey(table_name, fk_name);
}

antlrcpp::Any DBVisitor::visitAlter_table_add_pk(SQLParser::Alter_table_add_pkContext *ctx) {
    auto table_name{ctx->Identifier(0)->getText()};
    std::string pk_name;
    if (ctx->Identifier().size() == 2) {
        pk_name = ctx->Identifier(1)->getText();
    }

    std::vector<std::string> pk_fields;
    for (auto field: ctx->identifiers()->Identifier()) {
        pk_fields.push_back(field->getText());
    }

    return system.AddPrimaryKey(table_name, {pk_name, std::move(pk_fields)});
}

antlrcpp::Any DBVisitor::visitAlter_table_add_foreign_key(SQLParser::Alter_table_add_foreign_keyContext *ctx) {
    auto table_name{ctx->Identifier(0)->getText()};
    auto fk_name{ctx->Identifier(1)->getText()};
    auto ref_name{ctx->Identifier(2)->getText()};

    std::vector<std::string> ch_fields;
    for (auto field: ctx->identifiers(0)->Identifier()) {
        ch_fields.push_back(field->getText());
    }
    std::vector<std::string> ref_fields;
    for (auto field: ctx->identifiers(1)->Identifier()) {
        ref_fields.push_back(field->getText());
    }

    return system.AddForeignKey(table_name, {fk_name, ref_name, std::move(ch_fields), std::move(ref_fields)});
}

antlrcpp::Any DBVisitor::visitAlter_table_add_unique(SQLParser::Alter_table_add_uniqueContext *ctx) {
    auto table_name{ctx->Identifier()->getText()};
    std::vector<std::string> uk_fields;
    for (auto field: ctx->identifiers()->Identifier()) {
        uk_fields.push_back(field->getText());
    }

    return system.AddUnique(table_name, uk_fields);
}