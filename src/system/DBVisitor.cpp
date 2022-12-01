//
// Created by lambda on 22-11-17.
//

#include "DBVisitor.h"

#include <defines.h>
#include <system/DBSystem.h>
#include <exception/OperationException.h>
#include <system/WhereConditions.h>

#include <magic_enum.hpp>

#include <chrono>
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
        if (dynamic_cast<SQLParser::StatementContext *>(child)) {
            DebugLog << "Process query: " << child->getText();
            auto begin{std::chrono::high_resolution_clock::now()};
            antlrcpp::Any result{child->accept(this)};
            std::chrono::duration<double> elapse{std::chrono::high_resolution_clock::now() - begin};
            auto result_ptr{result.as<std::shared_ptr<Result >>()};
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

            // default value config
            auto default_val_node{nf->value()};
            std::shared_ptr<Field> default_value{nullptr};
            try {
                if (default_val_node) {
                    switch (field_type) {  // TODO: use `GetValue` instead, this implementation is in correct
                        case FieldType::INT:
                            default_value = std::make_shared<Int>(std::stoi(default_val_node->getText()));
                            break;
                        case FieldType::FLOAT:
                            default_value = std::make_shared<Float>(std::stof(default_val_node->getText()));
                            break;
                        case FieldType::VARCHAR:
                            default_value = std::make_shared<VarChar>(default_val_node->getText());
                            break;
                        case FieldType::CHAR:
                            break;
                    }
                }
            } catch (std::invalid_argument &e) {
                throw OperationError{"Invalid default value for `{}`", field_name};
            }

            // max_len config
            if (field_type == FieldType::VARCHAR) {
                auto max_len_raw = std::stoi(nf->type_()->Integer()->getText());
                if (max_len_raw > RECORD_LEN_MAX) {
                    throw OperationError{"Column length too big for column `{} (max = {})`", field_name,
                                         RECORD_LEN_MAX};
                }
                max_size = max_len_raw;
            }


            field_meta.push_back(std::shared_ptr<FieldMeta>(new FieldMeta{
                    .type{field_type},
                    .name{field_name},
                    .field_id{field_id_counter++},

                    .max_size{max_size},
                    .unique{false},
                    .not_null{not_null == nullptr},
                    .has_default{default_value == nullptr},
                    .default_value{default_value},
            }));
            continue;
        }

        auto fk{dynamic_cast<SQLParser::Foreign_key_fieldContext *>(field_ctx)};
        if (fk) {
            std::string fk_name;
            if (fk->Identifier(0)) {
                fk_name = fk->Identifier(0)->getText();
            }
            std::string reference_table_name{fk->Identifier(1)->getText()};
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
                throw OperationError{"Multiple primary key defined"};
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
    return SQLBaseVisitor::visitDrop_table(ctx);
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

    /**
     * Value Insertion for `INSERT` query without column specified
     */
    for (int row{0}; row < insertions.size(); ++row) {
        try {
            auto value_list{insertions[row]->value()};
            auto friendly_row{row + 1};  // adjust to human convention
            if (value_list.size() != field_count) {
                throw OperationError{
                        "Column count doesn't match value count at row {}",
                        friendly_row
                };
            }
            std::vector<std::shared_ptr<Field>> fields;
            for (int i{0}; i < field_count; ++i) {
                fields.push_back(GetValue(value_list[i], field_seq[i]));
            }
            auto record{std::make_shared<Record>(std::move(fields))};
            system.CheckConstraint(table_meta, record);
            system.Insert(table_id, record);
        } catch (OperationError &e) {
            auto result{system.InsertResult()};
            result->AddInfo(e.what());
            return result;
        }
    }
    return system.InsertResult();
}

antlrcpp::Any DBVisitor::visitDescribe_table(SQLParser::Describe_tableContext *ctx) {
    return system.DescribeTable(ctx->Identifier()->getText());
}

antlrcpp::Any DBVisitor::visitSelect_table(SQLParser::Select_tableContext *ctx) {
    return SQLBaseVisitor::visitSelect_table(ctx);
}

antlrcpp::Any DBVisitor::visitSelect_table_(SQLParser::Select_table_Context *ctx) {
    return SQLBaseVisitor::visitSelect_table_(ctx);
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

}

antlrcpp::Any DBVisitor::visitColumn(SQLParser::ColumnContext *ctx) {
    auto table_name{ctx->Identifier(0)->getText()};
    auto field_name{ctx->Identifier(1)->getText()};
    auto table_id = system.GetTableID(table_name);
    if (selected_tables.find(table_id) == selected_tables.end()) {
        throw OperationError{"Table {} is not selected", table_name};
    }

    const auto &field_meta{system.GetTableMeta(table_id)->field_meta.ToMeta<OperationError>(
            field_name,
            "Table {} does not have column {}",
            table_name,
            field_name
    )};
    return std::make_pair(table_id, field_meta);
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

    auto str_p{ctx->String()};
    if (str_p) {
        std::string str_val{str_p->getText()};
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
    auto [table_id, field_meta] {ctx->column()->accept(this).as<std::pair<TableID, std::shared_ptr<FieldMeta>>>()};
    auto comparer{ConvertOperator(ctx->operator_())};
    if (ctx->expression()->value()) {
        auto value_field{GetValue(ctx->expression()->value(), field_meta, true)};
        return make_cond<ValueCompareCondition>(value_field, table_id, field_meta->field_id, comparer);
    }
    if (ctx->expression()->column()) {
        auto [rhs_table_id, rhs_field_meta] {
                ctx->expression()->column()->accept(this).as<std::pair<TableID, std::shared_ptr<FieldMeta>>>()
        };
        if (rhs_table_id == table_id) {
            return make_cond<FieldCmpCondition>(table_id, field_meta->field_id, rhs_field_meta->field_id,
                                                       comparer);
        } else {
            return make_cond<JoinCondition>(
                    std::vector<JoinCond>{{field_meta->field_id, rhs_field_meta->field_id, comparer}});
        }
    }
    assert(false);
}

antlrcpp::Any DBVisitor::visitWhere_operator_select(SQLParser::Where_operator_selectContext *ctx) {
    // TODO
    return SQLBaseVisitor::visitWhere_operator_select(ctx);
}

antlrcpp::Any DBVisitor::visitWhere_null(SQLParser::Where_nullContext *ctx) {
    auto [table_id, field_meta] {ctx->column()->accept(this).as<std::pair<TableID, std::shared_ptr<FieldMeta>>>()};
    bool filter_not_null{ctx->children[2]->getText() == "NOT"};
    return make_cond<NullCompCondition>(table_id, field_meta->field_id, filter_not_null);
}

antlrcpp::Any DBVisitor::visitWhere_in_select(SQLParser::Where_in_selectContext *ctx) {
    // TODO
    return SQLBaseVisitor::visitWhere_in_select(ctx);
}

antlrcpp::Any DBVisitor::visitWhere_like_string(SQLParser::Where_like_stringContext *ctx) {
    auto [table_id, field_meta] {ctx->column()->accept(this).as<std::pair<TableID, std::shared_ptr<FieldMeta>>>()};
    return make_cond<LikeCondition>(ctx->String()->getText(), table_id, field_meta->field_id);
}

antlrcpp::Any DBVisitor::visitWhere_in_list(SQLParser::Where_in_listContext *ctx) {
    auto [table_id, field_meta] {ctx->column()->accept(this).as<std::pair<TableID, std::shared_ptr<FieldMeta>>>()};
    std::vector<std::shared_ptr<Field>> values;
    for (const auto &value_ctx: ctx->value_list()->value()) {
        values.push_back(GetValue(value_ctx, field_meta, true));
    }
    return make_cond<ValueInListCondition>(values, table_id, field_meta->field_id);
}