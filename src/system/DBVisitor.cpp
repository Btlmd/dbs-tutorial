//
// Created by lambda on 22-11-17.
//

#include "DBVisitor.h"

#include "defines.h"
#include "DBSystem.h"
#include "exception/OperationException.h"
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
            auto result_ptr{result.as<std::shared_ptr<Result>>()};
            result_ptr->SetRuntime(elapse.count());
            results->emplace_back(result_ptr);
//            exit(-1);
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
            std::shared_ptr<Field>default_value{nullptr};
            try {
                if (default_val_node) {
                    switch (field_type) {
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
    auto &field_meta{system.GetTableMeta(table_id)->field_meta};
    auto field_count{field_meta.Count()};
    auto &field_seq{field_meta.field_seq};
    auto insertions{ctx->value_lists()->value_list()};

    /**
     * Value Insertion for `INSERT` query without column specified
     */
    for (int row{0}; row < insertions.size(); ++row) {
        auto value_list{insertions[row]->value()};
        auto friendly_row{row + 1};  // adjust to human convention
        if (value_list.size() != field_count) {
            throw OperationError{
                    "Column count doesn't match value count at row {}",
                    friendly_row
            };
        }
        auto null_bitmap{new NullBitmap{field_count}};
        std::vector<std::shared_ptr<Field>> fields;
        for (int i{0}; i < field_count; ++i) {
            auto null_p{value_list[i]->Null()};
            if (null_p) {
                null_bitmap->Set(i);
                continue;
            }

            auto int_p{value_list[i]->Integer()};
            if (int_p) {
                if (field_seq[i]->type == FieldType::INT) {
                    fields.push_back(std::make_shared<Int>(std::stoi(int_p->getText())));
                } else if (field_seq[i]->type == FieldType::FLOAT) {
                    fields.push_back(std::make_shared<Float>(std::stof(int_p->getText())));
                } else {
                    auto type_name{magic_enum::enum_name(fields[i]->type)};
                    throw OperationError{
                            "Incompatiable type in row {}; Expecting {} for column {}, got INT instead",
                            friendly_row,
                            type_name,
                            field_seq[i]->name
                    };
                }
                continue;
            }

            auto float_p{value_list[i]->Float()};
            if (float_p) {
                if (field_seq[i]->type == FieldType::FLOAT) {
                    fields.push_back(std::make_shared<Float>(std::stof(float_p->getText())));
                } else {
                    auto type_name{magic_enum::enum_name(fields[i]->type)};
                    throw OperationError{
                            "Incompatible type in row {}; Expecting {} for column {}, got FLAOT instead",
                            friendly_row,
                            type_name,
                            field_seq[i]->name
                    };
                }
                continue;
            }

            auto str_p{value_list[i]->String()};
            if (str_p) {
                std::string str_val{str_p->getText()};
                if (field_seq[i]->type == FieldType::CHAR) {
                    if (str_val.size() > field_seq[i]->max_size) {
                        throw OperationError{
                                "Data too long for column `{}` at row {}",
                                field_seq[i]->name,
                                friendly_row
                        };
                    }
                    auto char_f{std::make_shared<Char>(field_seq[i]->max_size)};
                    str_p->getText().copy(char_f->data, str_p->getText().size(), field_seq[i]->max_size);
                    fields.push_back(char_f);
                } else if (field_seq[i]->type == FieldType::VARCHAR) {
                    if (str_val.size() > field_seq[i]->max_size) {
                        throw OperationError{
                                "Data too long for column `{}` at row {}",
                                field_seq[i]->name,
                                friendly_row
                        };
                    }
                    fields.push_back(std::make_shared<VarChar>(str_val));
                } else {
                    auto type_name{magic_enum::enum_name(fields[i]->type)};
                    throw OperationError{
                            "Incompatible type in row {}; Expecting {} for column {}, got STRING instead",
                            friendly_row,
                            type_name,
                            field_seq[i]->name
                    };
                }
                continue;
            }

        }
    }
}

