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

antlrcpp::Any DBVisitor::visitCreate_db(SQLParser::Create_dbContext *ctx) {
    visitChildren(ctx);
    std::string db_name{ctx->Identifier()->getSymbol()->getText()};
    return system.CreateDatabase(db_name);
}

antlrcpp::Any DBVisitor::visitProgram(SQLParser::ProgramContext *ctx) {
    auto results{new ResultList};
    for (auto &child: ctx->children) {
        if (dynamic_cast<SQLParser::StatementContext *>(child)) {
            DebugLog << "Process query: " << child->getText();
            auto begin{std::chrono::high_resolution_clock::now()};
            antlrcpp::Any result{child->accept(this)};
            std::chrono::duration<double> elapse{std::chrono::high_resolution_clock::now() - begin};
            result.as<Result *>()->SetRuntime(elapse.count());
            results->emplace_back(result);
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
    std::vector<FieldMeta *> field_meta;

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
            Field *default_value{nullptr};
            try {
                if (default_val_node) {
                    switch (field_type) {
                        case FieldType::INT:
                            default_value = new Int{std::stoi(default_val_node->getText())};
                            break;
                        case FieldType::FLOAT:
                            default_value = new Float{std::stof(default_val_node->getText())};
                            break;
                        case FieldType::VARCHAR:
                            default_value = new VarChar{default_val_node->getText()};
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


            field_meta.push_back(new FieldMeta{
                    .type{field_type},
                    .name{field_name},
                    .field_id{field_id_counter++},

                    .max_size{max_size},
                    .unique{false},
                    .not_null{not_null == nullptr},
                    .has_default{default_value == nullptr},
                    .default_value{default_value},
            });
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

antlrcpp::Any DBVisitor::visitNormal_field(SQLParser::Normal_fieldContext *ctx) {
    std::cout << "normal fc" << ctx->getText() << std::endl;
    return SQLBaseVisitor::visitNormal_field(ctx);
}

antlrcpp::Any DBVisitor::visitForeign_key_field(SQLParser::Foreign_key_fieldContext *ctx) {
    visitChildren(ctx);
    std::string fk_name{ctx->Identifier(0)->getText()};
    std::string reference_table_name{ctx->Identifier(1)->getText()};
    std::vector<std::string> field_names;
    std::vector<std::string> reference_field_names;
    for (const auto &ident: ctx->identifiers(0)->Identifier()) {
        field_names.emplace_back(ident->getText());
    }
    for (const auto &ident: ctx->identifiers(1)->Identifier()) {
        reference_field_names.emplace_back(ident->getText());
    }
    std::cout << "fk_name " << fk_name << std::endl;
    std::cout << "fk_ref " << reference_table_name << std::endl;
    std::cout << "foreign_keys fc " << field_names[0] << std::endl;
    std::cout << "foreign_keys fc " << reference_field_names[0] << std::endl;
    return ctx;
}

antlrcpp::Any DBVisitor::visitPrimary_key_field(SQLParser::Primary_key_fieldContext *ctx) {
    std::cout << "primary_key fc" << ctx->getText() << std::endl;
    return SQLBaseVisitor::visitPrimary_key_field(ctx);
}

antlrcpp::Any DBVisitor::visitUse_db(SQLParser::Use_dbContext *ctx) {
    return system.UseDatabase(ctx->Identifier()->getText());
}

