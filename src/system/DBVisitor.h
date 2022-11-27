//
// Created by lambda on 22-11-17.
//

#ifndef DBS_TUTORIAL_DBVISITOR_H
#define DBS_TUTORIAL_DBVISITOR_H

#include <vector>

#include "system/grammar/SQLBaseVisitor.h"
#include "DBSystem.h"

class DBVisitor : public SQLBaseVisitor {
public:
    DBSystem &system;

    explicit DBVisitor(DBSystem &system_mgr) : system{system_mgr} {}

    antlrcpp::Any visitCreate_db(SQLParser::Create_dbContext *ctx) override;

    antlrcpp::Any visitShow_dbs(SQLParser::Show_dbsContext *ctx) override;

    antlrcpp::Any visitDrop_db(SQLParser::Drop_dbContext *ctx) override;

    antlrcpp::Any visitProgram(SQLParser::ProgramContext *ctx) override;

    antlrcpp::Any visitStatement(SQLParser::StatementContext *ctx) override;

    antlrcpp::Any visitCreate_table(SQLParser::Create_tableContext *ctx) override;

    antlrcpp::Any visitDrop_table(SQLParser::Drop_tableContext *ctx) override;

    antlrcpp::Any visitField_list(SQLParser::Field_listContext *ctx) override;

    antlrcpp::Any visitNormal_field(SQLParser::Normal_fieldContext *ctx) override;

    antlrcpp::Any visitPrimary_key_field(SQLParser::Primary_key_fieldContext *ctx) override;

    antlrcpp::Any visitForeign_key_field(SQLParser::Foreign_key_fieldContext *ctx) override;

};

typedef std::vector<antlrcpp::Any> ResultList;

typedef std::tuple<std::string, std::string, std::vector<std::string>, std::vector<std::string>> RawForeignKey;
typedef std::tuple<std::string, std::vector<std::string>> RawPrimaryKey;


#endif //DBS_TUTORIAL_DBVISITOR_H
