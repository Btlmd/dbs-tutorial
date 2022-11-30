//
// Created by lambda on 22-11-17.
//

#ifndef DBS_TUTORIAL_DBVISITOR_H
#define DBS_TUTORIAL_DBVISITOR_H

#include "grammar/SQLBaseVisitor.h"
#include "DBSystem.h"

class DBVisitor : public SQLBaseVisitor {
public:
    DBSystem &system;

    explicit DBVisitor(DBSystem &system_mgr) : system{system_mgr} {}

    antlrcpp::Any visitUse_db(SQLParser::Use_dbContext *ctx) override;

    antlrcpp::Any visitCreate_db(SQLParser::Create_dbContext *ctx) override;

    antlrcpp::Any visitShow_dbs(SQLParser::Show_dbsContext *ctx) override;

    antlrcpp::Any visitDrop_db(SQLParser::Drop_dbContext *ctx) override;

    antlrcpp::Any visitProgram(SQLParser::ProgramContext *ctx) override;

    antlrcpp::Any visitStatement(SQLParser::StatementContext *ctx) override;

    antlrcpp::Any visitCreate_table(SQLParser::Create_tableContext *ctx) override;

    antlrcpp::Any visitDrop_table(SQLParser::Drop_tableContext *ctx) override;

    antlrcpp::Any visitField_list(SQLParser::Field_listContext *ctx) override;

    antlrcpp::Any visitInsert_into_table(SQLParser::Insert_into_tableContext *context) override;

    antlrcpp::Any visitDescribe_table(SQLParser::Describe_tableContext *context) override;

};


#endif //DBS_TUTORIAL_DBVISITOR_H
