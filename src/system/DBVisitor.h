//
// Created by lambda on 22-11-17.
//

#ifndef DBS_TUTORIAL_DBVISITOR_H
#define DBS_TUTORIAL_DBVISITOR_H

#include <stack>

#include <grammar/SQLBaseVisitor.h>
#include <system/DBSystem.h>
#include <system/WhereConditions.h>


class DBVisitor : public SQLBaseVisitor {
private:
    DBSystem &system;

    std::stack<std::vector<TableID>> selected_tables_stack;

    static std::shared_ptr<Cmp> ConvertOperator(SQLParser::Operator_Context *ctx);

    static std::shared_ptr<Field> GetValue(SQLParser::ValueContext *ctx, const std::shared_ptr<FieldMeta> &selected_field, bool ignore_max_size = false);

    static void strip_quote(std::string &possibly_quoted);

public:
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

    antlrcpp::Any visitInsert_into_table(SQLParser::Insert_into_tableContext *ctx) override;

    antlrcpp::Any visitDescribe_table(SQLParser::Describe_tableContext *ctx) override;

    antlrcpp::Any visitSelect_table_(SQLParser::Select_table_Context *ctx) override;

    antlrcpp::Any visitSelect_table(SQLParser::Select_tableContext *ctx) override;

    antlrcpp::Any visitWhere_and_clause(SQLParser::Where_and_clauseContext *ctx) override;

    antlrcpp::Any visitWhere_operator_expression(SQLParser::Where_operator_expressionContext *ctx) override;

    antlrcpp::Any visitWhere_operator_select(SQLParser::Where_operator_selectContext *ctx) override;

    antlrcpp::Any visitWhere_null(SQLParser::Where_nullContext *ctx) override;

    antlrcpp::Any visitWhere_in_list(SQLParser::Where_in_listContext *ctx) override;

    antlrcpp::Any visitWhere_in_select(SQLParser::Where_in_selectContext *ctx) override;

    antlrcpp::Any visitWhere_like_string(SQLParser::Where_like_stringContext *ctx) override;

    antlrcpp::Any visitColumn(SQLParser::ColumnContext *ctx) override;

    antlrcpp::Any visitSelector(SQLParser::SelectorContext *ctx) override;

    antlrcpp::Any visitDelete_from_table(SQLParser::Delete_from_tableContext *ctx) override;

    antlrcpp::Any visitUpdate_table(SQLParser::Update_tableContext *ctx) override;

    antlrcpp::Any visitSet_clause(SQLParser::Set_clauseContext *ctx) override;

    antlrcpp::Any visitAlter_add_index(SQLParser::Alter_add_indexContext *ctx) override;

    antlrcpp::Any visitAlter_drop_index(SQLParser::Alter_drop_indexContext *ctx) override;

};


#endif //DBS_TUTORIAL_DBVISITOR_H
