//
// Created by lambda on 23-1-5.
//
#include <grammar/SQLLexer.h>
#include <grammar/SQLBaseVisitor.h>

#include <record/Field.h>
#include <magic_enum.hpp>
#include <tuple>
#include <fstream>
#include <filesystem>
#include <boost/algorithm/string.hpp>
#include <fmt/core.h>
#include <argparse/argparse.hpp>

typedef std::pair<std::string, std::vector<FieldType>> meta_t;

constexpr std::size_t block_size{50000};

class SQLCreateTableVisitor : public SQLBaseVisitor {
    antlrcpp::Any visitCreate_table(SQLParser::Create_tableContext *ctx) override {
        auto fields{ctx->field_list()->field()};
        std::vector<FieldType> field_types;
        for (auto field_ctx: fields) {
            auto nf{dynamic_cast<SQLParser::Normal_fieldContext *>(field_ctx)};
            if (nf) {
                auto field_type_caster{magic_enum::enum_cast<FieldType>(nf->type_()->children[0]->getText())};
                assert(field_type_caster.has_value());
                field_types.push_back(field_type_caster.value());
            }
        }
        return std::make_pair(ctx->Identifier()->getText(), field_types);
    }

    antlrcpp::Any visitProgram(SQLParser::ProgramContext *ctx) override {
        std::vector<meta_t> results;
        for (auto &child: ctx->children) {
            auto stmt_ctx{dynamic_cast<SQLParser::StatementContext *>(child)};
            if (stmt_ctx && !stmt_ctx->Annotation() && !stmt_ctx->Null()) {
                try {
                    results.emplace_back(child->accept(this).as<meta_t>());
                } catch (std::bad_cast &) {
                    continue;
                }
            }
        }
        return results;
    }

    antlrcpp::Any visitStatement(SQLParser::StatementContext *ctx) override {
        return ctx->children[0]->accept(this);
    }
};

std::size_t convert_csv(
        const std::string &table_name,
        const std::vector<FieldType> &schema,
        const std::string &csv_src,
        const std::string &sql_dst
) {
    std::ifstream src{csv_src};
    std::ofstream dst{sql_dst, std::ios::app};
    std::vector<std::string> cols;
    std::vector<std::string> values;
    std::size_t counter{0};
    auto dump{[&]() -> void {
        if (values.empty()) {
            return;
        }
        dst << "INSERT INTO " << table_name << " VALUES " << boost::join(values, ", ") << ";" << std::endl;
        values.clear();
    }};
    for (std::string line; std::getline(src, line);) {
        boost::split(cols, line, boost::is_any_of(","));
        assert(cols.size() == schema.size());
        for (std::size_t i{0}; i < schema.size(); ++i) {
            switch (schema[i]) {
                case FieldType::DATE:
                case FieldType::CHAR:
                case FieldType::VARCHAR:
                    cols[i] = '\'' + cols[i] + '\'';
                    break;
                default:
                    break;
            }
        }
        values.emplace_back('(' + boost::join(cols, ", ") + ')');
        cols.clear();
        ++counter;
        if (counter % block_size == 0) {
            dump();
        }
    }
    dump();
    return counter;
}

int main(int argc, char **argv) {
    argparse::ArgumentParser aparser{"csv_convertor"};
    aparser.add_argument("-s", "--src");
    aparser.parse_args(argc, argv);

    std::filesystem::path src_file_path{aparser.get<std::string>("--src")};
    auto base_path{src_file_path.parent_path()};

    std::ifstream src_file{src_file_path};
    std::string buffer;
    for (std::string line; std::getline(src_file, line);) {
        buffer += line;
    }
    boost::algorithm::replace_all(buffer, "\r", "\n");

    auto dst_file{base_path / "init.sql"};
    std::ofstream dst_file_stream{dst_file};
    dst_file_stream << buffer << std::endl;
    dst_file_stream.flush();

    SQLCreateTableVisitor visitor;
    antlr4::ANTLRInputStream input{buffer};
    SQLLexer lexer{&input};
    antlr4::CommonTokenStream tokens(&lexer);
    tokens.fill();
    SQLParser parser{&tokens};
    antlr4::tree::ParseTree *tree{parser.program()};
    if (parser.getNumberOfSyntaxErrors() > 0) {
        throw OperationError{"Syntax Error"};
    }
    auto results{tree->accept(&visitor).as<std::vector<meta_t>>()};
    for (const auto &[name, fields]: results) {
        std::string lower_name{name};
        fmt::print("Processing {} with {} cols ...\n", name, fields.size());
        boost::algorithm::to_lower(lower_name);
        auto count{convert_csv(
                name,
                fields,
                base_path / (lower_name + ".csv"),
                dst_file
        )};
        fmt::print(" > {} records\n", count);
    }
}