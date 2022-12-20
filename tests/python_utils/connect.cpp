
#include <boost/python.hpp>
#include <boost/python/dict.hpp>
#include <boost/python/list.hpp>
#include <boost/python/tuple.hpp>
#include <boost/python/extract.hpp>
#include <boost/python/str.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include <string>
#include <sstream>
#include <vector>

#include <utils/System.h>
#include <system/DBVisitor.h>
#include <system/DBSystem.h>

#include <memory>

typedef std::vector<std::string> StringList;

using namespace boost::python;

list query(std::string query) {
    FileSystem::MakeDirectory(DB_DIR);
    std::filesystem::remove_all(DB_DIR);

    auto dbms = DBSystem{};
    DBVisitor visitor{dbms};

    list multiple_resutls;

    auto result_list{ToResultList(query, visitor)};

    for (const auto &result: result_list) {
        dict ret;
        if (std::dynamic_pointer_cast<TextResult>(result)) {
            setitem(ret, "type", "text");
            setitem(ret, "resp", result->ToString());
        }

        auto table_result{std::dynamic_pointer_cast<TableResult>(result)};
        if (table_result) {
            if (table_result->record_list == nullptr) {
                list returns;
                setitem(ret, "type", "sys_table");
                for (const auto &record: table_result->records) {
                    list row;
                    for (const auto &field: record) {
                        row.append(field);
                    }
                    returns.append(row);
                }
                setitem(ret, "resp", returns);
            } else {
                list returns;
                setitem(ret, "type", "record_table");
                for (const auto &record: *(table_result->record_list)) {
                    list row;
                    for (const auto &field: record->fields) {
                        if (field->is_null) {
                            row.append(object());
                            continue;
                        }

                        auto int_p{std::dynamic_pointer_cast<Int>(field)};
                        if (int_p) {
                            row.append(int_p->value);
                            continue;
                        }
                        auto float_p{std::dynamic_pointer_cast<Float>(field)};
                        if (float_p) {
                            row.append(float_p->value);
                            continue;
                        }
                        auto date_p{std::dynamic_pointer_cast<Date>(field)};
                        if (date_p) {
                            row.append(date_p->ToString());
                            continue;
                        }
                        auto str_p{std::dynamic_pointer_cast<String>(field)};
                        if (str_p) {
                            row.append(str_p->data);
                            continue;
                        }
                        assert(false);
                    }
                    returns.append(row);
                }
                setitem(ret, "resp", returns);
            }
        }
        multiple_resutls.append(ret);
    }
    return multiple_resutls;
}

BOOST_PYTHON_MODULE (connect_db) {
    using namespace boost::python;
    def("query", query);
}

