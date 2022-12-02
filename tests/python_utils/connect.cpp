
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

    list ret;

    auto result_list{ToResultList(query, visitor)};

    for (const auto &result: result_list) {
        if (std::dynamic_pointer_cast<TextResult>(result)) {
            ret.append(result->ToString());
            continue;
        }
        list returns;
        for (const auto &record: std::dynamic_pointer_cast<TableResult>(result)->records) {
            list row;
            for (const auto &field: record) {
                row.append(field);
            }
            returns.append(row);
        }
        ret.append(returns);
    }
    return ret;
}

BOOST_PYTHON_MODULE (connect_db) {
    using namespace boost::python;
    def("query", query);
}

