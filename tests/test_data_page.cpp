#include <gtest/gtest.h>
#include <memory>

#define private public
#include <utils/System.h>
#include <system/DBVisitor.h>
#include <system/DBSystem.h>
#include <io/Page.h>
#include <record/Field.h>
#include <record/DataPage.h>
#undef private

class DataPageTest: public testing::Test {
protected:
    std::shared_ptr<DBSystem> dbms;
    std::shared_ptr<DBVisitor> visitor;
    void SetUp() override {
        init_logger();
        FileSystem::MakeDirectory(DB_DIR);
        std::filesystem::remove_all(DB_DIR);
        dbms = std::make_shared<DBSystem>();
        visitor = std::make_shared<DBVisitor>(*dbms);

        std::string buffer{"CREATE DATABASE DB;"
                           "USE DB;"
                           "CREATE TABLE T(A INT, B VARCHAR(10), C FLOAT, D CHAR(15));"
        };
        process_input(buffer, *visitor);
    }
};

TEST_F(DataPageTest, basic_op) {
    auto table_meta{dbms->GetTableMeta(0)};
    Page page;
    DataPage dp{&page, *table_meta};
    dp.Init();
    RecordList records = {
            std::make_shared<Record>(std::vector{0_i, "a---"_v, 4_f, _ch("e----", 15)}),
            std::make_shared<Record>(std::vector{1_i, "b----"_v, 5_f, _ch("f----", 15)}),
            std::make_shared<Record>(std::vector{2_i, "c-----"_v, 6_f, _ch("g----", 15)}),
            std::make_shared<Record>(std::vector{3_i, "d------"_v, 7_f, _ch("h----", 15)})
    };

    for (const auto & rec: records) {
        dp.Insert(rec);
    }

    for (int i{0}; i < 4; ++i) {
        ASSERT_EQ(*dp.Select(i), *records[i]);
    }

    dp.Delete(2);
    ASSERT_LT(*dp.FooterSlot(2), 0);
    for(int i: {0, 1, 3}) {
        ASSERT_EQ(*dp.Select(i), *records[i]);
    }

    dp.Contiguous();
    ASSERT_LT(*dp.FooterSlot(2), 0);
    for(int i: {0, 1, 3}) {
        ASSERT_EQ(*dp.Select(i), *records[i]);
    }
    ASSERT_EQ(*dp.FooterSlot(0) + dp.Select(0)->Size(), *dp.FooterSlot(1));
    ASSERT_EQ(*dp.FooterSlot(1) + dp.Select(1)->Size(), *dp.FooterSlot(3));
    ASSERT_EQ(*dp.FooterSlot(3) + dp.Select(3)->Size(), *dp.FooterSlot(4));

    dp.Insert(records[2]);
    ASSERT_LT(*dp.FooterSlot(2), 0);
    ASSERT_GE(*dp.FooterSlot(4), 0);
    for(int i: {0, 1, 3}) {
        ASSERT_EQ(*dp.Select(i), *records[i]);
    }
    ASSERT_EQ(*dp.Select(4), *records[2]);

    auto new_rec = std::make_shared<Record>(std::vector{5_i, "++++"_v, 10_f, _ch("----", 15)});

    dp.Update(1, new_rec);
    ASSERT_EQ(*dp.Select(0), *records[0]);
    ASSERT_EQ(*dp.Select(1), *new_rec);
    ASSERT_EQ(*dp.Select(3), *records[3]);
    ASSERT_EQ(*dp.Select(4), *records[2]);
}
