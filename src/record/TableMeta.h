//
// Created by lambda on 22-11-20.
//

#ifndef DBS_TUTORIAL_TABLEMETA_H
#define DBS_TUTORIAL_TABLEMETA_H

#include <vector>

#include <boost/bimap.hpp>

#include <defines.h>
#include <record/Field.h>
#include <io/BufferSystem.h>

/**
 *
 */
class TableMeta {
public:

    PageID page_count;
//    std::vector<FieldMeta *> field_meta;
    PrimaryKey *primary_key{nullptr};
    std::vector<ForeignKey *> foreign_keys;

    boost::bimap<std::string, FieldMeta *> field_meta;

    void Write();

    static TableMeta *FromSrc(FileID fd, BufferSystem &buffer);

    TableMeta(PageID page_count, std::vector<FieldMeta *> fields, PrimaryKey *pk, std::vector<ForeignKey *> fk,
              FileID fd, BufferSystem &buffer)
            : fd{fd}, page_count{page_count}, primary_key{pk}, foreign_keys{std::move(fk)},
              buffer{buffer} {
        for (const auto fm: fields) {
            field_meta.insert({fm->name, fm});
        }
    }

    ~TableMeta() {
        delete primary_key;
        // not best practice, but I've already written so many factories that yields a pointer :(
        for (const auto &fk: foreign_keys) {
            delete fk;
        }
        for (const auto &fm: field_meta) {
            delete fm;
        }
    }

private:
    TableMeta(BufferSystem buffer) : buffer{buffer} {}

    BufferSystem &buffer;
    FileID fd;
};


#endif //DBS_TUTORIAL_TABLEMETA_H
