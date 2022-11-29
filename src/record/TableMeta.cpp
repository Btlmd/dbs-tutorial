//
// Created by lambda on 22-11-20.
//

#include "TableMeta.h"

#include <record/Field.h>
#include <utils/Serialization.h>

#include <utility>

TableMeta *TableMeta::FromSrc(FileID fd, BufferSystem &buffer) {
    return nullptr;
}

void TableMeta::Write() {
}

PageID TableMeta::FindFreeSpace(RecordSize size) {
    /**
     * TODO: [c7w] implement FreeSpaceMap
     * See https://thu-db.github.io/dbs-tutorial/chapter-2/variable.html
     * And https://github.com/postgres/postgres/blob/master/src/backend/storage/freespace/README
     */
    return 0;
}
