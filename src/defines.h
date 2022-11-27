//
// Created by lambda on 22-11-17.
//

#ifndef DBS_TUTORIAL_DEFINES_H
#define DBS_TUTORIAL_DEFINES_H

#include <cstdint>
#include <string>
#include <fcntl.h>
#include <filesystem>
#include <limits>


// Types

typedef decltype(open(std::declval<const char *>(), std::declval<int>())) FileID;
typedef int32_t PageID;
typedef int32_t DbID;
typedef int32_t TableID;
typedef int16_t FieldID;
typedef int16_t RecordSize;

// System Constants

constexpr int32_t BUFFER_SIZE{60000};
constexpr int32_t PAGE_SIZE{4096};

// Max Length

constexpr int32_t TABLE_NAME_LEN_MAX{64};
constexpr int32_t FIELD_NAME_LEN_MAX{64};
constexpr int32_t CONSTRAINT_NAME_LEN_MAX{64};
constexpr int32_t RECORD_LEN_MAX{2048};

constexpr int32_t MAX_FIELD_COUNT{128};
constexpr int32_t MAX_FK_COUNT{128};
constexpr int32_t MAX_TABLE_COUNT{std::numeric_limits<TableID>::max()};
constexpr int32_t MAX_DB_COUNT{std::numeric_limits<DbID>::max()};

// File Structure

const std::filesystem::path DB_DIR{"./databases"};
constexpr char TABLE_FILE[]{"tables.bin"};
constexpr char TABLE_META_PATTERN[]{"{}_meta.bin"};
constexpr char TABLE_DATA_PATTERN[]{"{}_data.bin"};
constexpr char TABLE_INDEX_PATTERN[]{"{}_index.bin"};

#define DEBUG 1

// Logging Settings

#include <boost/log/trivial.hpp>
constexpr boost::log::trivial::severity_level SEVERITY{boost::log::trivial::trace};
constexpr char LOGGING_PATTERN[]{"{:%Y_%m_%d_%H:%M:%S}.log"};
#define TraceLog BOOST_LOG_TRIVIAL(trace)
#define DebugLog BOOST_LOG_TRIVIAL(debug)
#define InfoLog BOOST_LOG_TRIVIAL(info)
#define WarningLog BOOST_LOG_TRIVIAL(warning)


#endif //DBS_TUTORIAL_DEFINES_H