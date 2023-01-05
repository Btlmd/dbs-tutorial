//
// Created by lambda on 22-11-20.
//

#ifndef DBS_TUTORIAL_FIELD_H
#define DBS_TUTORIAL_FIELD_H

#include <cstdint>
#include <string>
#include <cstring>
#include <compare>

#include <boost/algorithm/string.hpp>

#include <defines.h>
#include <utils/Serialization.h>
#include <exception/OperationException.h>

enum class FieldType {
    INVALID = 0,
    INT = 1,
    FLOAT = 2,
    CHAR = 3,
    VARCHAR = 4,
    DATE = 5
};

class PrimaryKey {
public:
    char name[CONSTRAINT_NAME_LEN_MAX + 1];
    FieldID field_count;
    FieldID fields[MAX_FIELD_COUNT];

    [[nodiscard]] std::vector<FieldID> to_vector() const {
        std::vector<FieldID> field_vec;
        for (FieldID i{0}; i < field_count; ++i) {
            field_vec.push_back(fields[i]);
        }
        return std::move(field_vec);
    }
};

class ForeignKey {
public:
    char name[CONSTRAINT_NAME_LEN_MAX + 1];
    FieldID field_count;
    FieldID fields[MAX_FIELD_COUNT];
    TableID reference_table;
    FieldID reference_fields[MAX_FIELD_COUNT];

    [[nodiscard]] std::vector<FieldID> ToVector() const {
        std::vector<FieldID> vec;
        for (FieldID i{0}; i < field_count; ++i) {
            vec.push_back(fields[i]);
        }
        return std::move(vec);
    }

    [[nodiscard]] std::vector<FieldID> ReferenceToVector() const {
        std::vector<FieldID> vec;
        for (FieldID i{0}; i < field_count; ++i) {
            vec.push_back(reference_fields[i]);
        }
        return std::move(vec);
    }
};

class UniqueKey {
public:
//    char name[CONSTRAINT_NAME_LEN_MAX + 1];
    FieldID field_count;
    FieldID fields[MAX_FIELD_COUNT];

    [[nodiscard]] std::vector<FieldID> ToVector() const {
        std::vector<FieldID> vec;
        for (FieldID i{0}; i < field_count; ++i) {
            vec.push_back(fields[i]);
        }
        return std::move(vec);
    }
};

class IndexKey {
public:
    FieldID field_count{0};
    int reference_count{0};  // Constraint count
    bool user_created{true};  // Whether it can be dropped directly. If index added by user manually, then true.
    // Else index was added by other key constraints, false.
    // In face of user insertion:
    // If user_created == true, raise error (Already exists by user)
    // If user_created == false, mark user created as true

    // In face of user deletion:
    // If user_created == true, mark user_created as false
    // If user_created == false, raise error (Cannot drop, do not exist)

    // In face of constraint insertion:
    // Add reference_count by 1

    // In face of constraint deletion:
    // Remove 1 from reference_count

    // Delete key if and only if reference_count == 0 and user_created == false

    FieldID fields[MAX_FIELD_COUNT];

    bool operator==(const IndexKey &other) const {
        if (field_count != other.field_count) {
            return false;
        }
        for (int i = 0; i < field_count; ++i) {
            if (fields[i] != other.fields[i]) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] FieldID Find(FieldID field_id) const {
        for (int i = 0; i < field_count; ++i) {
            if (fields[i] == field_id) {
                return i;
            }
        }
        return -1;
    }

    std::pair<int, std::pair<std::shared_ptr<class IndexField>, std::shared_ptr<class IndexField>>>
    FilterCondition(const std::shared_ptr<class AndCondition> &and_cond);
};

class Field {
public:
    FieldType type;
    bool is_null;

    explicit Field(FieldType type, bool is_null = false) : type{type}, is_null{is_null} {}

    Field() = default;

    virtual ~Field() = default;

    /**
     * Return the size of the field in storage
     * @return
     */
    [[nodiscard]] virtual RecordSize Size() const = 0;

    /**
     * Dump the field into `dst` and move `dst` forward
     * Note that `NULL` is not dumped and is expected to be stored in null bitmap
     * @param dst
     */
    virtual void Write(uint8_t *&dst) const = 0;

    virtual std::size_t Hash() const = 0;

    [[nodiscard]] virtual std::string ToString() const = 0;

    virtual std::partial_ordering operator<=>(const Field &rhs) const = 0;

    virtual bool operator==(const Field &rhs) const = 0;

    /**
     * Generate a field from a specific source
     * @param type
     * @param src
     * @param max_len
     * @return
     */
    static std::shared_ptr<Field> LoadField(FieldType type, const uint8_t *&src, RecordSize max_len = -1);

    /**
     * Generate a NULL field for a specific type
     * @param type
     * @param max_len
     * @return
     */
    static std::shared_ptr<Field> MakeNull(FieldType type, RecordSize max_len = -1);
};

class Float : public Field {
public:
    float value;

    explicit Float(float value) : Field{FieldType::FLOAT}, value{value} {}

    static std::shared_ptr<Float> FromSrc(const uint8_t *&src) {
        float value;
        read_var(src, value);
        return std::make_shared<Float>(value);
    }

    std::partial_ordering operator<=>(const Field &rhs) const override {
        if (is_null || rhs.is_null) {
            return std::partial_ordering::unordered;
        }
        auto float_rhs{dynamic_cast<const Float &>(rhs)};
        return value <=> float_rhs.value;
    }

    bool operator==(const Field &rhs) const override {
        if (is_null || rhs.is_null) {
            return false;
        }
        auto float_rhs{dynamic_cast<const Float &>(rhs)};
        return value == float_rhs.value;
    }

    [[nodiscard]] RecordSize Size() const override {
        return sizeof(value);
    }

    [[nodiscard]] std::string ToString() const override {
        return fmt::format("{:.3f}", value);
    }

    void Write(uint8_t *&dst) const override {
        write_var(dst, value);
    }

    [[nodiscard]] std::size_t Hash() const override {
        return *(uint32_t *) (&value);
    }
};

class Int : public Field {
public:
    int value;

    explicit Int(int value) : Field{FieldType::INT}, value{value} {}

    static std::shared_ptr<Int> FromSrc(const uint8_t *&src) {
        int value;
        read_var(src, value);
        return std::make_shared<Int>(value);
    }

    std::partial_ordering operator<=>(const Field &rhs) const override {
        if (is_null || rhs.is_null) {
            return std::partial_ordering::unordered;
        }
        auto int_rhs{dynamic_cast<const Int &>(rhs)};
        return value <=> int_rhs.value;
    }

    bool operator==(const Field &rhs) const override {
        if (is_null || rhs.is_null) {
            return false;
        }
        auto int_rhs{dynamic_cast<const Int &>(rhs)};
        return value == int_rhs.value;
    }

    [[nodiscard]] RecordSize Size() const override {
        return sizeof(value);
    }

    [[nodiscard]] std::string ToString() const override {
        return std::to_string(value);
    }

    void Write(uint8_t *&dst) const override {
        write_var(dst, value);
    }

    [[nodiscard]] std::shared_ptr<Float> ToFloat() const {
        return std::make_shared<Float>(value);
    }

    [[nodiscard]] std::size_t Hash() const override {
        return *(uint32_t *) (&value);
    }
};

class Date : public Field {
public:
    int value;

    static int StringToDateInt(const std::string &date_format) {
        std::vector<std::string> result;
        boost::split(result, date_format, boost::is_any_of("-"));
        int value{0};
        if (result.size() == 3) {
            value += std::stoi(result[0]) * 1'0000;
            value += std::stoi(result[1]) * 100;
            value += std::stoi(result[2]);
            return value;
        } else {
            throw OperationError{"Invalid date string {}", date_format};
        }
    }

    static std::string DateIntToString(int value) {
        return fmt::format(
                "{:04d}-{:02d}-{:02d}",
                value % 1'0000'0000 / 1'0000,
                value % 1'0000 / 100,
                value % 100
        );
    }

    explicit Date(int value) : Field{FieldType::DATE}, value{value} {}

    explicit Date(const std::string &date_format) : Field{FieldType::DATE}, value{StringToDateInt(date_format)} {}

    static std::shared_ptr<Date> FromSrc(const uint8_t *&src) {
        int value;
        read_var(src, value);
        return std::make_shared<Date>(value);
    }

    std::partial_ordering operator<=>(const Field &rhs) const override {
        if (is_null || rhs.is_null) {
            return std::partial_ordering::unordered;
        }
        auto date_rhs{dynamic_cast<const Date &>(rhs)};
        return value <=> date_rhs.value;
    }

    bool operator==(const Field &rhs) const override {
        if (is_null || rhs.is_null) {
            return false;
        }
        auto date_rhs{dynamic_cast<const Date &>(rhs)};
        return value == date_rhs.value;
    }

    [[nodiscard]] RecordSize Size() const override {
        return sizeof(value);
    }

    [[nodiscard]] std::string ToString() const override {
        return DateIntToString(value);
    }

    void Write(uint8_t *&dst) const override {
        write_var(dst, value);
    }

    [[nodiscard]] std::size_t Hash() const override {
        return *(uint32_t *) (&value);
    }
};

class String : public Field {
private:
    static std::hash<std::string> hash;
public:
    std::string data;

    explicit String(std::string str_data) : data{std::move(str_data)}, Field{FieldType::INVALID} {}

    explicit String(std::string str_data, FieldType field_type) : data{std::move(str_data)}, Field{field_type} {}

    std::partial_ordering operator<=>(const Field &rhs) const override {
        if (is_null || rhs.is_null) {
            return std::partial_ordering::unordered;
        }
        auto str_rhs{dynamic_cast<const String *>(&rhs)};
        return data <=> str_rhs->data;
    }

    bool operator==(const Field &rhs) const override {
        if (is_null || rhs.is_null) {
            return false;
        }
        auto str_rhs{dynamic_cast<const String *>(&rhs)};
        return data == str_rhs->data;
    }

    [[nodiscard]] std::string ToString() const override {
        return '\'' + data + '\'';
    }

    [[nodiscard]] std::string Raw() const {
        return data;
    }

    ~String() = default;

    [[nodiscard]] std::size_t Hash() const override {
        return hash(data);
    }
};

class Char : public String {
public:
    RecordSize max_len;

    /**
     * Caller should ensure that the data is valid, i.e. data.size() <= max_size
     * @param str_data
     */
    Char(std::string str_data, RecordSize max_len) : String{std::move(str_data), FieldType::CHAR}, max_len{max_len} {}

    ~Char() = default;

    static std::shared_ptr<Char> FromSrc(const uint8_t *&src, RecordSize max_len) {
        std::string buffer;
        read_string_null_terminated(src, buffer);
        assert(buffer.size() <= max_len);
        src += max_len + 1;
        return std::make_shared<Char>(std::move(buffer), max_len);
    }

    [[nodiscard]] RecordSize Size() const override {
        return max_len + 1;
    }

    void Write(uint8_t *&dst) const override {
        write_string_null_terminated(dst, data);
        dst += max_len + 1;
    }
};

class VarChar : public String {
public:
    VarChar() = default;

    /**
     * Caller should ensure that the data is valid, i.e. data.size() <= max_size
     * @param str_data
     */
    explicit VarChar(std::string str_data) : String{std::move(str_data), FieldType::VARCHAR} {}

    ~VarChar() = default;

    static std::shared_ptr<VarChar> FromSrc(const uint8_t *&src) {
        std::string buffer;
        read_string(src, buffer);
        return std::make_shared<VarChar>(std::move(buffer));
    }

    [[nodiscard]] RecordSize Size() const override {
        return data.size() + sizeof(RecordSize);
    }

    void Write(uint8_t *&dst) const override {
        write_string(dst, data);
    }
};

class FieldMeta {
public:
    FieldMeta() = default;

    FieldMeta(
            FieldType type,
            std::string name,
            FieldID field_id,
            RecordSize max_size = -1,
            bool not_null = false,
            bool has_default = false,
            std::shared_ptr<Field> default_value = nullptr
    ) : type{type},
        name{std::move(name)},
        field_id{field_id},
        max_size{max_size},
        not_null{not_null},
        has_default{has_default},
        default_value{default_value} {}

    FieldType type;
    std::string name;
    FieldID field_id;

    RecordSize max_size{-1};
    bool not_null{false};
    bool has_default{false};
    std::shared_ptr<Field> default_value{nullptr};

    static std::shared_ptr<FieldMeta> FromSrc(const uint8_t *&src) {
        const uint8_t *start_point{src};  // be optimized ifndef DEBUG
        auto meta{std::make_shared<FieldMeta>()};
        read_var(src, meta->type);
        read_string(src, meta->name);
        read_var(src, meta->max_size);
        read_var(src, meta->field_id);
        read_var(src, meta->not_null);
        read_var(src, meta->has_default);
        if (meta->has_default) {
            meta->default_value = Field::LoadField(meta->type, src, meta->max_size);
        }
        assert(src - start_point == meta->Size());
        return meta;
    }

    void Write(uint8_t *&dst) const {
        const uint8_t *start_point{dst};
        write_var(dst, type);
        write_string(dst, name);
        write_var(dst, max_size);
        write_var(dst, field_id);
        write_var(dst, not_null);
        write_var(dst, has_default);
        if (has_default) {
            default_value->Write(dst);
        }
        assert(dst - start_point == Size());
    }

    [[nodiscard]] RecordSize Size() const {
        auto _size{sizeof(type) + sizeof(max_size) + sizeof(field_id) + sizeof(not_null) + sizeof(has_default)};
        _size += name.size() + sizeof(RecordSize);
        if (has_default) {
            assert(default_value != nullptr);
            _size += default_value->Size();
        }
        return static_cast<RecordSize>(_size);
    }

};

inline std::shared_ptr<Field> Field::LoadField(FieldType type, const uint8_t *&src, RecordSize max_len) {
    switch (type) {
        case FieldType::INT:
            return Int::FromSrc(src);
        case FieldType::FLOAT:
            return Float::FromSrc(src);
        case FieldType::CHAR:
            assert(max_len > 0);
            return Char::FromSrc(src, max_len);
        case FieldType::VARCHAR:
            return VarChar::FromSrc(src);
        case FieldType::DATE:
            return Date::FromSrc(src);
        default:
            assert(false);
    }
}

inline std::shared_ptr<Field> Field::MakeNull(FieldType type, RecordSize max_len) {
    switch (type) {
        case FieldType::INT: {
            auto int_p{std::make_shared<Int>(0)};
            int_p->is_null = true;
            return int_p;
        }
        case FieldType::FLOAT: {
            auto float_p{std::make_shared<Float>(0)};
            float_p->is_null = true;
            return float_p;
        }
        case FieldType::CHAR: {
            auto char_p{std::make_shared<Char>("", max_len)};
            char_p->is_null = true;
            return char_p;
        }
        case FieldType::VARCHAR: {
            auto varchar_p{std::make_shared<VarChar>("")};
            varchar_p->is_null = true;
            return varchar_p;
        }
        case FieldType::DATE: {
            auto varchar_p{std::make_shared<Date>(0)};
            varchar_p->is_null = true;
            return varchar_p;
        }
        default:
            assert(false);
    }
}

std::shared_ptr<Field> inline operator "" _i(unsigned long long el) {
    return std::make_shared<Int>(el);
}

std::shared_ptr<Field> inline operator "" _f(unsigned long long el) {
    return std::make_shared<Float>(el);
}

std::shared_ptr<Field> inline operator "" _f(long double el) {
    return std::make_shared<Float>(el);
}

std::shared_ptr<Field> inline operator "" _v(const char *el, std::size_t s) {
    return std::make_shared<VarChar>(std::string{el, s});
}

std::shared_ptr<Field> inline _ch(std::string data, RecordSize max_l) {
    return std::make_shared<Char>(data, max_l);
}

class FieldHash {
public:
    std::size_t operator()(const std::shared_ptr<Field> &f) const {
        return f->Hash();
    }
};

class FieldEqual {
public:
    bool operator()(const std::shared_ptr<Field> &lhs, const std::shared_ptr<Field> &rhs) const {
        return *lhs == *rhs;
    }
};

/**
 * Used for sorting field
 * Order `NULL` as the smallest element
 */
class FieldCompare {
public:
    /**
     * return true if l_f is consider less than r_f
     * @param l_f
     * @param r_f
     * @return
     */
    bool operator()(const std::shared_ptr<Field> &l_f, const std::shared_ptr<Field> &r_f) const {
        if (l_f->is_null && r_f->is_null) {
            return false;
        }
        if (l_f->is_null && !r_f->is_null) {
            return true;
        }
        if (!l_f->is_null && r_f->is_null) {
            return false;
        }
        return *l_f < *r_f;
    }
};

#endif //DBS_TUTORIAL_FIELD_H
