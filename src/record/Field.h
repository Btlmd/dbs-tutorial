//
// Created by lambda on 22-11-20.
//

#ifndef DBS_TUTORIAL_FIELD_H
#define DBS_TUTORIAL_FIELD_H

#include <cstdint>
#include <string>
#include <cstring>
#include <compare>

#include <defines.h>
#include <utils/Serialization.h>
#include <exception/OperationException.h>

enum class FieldType {
    INVALID = 0,
    INT = 1,
    FLOAT = 2,
    CHAR = 3,
    VARCHAR = 4,
};

class PrimaryKey {
public:
    char name[CONSTRAINT_NAME_LEN_MAX + 1];
    FieldID field_count;
    FieldID fields[MAX_FIELD_COUNT];
};

class ForeignKey {
public:
    char name[CONSTRAINT_NAME_LEN_MAX + 1];
    FieldID field_count;
    FieldID fields[MAX_FIELD_COUNT];
    TableID reference_table;
    FieldID reference_fields[MAX_FIELD_COUNT];
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

private:
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

    std::shared_ptr<Float> ToFloat() const {
        return std::make_shared<Float>(value);
    }
};

class String: public Field {
public:
    std::string data;

    explicit String(std::string str_data): data{std::move(str_data)}, Field{FieldType::INVALID} {}

    explicit String(std::string str_data, FieldType field_type): data{std::move(str_data)}, Field{field_type} {}

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
    explicit VarChar(std::string str_data): String{std::move(str_data), FieldType::VARCHAR} {}

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
    FieldType type;
    std::string name;
    FieldID field_id;

    RecordSize max_size{-1};
    bool unique{false};
    bool not_null{false};
    bool has_default{false};
    std::shared_ptr<Field> default_value{nullptr};

    [[nodiscard]] RecordSize Size() const {
        return sizeof(FieldMeta) - sizeof(std::string) + sizeof(std::size_t) + name.size();
    }

    static std::shared_ptr<FieldMeta> FromSrc(const uint8_t *&src) {
        const uint8_t *start_point{src};  // be optimized ifndef DEBUG
        auto meta{std::make_shared<FieldMeta>()};
        read_var(src, meta->type);
        read_string(src, meta->name);
        read_var(src, meta->max_size);
        read_var(src, meta->unique);
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
        write_var(dst, unique);
        write_var(dst, not_null);
        write_var(dst, has_default);
        if (has_default) {
            default_value->Write(dst);
        }
        assert(dst - start_point == Size());
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
        default:
            assert(false);
    }
}

inline std::shared_ptr<Field> Field::MakeNull(FieldType type, RecordSize max_len) {
    switch (type) {
        case FieldType::INT:{
            auto int_p{std::make_shared<Int>(0)};
            int_p->is_null = true;
            return int_p;
        }
        case FieldType::FLOAT:{
            auto float_p{std::make_shared<Float>(0)};
            float_p->is_null = true;
            return float_p;
        }
        case FieldType::CHAR:{
            auto char_p{std::make_shared<Char>("", max_len)};
            char_p->is_null = true;
            return char_p;
        }
        case FieldType::VARCHAR:{
            auto varchar_p{std::make_shared<VarChar>("")};
            varchar_p->is_null = true;
            return varchar_p;
        }
        default:
            assert(false);
    }
}

#endif //DBS_TUTORIAL_FIELD_H
