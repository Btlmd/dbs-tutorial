//
// Created by c7w on 2022/12/18.
//

#ifndef DBS_TUTORIAL_INDEXFIELD_H
#define DBS_TUTORIAL_INDEXFIELD_H

#include "defines.h"
#include <exception/OperationException.h>
#include <record/Field.h>

enum IndexFieldType {
    INVALID,
    INT,
    INT2  // std::pair<INT, INT>, joint indexing
};

class IndexField {
   public:
    IndexFieldType type;

    explicit IndexField(IndexFieldType type) : type{type} {}

    IndexField() = default;
    virtual ~IndexField() = default;

    virtual std::partial_ordering operator<=>(const IndexField &rhs) const = 0;
    virtual bool operator==(const IndexField &rhs) const = 0;

    [[nodiscard]] virtual RecordSize ShortSize() const = 0;
    [[nodiscard]] virtual RecordSize Size() const = 0;

    virtual std::string ToString() const = 0;

    // For leaf nodes
    static std::shared_ptr<IndexField> LoadIndexField(IndexFieldType type, const uint8_t *&src);
    virtual void Write(uint8_t *&dst) const = 0;

    // For internal nodes
    static std::shared_ptr<IndexField> LoadIndexFieldShort(IndexFieldType type, const uint8_t *&src);
    virtual void WriteShort(uint8_t *&dst) const = 0;

    static std::shared_ptr<IndexField> MakeNull(IndexFieldType type);

    static std::shared_ptr<IndexField> FromDataField(IndexFieldType type, const std::vector<std::shared_ptr<Field>> &fields);
};


class IndexINT : public IndexField {
   public:
    int value;
    bool is_null;
    explicit IndexINT(int value, bool is_null) : IndexField{IndexFieldType::INT},
                                                 value{value}, is_null(is_null) {}

    // Make NULL values appear to be the smallest value
    std::partial_ordering operator<=>(const IndexField &rhs) const override {
        auto int_rhs{dynamic_cast<const IndexINT &>(rhs)};
        if (is_null && int_rhs.is_null) {
            return std::partial_ordering::equivalent;
        } else if (is_null) {
            return std::partial_ordering::less;
        } else if (int_rhs.is_null) {
            return std::partial_ordering::greater;
        } else {
            return value <=> int_rhs.value;
        }
    }

    bool operator==(const IndexField &rhs) const override {
        auto int_rhs{dynamic_cast<const IndexINT &>(rhs)};
        if (is_null && int_rhs.is_null) {
            return true;
        } else if (is_null || int_rhs.is_null) {
            return false;
        } else {
            return value == int_rhs.value;
        }
    }

    static std::shared_ptr<IndexINT> FromSrc(const uint8_t *&src) {
        int value; bool is_null;
        read_var(src, value);
        read_var(src, is_null);
        return std::make_shared<IndexINT>(value, is_null);
    }

    static std::shared_ptr<IndexINT> FromSrcShort(const uint8_t *&src) {
        int value; bool is_null;
        read_var(src, value);
        read_var(src, is_null);
        return std::make_shared<IndexINT>(value, is_null);
    }

    virtual std::string ToString() const override{
        if (is_null) { return "NULL"; }
        return fmt::format("{}", value);
    }

    void Write(uint8_t *&dst) const override {
        write_var(dst, value);
        write_var(dst, is_null);
    }

    void WriteShort(uint8_t *&dst) const override {
        write_var(dst, value);
        write_var(dst, is_null);
    }

    [[nodiscard]] virtual RecordSize ShortSize() const override {return sizeof(value) + sizeof(is_null);}
    [[nodiscard]] virtual RecordSize Size() const override {return sizeof(value) + sizeof(is_null);}

    static std::shared_ptr<IndexField> FromDataField(const std::vector<std::shared_ptr<Field>> &fields) {
        if (fields.size() != 1) {
            throw OperationError("IndexINT::FromDataField: fields.size() != 1");
        }
        auto pointer = std::dynamic_pointer_cast<Int>(fields[0]);
        if (pointer == nullptr) {
            throw OperationError("IndexINT::FromDataField: fields[0] is not Int");
        }

        return std::make_shared<IndexINT>(pointer->value, pointer->is_null);
    }

};

class IndexINT2 : public IndexField {
   public:
    int value1, value2;
    bool is_null1, is_null2;
    explicit IndexINT2(int value1, bool is_null1, int value2, bool is_null2) :
                                            IndexField{IndexFieldType::INT2}, value1{value1},
                                            value2{value2}, is_null1(is_null1), is_null2(is_null2) {}

    explicit IndexINT2(int value1, bool is_null1, bool from_src=false) :
                                            IndexField{IndexFieldType::INT2}, value1{value1},
                                            value2{0}, is_null1(is_null1), is_null2(true) {
        if (from_src) {
            is_null2 = false; value2 = std::numeric_limits<int>::max();
        }
    }

    // Make NULL values appear to be the smallest value
    std::partial_ordering operator<=>(const IndexField &rhs) const override {
        auto int_rhs{dynamic_cast<const IndexINT2 &>(rhs)};
        if ((is_null1 && int_rhs.is_null1) || (!is_null1 && !int_rhs.is_null1 && value1 == int_rhs.value1)) {
            if (is_null2 && int_rhs.is_null2) {
                return std::partial_ordering::equivalent;
            } else if (is_null2) {
                return std::partial_ordering::less;
            } else if (int_rhs.is_null2) {
                return std::partial_ordering::greater;
            } else {
                return value2 <=> int_rhs.value2;
            }
        } else if (is_null1) {
            return std::partial_ordering::less;
        } else if (int_rhs.is_null1) {
            return std::partial_ordering::greater;
        } else {
            return value1 <=> int_rhs.value1;
        }
    }

    bool operator==(const IndexField &rhs) const override {
        auto int_rhs{dynamic_cast<const IndexINT2 &>(rhs)};
        if ((is_null1 && int_rhs.is_null1) || (!is_null1 && !int_rhs.is_null1 && value1 == int_rhs.value1)) {
            if (is_null2 && int_rhs.is_null2) {
                return true;
            } else if (is_null2 || int_rhs.is_null2) {
                return false;
            } else {
                return value2 == int_rhs.value2;
            }
        } else if (is_null1 || int_rhs.is_null1) {
            return false;
        } else {
            return value1 == int_rhs.value1;
        }
    }

    virtual std::string ToString() const override{
        std::string ret = "";
        if (is_null1) { ret = ret + "NULL"; }
        else { ret += fmt::format("{}", value1); }
        ret += " ";
        if (is_null2) { ret = ret + "NULL"; }
        else { ret += fmt::format("{}", value2); }
        return ret;
    }

    static std::shared_ptr<IndexINT2> FromSrc(const uint8_t *&src) {
        int value1, value2;
        bool is_null1, is_null2;
        read_var(src, value1);
        read_var(src, is_null1);
        read_var(src, value2);
        read_var(src, is_null2);
        return std::make_shared<IndexINT2>(value1, is_null1, value2, is_null2);
    }

    static std::shared_ptr<IndexINT2> FromSrcShort(const uint8_t *&src) {
        int value1; bool is_null1;
        read_var(src, value1);
        read_var(src, is_null1);
        return std::make_shared<IndexINT2>(value1, is_null1, true);
    }

    void Write(uint8_t *&dst) const override {
        write_var(dst, value1);
        write_var(dst, is_null1);
        write_var(dst, value2);
        write_var(dst, is_null2);
    }

    void WriteShort(uint8_t *&dst) const override {
        write_var(dst, value1);
        write_var(dst, is_null1);
    }

    [[nodiscard]] virtual RecordSize ShortSize() const override {return sizeof(value1) + sizeof(is_null1);}
    [[nodiscard]] virtual RecordSize Size() const override {return (sizeof(value1) + sizeof(is_null1)) * 2;}

    static std::shared_ptr<IndexField> FromDataField(const std::vector<std::shared_ptr<Field>> &fields) {
        if (fields.size() != 2) {
            throw std::runtime_error("IndexINT2::FromDataField: fields.size() != 2");
        }

        auto pointer1 = std::dynamic_pointer_cast<Int>(fields[0]);
        auto pointer2 = std::dynamic_pointer_cast<Int>(fields[1]);

        if (pointer1 == nullptr || pointer2 == nullptr) {
            throw std::runtime_error("IndexINT2::FromDataField: fields[0] or fields[1] is not Int");
        }
        return std::make_shared<IndexINT2>(pointer1->value, pointer1->is_null, pointer2->value, pointer2->is_null);
    }
};

#endif  // DBS_TUTORIAL_INDEXFIELD_H
