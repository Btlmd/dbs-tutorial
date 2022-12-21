//
// Created by lambda on 22-12-21.
//

#ifndef DBS_TUTORIAL_OFFSETLIMITNODE_H
#define DBS_TUTORIAL_OFFSETLIMITNODE_H

#include <node/OpNode.h>

#include <memory>
#include <optional>

class OffsetLimitNode : public OpNode {
public:
    OffsetLimitNode(std::shared_ptr<OpNode> downstream, std::size_t limit, std::size_t offset) :
            OpNode{{downstream}}, lower{offset}, upper{offset + limit}, accu{0} {}

    bool Over() const override {
        return (accu >= upper) || children[0]->Over();
    }

    RecordList Next() override {
        assert(accu < upper);
        auto downstream{children[0]->Next()};
        auto prev_accu{accu};
        accu += downstream.size();

        auto intersection{Interval<std::size_t>::Intersect({prev_accu, accu}, {lower, upper})};
        if (!intersection.has_value()) {
            return {};
        }
        auto[left, right]{intersection.value()};
        left -= prev_accu;
        right -= prev_accu;

        assert(left <= right);
        assert(left >= 0);
        assert(right <= downstream.size());

//        return downstream;
        RecordList ret;
        std::copy(downstream.begin() + left, downstream.begin() + right, std::back_inserter(ret));
        return ret;
    }

    void Reset() override {
        OpNode::Reset();
        accu = 0;
    }

private:
    const std::size_t lower;
    const std::size_t upper;
    std::size_t accu;

    template<typename T>
    class Interval {
    public:
        T x;
        T y;

        Interval(T x, T y): x{x}, y{y} {
            assert(x <= y);
        }

        static std::optional<Interval> Intersect(Interval a, Interval b) {
            T left{std::max(a.x, b.x)}, right{std::min(a.y, b.y)};
            if (left >= right) {
                return std::nullopt;
            }
            return {{left, right}};
        }
    };
};

#endif //DBS_TUTORIAL_OFFSETLIMITNODE_H
