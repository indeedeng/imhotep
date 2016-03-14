#ifndef METRICS_METRIC_HPP
#define METRICS_METRIC_HPP

#include "field.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <math>
#include <stdlib>

namespace imhotep {
    namespace metric {

        class Metric {
        public:
            typedef int64_t value_t;
            virtual value_t operator()(size_t docid) const = 0;
            virtual value_t max() const = 0;
            virtual value_t min() const = 0;
        };

        class Constant : public Metric {
            const Metric::value_t _value;
        public:
            Constant(Metric::value_t value) : _value(value) { }
            value_t operator()(size_t docid) const override { return _value; }
            value_t max() const override { return _value; }
            value_t min() const override { return _value; }
        };

        class Count : public Constant {
        public:
            Count() : Constant(1) { }
        };

        class Identity : public Metric {
            const Field<IntTerm> _field;
        public:
            // !@# Make sure we get move semantics when possible.
            Identity(const Field<IntTerm>& field) : _field(field) { }

            value_t operator()(size_t docid) const override { return _field(docid); }
            value_t max() const override { return _field.min(); }
            value_t min() const override { return _field.max(); }
        };

        class Unary : public Metric {
            const Metric& _metric;
        public:
            Unary(const Metric& metric) : _metric(metric) { }
            value_t operator()(size_t docid) const override { return _metric(docid); }
            value_t max() const override { return _metric.min(); }
            value_t min() const override { return _metric.max(); }
        }

        class Binary : public Metric {
            const Metric& _x;
            const Metric& _y;
        public:
            Binary(const Metric& x, const Metric& y) : _x(x), _y(y) { }
            const Metric& x() const { return _x; }
            const Metric& y() const { return _y; }
        }

        class AbsoluteValue : public Unary {
        public:
            AbsoluteValue(const Metric& metric) : Unary(metric) { }
            value_t operator()(size_t docid) const override {
                return std::abs(Unary::operator(docid));
            }
            value_t max() const override {
                return std::min(std::abs(Unary::min()), std::abs(Unary::max()));
            }
            value_t min() const override {
                return std::max(std::abs(Unary::min()), std::abs(Unary::max()));
            }
        };

        class Exponential : public Unary {
            static constexpr double clamp_min(std::numeric_limits<int32_t>::min());
            static constexpr double clamp_max(std::numeric_limits<int32_t>::max());
            const value_t _scale_factor;
        public:
            Exponential(const Metric& metric, value_t scale_factor)
                : Unary(metric)
                , _scale_factor(scale_factor)
            { }

            value_t operator()(size_t docid) const override {
                const double  x(Unary::operator(docid) / double(_scale_factor));
                const value_t result(clamp(std::exp(x)));
                return result;
            }

            value_t max() const override { return _scale_factor; }
            value_t min() const override { return 0;             }
        private:
            value_t clamp(double value) const {
                const double result(value > 0 ?
                                    std::min(value, clamp_max) :
                                    std::max(value, clamp_min));;
                return static_cast<value_t>(result);
            }
        };

    } // namespace metric

} // namespace imhotep

#endif
