#ifndef STRING_TERM_ID_POOL_HPP
#define STRING_TERM_ID_POOL_HPP

#include <stdexcept>
#include <string>

#include <boost/bimap.hpp>
#include <boost/bimap/unordered_set_of.hpp>

#include "term.hpp"

namespace imhotep {

    class StringTermIdPool {    // !@# not threadsafe!!!
        typedef boost::bimap <
            boost::bimaps::unordered_set_of<StringTermId>,
            boost::bimaps::unordered_set_of<std::string>> Pool;

        Pool         _pool;
        StringTermId _latest = 0; // !@# should be atomic
    public:
        StringTermId intern(const std::string& value) {
            Pool::right_iterator it(_pool.right.find(value));
            if (it == _pool.right.end()) {
                ++_latest;
                it = _pool.right.insert(std::make_pair(value, _latest)).first;
            }
            return it->second;
        }

        const std::string& operator()(StringTermId id) const {
            Pool::left_const_iterator it(_pool.left.find(id));
            if (it == _pool.left.end()) {
                throw std::runtime_error("missing id"); // !@# enhance message
            }
            return it->second;
        }

        const size_t size() const { return _pool.size(); }
    };
}

#endif

