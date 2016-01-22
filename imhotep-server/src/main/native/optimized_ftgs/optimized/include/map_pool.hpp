#pragma once
/*
 * map_cache.hpp
 *
 *  Created on: Jan 20, 2016
 *      Author: darren
 */

#include <jni.h>

#include "binder.hpp"

namespace imhotep
{

    struct AddrAndLen {
        public:
            AddrAndLen(long addr_long, long len_long) :
                    addr(reinterpret_cast<void*>(addr_long)),
                    len(static_cast<size_t>(len_long))
            { }

            void* const addr;
            const size_t len;
    };

    class MapPool : public Binder {
        public:
            MapPool() : Binder(NULL, NULL), _getAddr_handle(NULL), _obj(NULL) { }

            MapPool(JNIEnv* env, jobject obj);

            const AddrAndLen get_mapping(const std::string& path) const;

        private:
            jmethodID  _getAddr_handle;
            jobject    _obj;
    };

} /* namespace imhotep */
