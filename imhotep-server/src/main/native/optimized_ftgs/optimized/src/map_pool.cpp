/*
 * mapcache.cpp
 *
 *  Created on: Jan 20, 2016
 *      Author: darren
 */

#include "map_pool.hpp"

namespace imhotep
{

    MapPool::MapPool(JNIEnv* env, jobject obj)
    : Binder(env, "com/indeed/flamdex/simple/MapCache$Pool")
    , _getAddr_handle(method_id_for("getMappedAddressAndLen", "(Ljava/lang/String;)[J"))
    , _obj(obj)
    { }

    const AddrAndLen MapPool::get_mapping(const std::string& path) const {
        const jstring path_jstring(_env->NewStringUTF(path.data()));

        jobject java_arr(_env->CallObjectMethod(_obj, _getAddr_handle, path_jstring));
        if (_env->ExceptionCheck()) {
            _env->ExceptionDescribe();

            std::ostringstream os;
            os << std::endl;
            os << __FUNCTION__ << ": getMappedAddress() failed for"
               << " obj: " << _obj
               << " path: " << path;
            throw imhotep_error(os.str());
        }
        if (java_arr == NULL) {
            std::ostringstream os;
            os << __FUNCTION__ << ": getMappedAddress() failed for"
               << " obj: " << _obj
               << " path: " << path;
            throw imhotep_error(os.str());
        }
        jlongArray arr = reinterpret_cast<jlongArray>(java_arr);
        long data[2];
        long *ptr = reinterpret_cast<long *>(&data);
        _env->GetLongArrayRegion(arr, 0, 2, ptr);

        AddrAndLen results(data[0], data[1]);
        return results;
    }

} /* namespace imhotep */
