#ifndef SPLIT_FILE_CACHE_HPP
#define SPLIT_FILE_CACHE_HPP

#include "split_file.hpp"

#include <memory>
#include <mutex>
#include <unordered_map>

namespace imhotep {

    /** Cache of SplitFiles organized by split filename. Since the cache exists
        largely to keep split files open during an FTGS operation, nothing ever
        gets evicted from it.
    */
    class SplitFileCache {
    public:
        std::shared_ptr<SplitFile> get(const std::string& filename) {
            std::lock_guard<std::mutex> lock_map(_map_mutex);
            std::shared_ptr<SplitFile>  result;

            Map::iterator it(_map.find(filename));
            if (it == _map.end()) {
                result = std::make_shared<SplitFile>(filename);
                _map[filename] = result;
            }
            else {
                result = it->second;
            }
            return result;
        }

    private:
        typedef std::unordered_map<std::string, std::shared_ptr<SplitFile>> Map;

        std::mutex _map_mutex;
        Map        _map;
    };

}

#endif
