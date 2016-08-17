#ifndef SHARD_METADATA_HPP
#define SHARD_METADATA_HPP

#include "shard.hpp"

#include <yaml-cpp/yaml.h>

#include <vector>

namespace imhotep {

class ShardMetadata {
    const imhotep::Shard& _shard;
    const YAML::Node      _metadata;
public:
    ShardMetadata(const imhotep::Shard& shard)
        : _shard(shard)
        , _metadata(YAML::LoadFile(shard.dir() + "/metadata.txt"))
    { }

    const imhotep::Shard& shard() const { return _shard; }

    const std::vector<std::string> int_fields() const {
        return _metadata["intFields"].as<std::vector<std::string>>();
    }

    const std::vector<std::string> str_fields() const {
        return _metadata["stringFields"].as<std::vector<std::string>>();
    }

    size_t num_docs() const { return _metadata["numDocs"].as<size_t>(); }
};

} // namespace imhotep

#endif
