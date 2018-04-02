#ifndef UNPACKING_CACHE_HPP
#define UNPACKING_CACHE_HPP

#include <boost/optional/optional_io.hpp>

#include "../../third_party/compute_detail/lru_cache.hpp"
#include "util/typedefs.hpp"

namespace osrm
{
namespace engine
{
class UnpackingCache
{
  private:
    boost::compute::detail::lru_cache<std::tuple<NodeID, NodeID, std::size_t>, EdgeDuration> cache;
    unsigned current_data_timestamp = 0;

  public:
    // TO FIGURE OUT HOW MANY LINES TO INITIALIZE CACHE TO:
    // Assume max cache size is 500mb (see bottom of OP here:
    // https://github.com/Project-OSRM/osrm-backend/issues/4798#issue-288608332)
    // Total cache size: 500 mb = 500 * 1024 *1024 bytes = 524288000 bytes
    // Assume std::size_t is 64 bits (my local machine this is the case):
    // Current cache line = NodeID * 2 + std::size_t * 1 + EdgeDuration * 1
    //                    = std::uint32_t * 2 + std::size_t * 1 + std::int32_t * 1
    //                    = 4 bytes * 3 + 8 bytes = 20 bytes
    // Number of cache lines is 500 mb = 500 * 1024 *1024 bytes = 524288000 bytes / 20 = 26214400
    // Actually it should be 26214400 divided by the number of threads available

    UnpackingCache(unsigned timestamp) : cache(26214400), current_data_timestamp(timestamp){};

    void Clear(unsigned new_data_timestamp)
    {
        if (current_data_timestamp != new_data_timestamp)
        {
            cache.clear();
            current_data_timestamp = new_data_timestamp;
        }
    }

    bool IsEdgeInCache(std::tuple<NodeID, NodeID, std::size_t> edge)
    {
        return cache.contains(edge);
    }

    void AddEdge(std::tuple<NodeID, NodeID, std::size_t> edge, EdgeDuration duration)
    {
        cache.insert(edge, duration);
    }

    EdgeDuration GetDuration(std::tuple<NodeID, NodeID, std::size_t> edge)
    {
        boost::optional<EdgeDuration> duration = cache.get(edge);
        return *duration ? *duration : MAXIMAL_EDGE_DURATION;
    }
};
} // engine
} // osrm

#endif // UNPACKING_CACHE_HPP
