#ifndef SHARED_DATA_TYPE_HPP
#define SHARED_DATA_TYPE_HPP

#include "storage/block.hpp"
#include "storage/io_fwd.hpp"

#include "util/exception.hpp"
#include "util/exception_utils.hpp"
#include "util/log.hpp"

#include <boost/assert.hpp>

#include <array>
#include <cstdint>
#include <map>
#include <numeric>
#include <unordered_set>

namespace osrm
{
namespace storage
{

class DataLayout;
namespace serialization
{
inline void read(io::BufferReader &reader, DataLayout &layout);

inline void write(io::BufferWriter &writer, const DataLayout &layout);
}

namespace detail
{
// Removes the file name if name_prefix is a directory and name is not a file in that directory
inline std::string trimName(const std::string &name_prefix, const std::string &name)
{
    // list directory and
    if (name_prefix.back() == '/')
    {
        auto directory_position = name.find_first_of("/", name_prefix.length());
        // this is a "file" in the directory of name_prefix
        if (directory_position == std::string::npos)
        {
            return name;
        }
        else
        {
            return name.substr(0, directory_position);
        }
    }
    else
    {
        return name;
    }
}
}

// Added at the start and end of each block as sanity check
const constexpr char CANARY[4] = {'O', 'S', 'R', 'M'};

class DataLayout
{
  public:
    DataLayout() : blocks{} {}

    inline void SetBlock(const std::string &name, Block block) { blocks[name] = std::move(block); }

    inline uint64_t GetBlockEntries(const std::string &name) const
    {
        return GetBlock(name).num_entries;
    }

    inline uint64_t GetBlockSize(const std::string &name) const { return GetBlock(name).byte_size; }

    inline bool HasBlock(const std::string &name) const
    {
        return blocks.find(name) != blocks.end();
    }

    inline uint64_t GetSizeOfLayout() const
    {
        uint64_t result = 0;
        for (const auto &name_and_block : blocks)
        {
            result += 2 * sizeof(CANARY) + GetBlockSize(name_and_block.first) + BLOCK_ALIGNMENT;
        }
        return result;
    }

    template <typename T, bool WRITE_CANARY = false>
    inline T *GetBlockPtr(char *shared_memory, const std::string &name) const
    {
        static_assert(BLOCK_ALIGNMENT % std::alignment_of<T>::value == 0,
                      "Datatype does not fit alignment constraints.");

        char *ptr = (char *)GetAlignedBlockPtr(shared_memory, name);
        if (WRITE_CANARY)
        {
            char *start_canary_ptr = ptr - sizeof(CANARY);
            char *end_canary_ptr = ptr + GetBlockSize(name);
            std::copy(CANARY, CANARY + sizeof(CANARY), start_canary_ptr);
            std::copy(CANARY, CANARY + sizeof(CANARY), end_canary_ptr);
        }
        else
        {
            char *start_canary_ptr = ptr - sizeof(CANARY);
            char *end_canary_ptr = ptr + GetBlockSize(name);
            bool start_canary_alive = std::equal(CANARY, CANARY + sizeof(CANARY), start_canary_ptr);
            bool end_canary_alive = std::equal(CANARY, CANARY + sizeof(CANARY), end_canary_ptr);
            if (!start_canary_alive)
            {
                throw util::exception("Start canary of block corrupted. (" + name + ")" +
                                      SOURCE_REF);
            }
            if (!end_canary_alive)
            {
                throw util::exception("End canary of block corrupted. (" + name + ")" + SOURCE_REF);
            }
        }

        return (T *)ptr;
    }

    // Depending on the name prefix this function either lists all blocks with the same prefix
    // or all entries in the sub-directory.
    // '/ch/edge' -> '/ch/edge_filter/0/blocks', '/ch/edge_filter/1/blocks'
    // '/ch/edge_filters/' -> '/ch/edge_filter/0', '/ch/edge_filter/1'
    template <typename OutIter> void List(const std::string &name_prefix, OutIter out) const
    {
        std::unordered_set<std::string> returned_name;

        for (const auto &pair : blocks)
        {
            // check if string begins with the name prefix
            if (pair.first.find(name_prefix) == 0)
            {
                auto trimmed_name = detail::trimName(name_prefix, pair.first);
                auto ret = returned_name.insert(trimmed_name);
                if (ret.second)
                {
                    *out++ = trimmed_name;
                }
            }
        }
    }

  private:
    friend void serialization::read(io::BufferReader &reader, DataLayout &layout);
    friend void serialization::write(io::BufferWriter &writer, const DataLayout &layout);

    const Block &GetBlock(const std::string &name) const
    {
        auto iter = blocks.find(name);
        if (iter == blocks.end())
        {
            throw util::exception("Could not find block " + name);
        }

        return iter->second;
    }

    // Fit aligned storage in buffer to 64 bytes to conform with AVX 512 types
    inline void *align(void *&ptr) const noexcept
    {
        const auto intptr = reinterpret_cast<uintptr_t>(ptr);
        const auto aligned = (intptr - 1u + BLOCK_ALIGNMENT) & -BLOCK_ALIGNMENT;
        return ptr = reinterpret_cast<void *>(aligned);
    }

    inline void *GetAlignedBlockPtr(void *ptr, const std::string &name) const
    {
        auto block_iter = blocks.find(name);
        if (block_iter == blocks.end())
        {
            throw util::exception("Could not find block " + name);
        }

        for (auto iter = blocks.begin(); iter != block_iter; ++iter)
        {
            ptr = static_cast<char *>(ptr) + sizeof(CANARY);
            ptr = align(ptr);
            ptr = static_cast<char *>(ptr) + iter->second.byte_size;
            ptr = static_cast<char *>(ptr) + sizeof(CANARY);
        }

        ptr = static_cast<char *>(ptr) + sizeof(CANARY);
        ptr = align(ptr);
        return ptr;
    }

    static constexpr std::size_t BLOCK_ALIGNMENT = 64;
    std::map<std::string, Block> blocks;
};

struct SharedRegion
{
    static constexpr std::size_t MAX_NAME_LENGTH = 254;

    SharedRegion() : name{0}, timestamp{0} {}
    SharedRegion(const std::string &name, std::uint64_t timestamp) : name{0}, timestamp{timestamp}
    {
        std::copy_n(name.begin(), std::min(MAX_NAME_LENGTH, name.size()), name);
    }

    bool IsEmpty() { return timestamp == 0; }

    char name[255];
    std::uint64_t timestamp;
    std::uint8_t shm_key;
};

// Keeps a list of all shared regions in a fixed-sized struct
// for fast access and deserialization.
struct SharedRegionRegister
{
    using RegionID = std::uint8_t;
    static constexpr const RegionID INVALID_REGION_ID = std::numeric_limits<RegionID>::max();
    using ShmKey = decltype(SharedRegion::shm_key);

    // Returns the key of the region with the given name
    RegionID Find(const std::string &name) const
    {
        auto iter = std::find_if(regions.begin(), regions.end(), [&](const auto &region) {
            if (std::strncmp(region.name, name.c_str(), SharedRegion::MAX_NAME_LENGTH) == 0)
            {
                return region;
            }
        });

        if (iter == regions.end())
        {
            return INVALID_REGION_ID;
        }
        else
        {
            return std::distance(regions.begin(), iter);
        }
    }

    RegionID Register(const std::string &name)
    {
        auto iter = std::find_if(regions.begin(), regions.end(), [&](const auto& region) {
                    return region.IsEmpty();
                });
        if (iter == regions.end())
        {
            throw util::exception("No shared memory regions left. Could not register " + name + ".");
        }
        else
        {
            constexpr std::uint32_t INITIAL_TIMESTAMP = 1;
            *iter = SharedRegion{name, INITIAL_TIMESTAMP};
            RegionID key = std::distance(regions.begin(), iter);
            return key;
        }
    }

    void Deregister(const RegionID key)
    {
        regions[key] = SharedRegion{};
    }

    ShmKey ReserveKey()
    {
        auto free_key_iter = std::find(shm_key_in_use.begin(), shm_key_in_use.end(), false);
        if (free_key_iter == shm_key_in_use.end())
        {
            throw util::exception("Could not reserve a new SHM key. All keys are in use");
        }

        *free_key_iter = true;
        return std::distance(shm_key_in_use.begin(), free_key_iter);
    }

    void ReleaseKey(ShmKey key)
    {
        shm_key_in_use[key] = false;
    }

    static constexpr const std::uint8_t MAX_SHARED_REGIONS = std::numeric_limits<RegionID>::max() - 1;
    static_assert(MAX_SHARED_REGIONS < std::numeric_limits<RegionID>::max(), "Number of shared memory regions needs to be less than the region id size.");
    std::array<SharedRegion, MAX_SHARED_REGIONS> regions;

    static constexpr const std::uint8_t MAX_SHM_KEYS = std::numeric_limits<std::uint8_t>::max() - 1;
    std::array<bool, MAX_SHM_KEYS> shm_key_in_use;

    static constexpr const char *name = "osrm-region";
};
}
}

#endif /* SHARED_DATA_TYPE_HPP */
