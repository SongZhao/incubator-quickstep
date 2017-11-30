/**
 *   Copyright 2013-2016, Quickstep Research Group, Computer Sciences Department,
 *     University of Wisconsin—Madison.
 **/

#ifndef QUICKSTEP_STORAGE_BITWEAVING_V_INDEX_SUB_BLOCK_HPP_
#define QUICKSTEP_STORAGE_BITWEAVING_V_INDEX_SUB_BLOCK_HPP_

#include <cstddef>
#include <cstdint>

#include "expressions/predicate/PredicateCost.hpp"
#include "storage/bitweaving/BitWeavingIndexSubBlock.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/StorageConstants.hpp"
#include "storage/SubBlockTypeRegistryMacros.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "types/operations/comparisons/ComparisonID.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

template <typename T> class BitWeavingIndexSubBlockTest;
class ComparisonPredicate;
class CompressionDictionaryBuilder;

QUICKSTEP_DECLARE_SUB_BLOCK_TYPE_REGISTERED(BitWeavingVIndexSubBlock);

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief An IndexSubBlock which implements a class for BitWeaving/V method.
 * @warning The compressed key length must be smaller than or equal to 32 bits.
 **/
class BitWeavingVIndexSubBlock : public BitWeavingIndexSubBlock {
 public:
  BitWeavingVIndexSubBlock(const TupleStorageSubBlock &tuple_store,
                           const IndexSubBlockDescription &description,
                           const bool new_block,
                           void *sub_block_memory,
                           const std::size_t sub_block_memory_size);

  ~BitWeavingVIndexSubBlock() override {
  }

  /**
   * @brief Determine whether an IndexSubBlockDescription is valid for this
   *        type of IndexSubBlock.
   *
   * @param relation The relation an index described by description would
   *        belong to.
   * @param description A description of the parameters for this type of
   *        IndexSubBlock, which will be checked for validity.
   * @return Whether description is well-formed and valid for this type of
   *         IndexSubBlock belonging to relation (i.e. whether an IndexSubBlock
   *         of this type, belonging to relation, can be constructed according
   *         to description).
   **/
  static bool DescriptionIsValid(const CatalogRelationSchema &relation,
                                 const IndexSubBlockDescription &description);

  /**
   * @brief Estimate the average number of bytes (including any applicable
   *        overhead) used to index a single tuple in this type of
   *        IndexSubBlock. Used by StorageBlockLayout::finalize() to divide
   *        block memory amongst sub-blocks.
   * @warning description must be valid. DescriptionIsValid() should be called
   *          first if necessary.
   *
   * @param relation The relation tuples belong to.
   * @param description A description of the parameters for this type of
   *        IndexSubBlock.
   * @return The average/ammortized number of bytes used to index a single
   *         tuple of relation in an IndexSubBlock of this type described by
   *         description.
   **/
  static std::size_t EstimateBytesPerTuple(const CatalogRelationSchema &relation,
                                           const IndexSubBlockDescription &description);

  /**
   * @brief Estimate the total number of bytes (including any applicable
   *        overhead) occupied by this IndexSubBlock within the StorageBlock.
   *        This function is to be used by those indicies whose occupied size
   *        in the block does not depend on the number of tuples being indexed.
   * @warning description must be valid. DescriptionIsValid() should be called
   *          first if necessary.
   * @note This function will be invoked by StorageBlockLayout::finalize()
   *       if and only when EstimateBytesPerTuple() returns a zero size.
   *
   * @param relation The relation tuples belong to.
   * @param description A description of the parameters for this type of
   *        IndexSubBlock.
   * @return The total number of bytes occupied by this IndexSubBlock within
   *         the StorageBlock.
   **/
  static std::size_t EstimateBytesPerBlock(const CatalogRelationSchema &relation,
                                           const IndexSubBlockDescription &description) {
    return kZeroSize;
  }

  IndexSubBlockType getIndexSubBlockType() const override {
    return kBitWeavingV;
  }

  predicate_cost_t estimatePredicateEvaluationCost(
      const ComparisonPredicate &predicate) const override;

 private:
  using WordUnit = std::size_t;

  // If the value of kNumBitsPerGroup is changed, you also need to update
  // the implementaion of getMatchesForComparisonInstantiation.
  static constexpr std::size_t kNumBitsPerGroup = 4;
  static constexpr std::size_t kMaxNumGroups = (32 + kNumBitsPerGroup - 1) / kNumBitsPerGroup;
  static constexpr std::size_t kNumCodesPerSegment = sizeof(WordUnit) << 3;
  static constexpr std::size_t kNumWordsPerSegment = kNumBitsPerGroup;

  // Get the bytes need to store a BitWeaving/V index.
  std::size_t getBytesNeeded(std::size_t code_length, tuple_id num_tuples) const override;

  // Build the internal data structure of the BitWeaving/V method.
  bool buildBitWeavingInternal(void *internal_memory,
                               std::size_t internal_memory_size,
                               const CompressionDictionaryBuilder *builder) override;

  // Find the tuples matching a simple comparison predicate.
  TupleIdSequence* getMatchesForComparison(const std::uint32_t literal_code,
                                           const ComparisonID comp,
                                           const TupleIdSequence *filter) const override;

  // Initialize this block's metadata and structure. Usually called by the constructor.
  bool initialize(bool new_block, void *internal_memory, std::size_t internal_memory_size);

  // Set the code of a tuple specified by tid.
  // Note: the code position must be zeroed-out before the call.
  void setCode(const tuple_id tid, const std::uint32_t code);

  // Get the code of a tuple specified by tid.
  // Note: this method is only used by unittest.
  std::uint32_t getCode(const tuple_id tid) const;

  template <std::size_t CODE_LENGTH>
  TupleIdSequence* getMatchesForComparisonHelper(const std::uint32_t literal_code,
                                                 const ComparisonID comp,
                                                 const TupleIdSequence *filter) const;

  template <std::size_t CODE_LENGTH, ComparisonID COMP>
  TupleIdSequence* getMatchesForComparisonInstantiation(const std::uint32_t literal_code,
                                                        const TupleIdSequence *filter) const;

  template<std::size_t CODE_LENGTH, ComparisonID COMP, std::size_t GROUP_ID>
  inline void scanGroup(const std::size_t segment_offset,
                        WordUnit *mask_equal,
                        WordUnit *mask_less,
                        WordUnit *mask_greater,
                        WordUnit **literal_bit_ptr) const;

  std::size_t num_groups_;
  std::size_t num_filled_groups_;
  std::size_t num_bits_in_unfilled_group_;
  std::size_t num_segments_;
  std::size_t num_words_;
  WordUnit *group_words_[kMaxNumGroups];

  template <typename T> friend class BitWeavingIndexSubBlockTest;

  DISALLOW_COPY_AND_ASSIGN(BitWeavingVIndexSubBlock);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_BITWEAVING_V_INDEX_SUB_BLOCK_HPP_
