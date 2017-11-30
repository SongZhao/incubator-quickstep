/**
 *   Copyright 2013-2016, Quickstep Research Group, Computer Sciences Department,
 *     University of Wisconsin—Madison.
 **/

#ifndef QUICKSTEP_STORAGE_BITWEAVING_H_INDEX_SUB_BLOCK_HPP_
#define QUICKSTEP_STORAGE_BITWEAVING_H_INDEX_SUB_BLOCK_HPP_

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

QUICKSTEP_DECLARE_SUB_BLOCK_TYPE_REGISTERED(BitWeavingHIndexSubBlock);

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief An IndexSubBlock which implements a class for BitWeaving/H method.
 * @warning The compressed key length must be strictly smaller than 32 bits.
 **/
class BitWeavingHIndexSubBlock : public BitWeavingIndexSubBlock {
 public:
  BitWeavingHIndexSubBlock(const TupleStorageSubBlock &tuple_store,
                           const IndexSubBlockDescription &description,
                           const bool new_block,
                           void *sub_block_memory,
                           const std::size_t sub_block_memory_size);

  ~BitWeavingHIndexSubBlock() override {
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
    return kBitWeavingH;
  }

  predicate_cost_t estimatePredicateEvaluationCost(
      const ComparisonPredicate &predicate) const override;

 private:
  using WordUnit = std::size_t;

  // Get the bytes need to store a BitWeaving/H index.
  std::size_t getBytesNeeded(std::size_t code_length, tuple_id num_tuples) const override;

  // Build the internal data structure of the BitWeaving/H method.
  bool buildBitWeavingInternal(void *internal_memory,
                               std::size_t internal_memory_size,
                               const CompressionDictionaryBuilder *builder) override;

  // Find the tuples matching a simple comparison predicate.
  TupleIdSequence* getMatchesForComparison(const std::uint32_t literal_code,
                                           const ComparisonID comp,
                                           const TupleIdSequence *filter) const override;

  // Initialize this block's metadata and structure. Usually called by the constructor.
  bool initialize(bool new_block, void *internal_memory, std::size_t internal_memory_size);

  // Set the code at a word position with a bit offset.
  // Note: the code position must be zeroed-out before the call.
  inline void setCode(const std::size_t word_id,
                      const std::size_t bit_shift,
                      const std::uint32_t code) {
    DCHECK_LT(code, (1ULL << code_length_));
    DCHECK_LT(bit_shift, (sizeof(WordUnit) << 3));
    DCHECK_LT(word_id, num_words_);

    words_[word_id] |= static_cast<WordUnit>(code) << bit_shift;
  }

  // Get the code of a tuple specified by tid.
  // Note: this method is only used by unittest.
  inline std::uint32_t getCode(const tuple_id tid) const {
    DCHECK_LE(tid, tuple_store_.getMaxTupleID());
    std::size_t segment_id = tid / num_codes_per_segment_;
    std::size_t code_id_in_segment = tid % num_codes_per_segment_;
    std::size_t word_id = segment_id * num_words_per_segment_
        + code_id_in_segment % num_words_per_segment_;
    std::size_t code_id_in_word = code_id_in_segment / num_words_per_segment_;

    const WordUnit mask = (1ULL << code_length_) - 1;
    DCHECK_LT(word_id, num_words_);
    std::size_t shift = (num_codes_per_word_ - 1 - code_id_in_word) * num_bits_per_code_;
    return (words_[word_id] >> shift) & mask;
  }

  template <std::size_t CODE_LENGTH>
  TupleIdSequence* getMatchesForComparisonHelper(const std::uint32_t literal_code,
                                                 const ComparisonID comp,
                                                 const TupleIdSequence *filter) const;

  template <std::size_t CODE_LENGTH, ComparisonID COMP>
  TupleIdSequence* getMatchesForComparisonInstantiation(const std::uint32_t literal_code,
                                                        const TupleIdSequence *filter) const;

  std::size_t num_bits_per_code_;
  std::size_t num_codes_per_word_;
  std::size_t num_padding_bits_;
  std::size_t num_words_per_segment_;
  std::size_t num_codes_per_segment_;
  std::size_t num_segments_;
  std::size_t num_words_;
  WordUnit *words_;

  template <typename T>
  friend class BitWeavingIndexSubBlockTest;

  DISALLOW_COPY_AND_ASSIGN(BitWeavingHIndexSubBlock);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_BITWEAVING_H_INDEX_SUB_BLOCK_HPP_
