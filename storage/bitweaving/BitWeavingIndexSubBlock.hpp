/**
 *   Copyright 2013-2016, Quickstep Research Group, Computer Sciences Department,
 *     University of Wisconsin—Madison.
 **/

#ifndef QUICKSTEP_STORAGE_BITWEAVING_INDEX_SUB_BLOCK_HPP_
#define QUICKSTEP_STORAGE_BITWEAVING_INDEX_SUB_BLOCK_HPP_

#include <cstddef>
#include <cstdint>
#include <memory>

#include "catalog/CatalogRelationSchema.hpp"
#include "compression/CompressionDictionary.hpp"
#include "storage/IndexSubBlock.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/TupleIdSequence.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "types/operations/comparisons/ComparisonID.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

class ComparisonPredicate;
class CompressionDictionaryBuilder;

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief An IndexSubBlock which implements a base class for BitWeaving methods.
 *        It automatically selects most efficient coding for the compressed
 *        column (dictionary coding with external or internal dictionary, or
 *        truncation).
 **/
class BitWeavingIndexSubBlock : public IndexSubBlock {
 public:
  bool supportsAdHocAdd() const override {
    return false;
  }

  bool supportsAdHocRemove() const override {
    return false;
  }

  bool addEntry(const tuple_id tuple) override {
    LOG(FATAL) << "Called BitWeavingIndexSubBlock::addEntry(), "
               << "which is not supported.";
  }

  bool bulkAddEntries(const TupleIdSequence &tuples) override {
    LOG(FATAL) << "Called BitWeavingIndexSubBlock::bulkAddEntries(), "
               << "which is not supported.";
  }

  void removeEntry(const tuple_id tuple) override {
    LOG(FATAL) << "Called BitWeavingIndexSubBlock::removeEntry(), "
               << "which is not supported.";
  }

  void bulkRemoveEntries(const TupleIdSequence &tuples) override {
    LOG(FATAL) << "Called BitWeavingIndexSubBlock::bulkRemoveEntries(), "
               << "which is not supported.";
  }

  /**
   * @note Currently this version only supports simple comparisons of a literal
   *       value with an indexed attribute.
   **/
  TupleIdSequence* getMatchesForPredicate(const ComparisonPredicate &predicate,
                                          const TupleIdSequence *filter) const override;

  bool rebuild() override;

  /**
   * @brief Return the dictionary used in this BitWeavingIndexSubBlock.
   * @note The returned dictionary could be in this IndexSubBlock or its
   *       corresponding TupleStorageSubBlock.
   *
   * @return The dictionary used in this BitWeavingIndexSubBlock.
   **/
  const CompressionDictionary* getCompressionDictionary() const {
    return dictionary_;
  }

 protected:
  // Protected constructor.
  BitWeavingIndexSubBlock(const TupleStorageSubBlock &tuple_store,
                          const IndexSubBlockDescription &description,
                          const bool new_block,
                          void *sub_block_memory,
                          const std::size_t sub_block_memory_size,
                          const std::size_t maximum_code_length);

  // Protected destructor.
  virtual ~BitWeavingIndexSubBlock() {
  }

  // Compute the number of bytes needed for this SubBlock's header. The header
  // consists of the code length and the size of the internal dictionary.
  std::size_t getHeaderSize() const {
    std::size_t header_size_bytes = sizeof(std::size_t) << 1;
    return header_size_bytes + dictionary_size_bytes_;
  }

  const std::size_t maximum_code_length_;
  bool initialized_;

  attribute_id key_id_;
  const Type *key_type_;

  std::size_t code_length_;
  std::size_t dictionary_size_bytes_;
  const CompressionDictionary *dictionary_;
  std::unique_ptr<CompressionDictionary> internal_dictionary_;

 private:
  // Get the bytes need to store a BitWeaving index.
  virtual std::size_t getBytesNeeded(std::size_t code_length,
                                     tuple_id num_tuples) const = 0;

  // Build the internal data structure of BitWeaving methods. If '*builder' is
  // non-NULL, it is the CompressionDictionaryBuilder used to create
  // '*internal_dictionary_', and will be used for faster hash-based lookup of
  // codes for values.
  virtual bool buildBitWeavingInternal(void *internal_memory,
                                       std::size_t internal_memory_size,
                                       const CompressionDictionaryBuilder *builder) = 0;

  // Find the tuples matching a simple comparison predicate.
  virtual TupleIdSequence* getMatchesForComparison(const std::uint32_t comparison_literal_code,
                                                   const ComparisonID comp,
                                                   const TupleIdSequence *filter) const = 0;

  // Initialize this block's metadata and structure. Usually called by
  // the constructor.
  bool initializeCommon(const bool new_block);

  // Clear the index.
  void clearIndex();

  // Build the header of the index.
  void buildHeader();

  // Build an internal dictionary or a truncation compression scheme.
  // Overwrites '*builder' with a pointer to the CompressionDictionaryBuilder
  // used to build an internal dictionary, if any (caller is responsible for
  // deleting it).
  bool buildInternalDictionary(CompressionDictionaryBuilder **builder);

  DISALLOW_COPY_AND_ASSIGN(BitWeavingIndexSubBlock);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_BITWEAVING_INDEX_SUB_BLOCK_HPP_
