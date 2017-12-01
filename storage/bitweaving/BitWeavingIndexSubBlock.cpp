/**
 *   Copyright 2013-2016, Quickstep Research Group, Computer Sciences Department,
 *     University of Wisconsin—Madison.
 **/

#include "storage/bitweaving/BitWeavingIndexSubBlock.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogRelationSchema.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "compression/CompressionDictionary.hpp"
#include "compression/CompressionDictionaryBuilder.hpp"
#include "expressions/predicate/ComparisonPredicate.hpp"
#include "expressions/predicate/Predicate.hpp"
#include "expressions/scalar/Scalar.hpp"
#include "expressions/scalar/ScalarAttribute.hpp"
#include "storage/CompressedStoreUtil.hpp"
#include "storage/CompressedTupleStorageSubBlock.hpp"
#include "storage/IndexSubBlock.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/StorageErrors.hpp"
#include "storage/TupleIdSequence.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "storage/ValueAccessor.hpp"
#include "storage/ValueAccessorUtil.hpp"
#include "types/Type.hpp"
#include "types/TypedValue.hpp"
#include "types/operations/comparisons/ComparisonID.hpp"
#include "utility/BitManipulation.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

BitWeavingIndexSubBlock::BitWeavingIndexSubBlock(const TupleStorageSubBlock &tuple_store,
                                                 const IndexSubBlockDescription &description,
                                                 const bool new_block,
                                                 void *sub_block_memory,
                                                 const std::size_t sub_block_memory_size,
                                                 const std::size_t maximum_code_length)
    : IndexSubBlock(tuple_store,
                    description,
                    new_block,
                    sub_block_memory,
                    sub_block_memory_size),
      maximum_code_length_(maximum_code_length),
      initialized_(false),
      code_length_(0),
      dictionary_size_bytes_(0),
      dictionary_(nullptr),
      internal_dictionary_(nullptr) {
  std::cout << "Get Here! " << std::endl;
  DCHECK_EQ(description_.indexed_attribute_ids_size(), 1);
  key_id_ = description_.indexed_attribute_ids(0);
  key_type_ = &(relation_.getAttributeById(key_id_)->getType());

  bool initialize_now = true;
  if (tuple_store_.isCompressed()) {
    const CompressedTupleStorageSubBlock &compressed_tuple_store
        = static_cast<const CompressedTupleStorageSubBlock&>(tuple_store_);
    if (!compressed_tuple_store.compressedBlockIsBuilt()) {
      if (compressed_tuple_store.compressedUnbuiltBlockAttributeMayBeCompressed(key_id_)) {
        initialize_now = false;
      }
    }
  }

  if (initialize_now) {
    if (new_block) {
      initializeCommon(new_block);
    } else {
      if (!initializeCommon(new_block)) {
        throw MalformedBlock();
      }
    }
  }
}

bool BitWeavingIndexSubBlock::initializeCommon(const bool new_block) {
  if (new_block) {
    buildHeader();
  }

  std::size_t header_size_bytes = sizeof(std::size_t) << 1;
  code_length_ = *static_cast<std::size_t*>(sub_block_memory_);
  dictionary_size_bytes_ =
      *reinterpret_cast<std::size_t*>(static_cast<char*>(sub_block_memory_) + sizeof(std::size_t));

  bool use_external_dictionary = false;
  if (tuple_store_.isCompressed()) {
    const CompressedTupleStorageSubBlock &compressed_tuple_store
        = static_cast<const CompressedTupleStorageSubBlock&>(tuple_store_);
    if (!compressed_tuple_store.compressedBlockIsBuilt()) {
      // BitWeavingIndexSubBlock::initializeCommon() called with a key which
      // may be compressed before the associated TupleStorageSubBlock was
      // built. Wait until the rebuild() call to initialize this BitWeavingIndexSubBlock.
      return false;
    }

    // We ignore the truncation compression here as CompressedTupleStorageSubBlock
    // does not provide a API to get the code length in bits.
    use_external_dictionary = compressed_tuple_store
        .compressedAttributeIsDictionaryCompressed(key_id_);
  }

  if (use_external_dictionary) {
    DCHECK(new_block || dictionary_size_bytes_ == 0);

    // Reuse the compression dictionary in CompressedTupleStorageSubBlock.
    DCHECK(tuple_store_.isCompressed());
    const CompressedTupleStorageSubBlock &compressed_tuple_store
        = static_cast<const CompressedTupleStorageSubBlock&>(tuple_store_);
    DCHECK(compressed_tuple_store.compressedBlockIsBuilt());

    dictionary_ = &(compressed_tuple_store.compressedGetDictionary(key_id_));
    if (new_block) {
      code_length_ = dictionary_->codeLengthBits();
      *static_cast<std::size_t*>(sub_block_memory_) = code_length_;
    }
    DCHECK(code_length_ == dictionary_->codeLengthBits());
  } else {
    // Use the private compression dictionary.
    if (dictionary_size_bytes_ > 0) {
      DCHECK(!new_block);
      void *dictionary_memory = static_cast<char*>(sub_block_memory_) + header_size_bytes;
      internal_dictionary_.reset(new CompressionDictionary(*key_type_,
                                                           dictionary_memory,
                                                           dictionary_size_bytes_));
      dictionary_ = internal_dictionary_.get();
      DCHECK(code_length_ == dictionary_->codeLengthBits());
    }
  }

  initialized_ = true;

  return true;
}

void BitWeavingIndexSubBlock::clearIndex() {
  code_length_ = 0;
  dictionary_size_bytes_ = 0;
  dictionary_ = nullptr;
  internal_dictionary_.reset(nullptr);

  buildHeader();
}

void BitWeavingIndexSubBlock::buildHeader() {
  std::size_t header_size_bytes = sizeof(std::size_t) << 1;
  if (header_size_bytes >= sub_block_memory_size_) {
    throw BlockMemoryTooSmall("BitWeavingIndex", sub_block_memory_size_);
  }

  *static_cast<std::size_t*>(sub_block_memory_) = code_length_;
  *reinterpret_cast<std::size_t*>(static_cast<char*>(sub_block_memory_) + sizeof(std::size_t))
      = dictionary_size_bytes_;
}

bool BitWeavingIndexSubBlock::buildInternalDictionary(CompressionDictionaryBuilder **builder) {
  DCHECK(dictionary_ == internal_dictionary_.get());
  // We have processed empty tuple_store_ in rebuild().
  DCHECK(!tuple_store_.isEmpty());
  std::unique_ptr<CompressionDictionaryBuilder> dictionary_builder(
      new CompressionDictionaryBuilder(*key_type_));
  TypedValue maximum_integer;

  bool may_be_truncated = (key_type_->getTypeID() == kInt) || (key_type_->getTypeID() == kLong);
  bool first_non_null_value = true;

  //const std::int64_t maximum_supported_code = (1LL << maximum_code_length_) - 1;
  const std::int64_t maximum_supported_code = (1LL << 32) - 1;
  std::cout << "Test then: " <<  maximum_code_length_ << std::endl;

  std::unique_ptr<ValueAccessor> accessor(tuple_store_.createValueAccessor());
  InvokeOnAnyValueAccessor(
      accessor.get(),
      [&](auto *accessor) -> void {  // NOLINT(build/c++11)
    while (accessor->next()) {
      TypedValue value = accessor->getTypedValue(key_id_);

      dictionary_builder->insertEntryByReference(value);

      if (value.isNull()) {
        DCHECK(key_type_->isNullable());
        may_be_truncated = false;

        continue;
      }

      if (first_non_null_value) {
        // Initialize the maximum_integer.
        maximum_integer = value;
        first_non_null_value = false;
      }

      if (may_be_truncated) {
        switch (key_type_->getTypeID()) {
          case kInt:
            // Use dictionary compression if the integer is negative or is
            // greater than maximum_supported_code.
            // TODO(yinan): Support negative values with the truncation compression.
            // Add a base value for the truncation compression.
            if (value.getLiteral<int>() < 0 || value.getLiteral<int>() > maximum_supported_code) {
              may_be_truncated = false;
            } else if (maximum_integer.getLiteral<int>() < value.getLiteral<int>()) {
              maximum_integer = value;
            }
            break;
          case kLong:
            // Use dictionary compression if the integer is negative or is
            // greater than maximum_supported_code.
            if (value.getLiteral<std::int64_t>() < 0
                || value.getLiteral<std::int64_t>() > maximum_supported_code) {
              may_be_truncated = false;
            } else if (maximum_integer.getLiteral<std::int64_t>() < value.getLiteral<std::int64_t>()) {
              maximum_integer = value;
            }
            break;
          default:
            LOG(FATAL) << "Non-integer type encountered in computing maximum_integer.";
        }
      }
    }
  });  // NOLINT(whitespace/parens)

  code_length_ = dictionary_builder->codeLengthBits();
  dictionary_size_bytes_ = dictionary_builder->dictionarySizeBytes();

  // Calculate the total number of bytes (including storage for the
  // dictionary itself) needed to store all values with dictionary
  // compression.
  const std::size_t num_tuple_positions = tuple_store_.getMaxTupleID() + 1;
  std::size_t dictionary_block_bytes = dictionary_builder->dictionarySizeBytes()
                            + getBytesNeeded(code_length_, num_tuple_positions);

  if (may_be_truncated) {
    // Check if we should truncate integers.
    std::int64_t maximum_value;
    switch (key_type_->getTypeID()) {
      case kInt:
        maximum_value = static_cast<std::int64_t>(maximum_integer.getLiteral<int>());
        break;
      case kLong:
        maximum_value = maximum_integer.getLiteral<std::int64_t>();
        break;
      default:
        LOG(FATAL) << "Non-integer type encountered in computing maximum_integer "
                   << "of a BitWeavingIndexSubBlock.";
    }

    // Compute the code length for truncated integers.
    unsigned int leading_zero_bits = leading_zero_count<std::uint64_t>(maximum_value);
    unsigned int truncated_code_length = 64 - leading_zero_bits;

    // Calculate the number of bytes needed to store all values when truncating.
    std::size_t truncated_block_bytes = getBytesNeeded(truncated_code_length,
                                                       num_tuple_positions);

    // Choose the compression method based on both space and time costs.
    // We choose to use the truncation compression if 1) truncation compression
    // uses less space; 2) the code length of truncation compression does not exceed
    // the maximum code length allowed in this BitWeaving method, and 3) the code
    // length of truncation compression is not longer than twice the code length of
    // the dictionary compression.
    if (truncated_block_bytes <= dictionary_block_bytes
        && truncated_code_length <= maximum_code_length_
        && truncated_code_length <= code_length_ * 2) {
      code_length_ = truncated_code_length;
      dictionary_size_bytes_ = 0;
    }
  }

  // Compute the number of bytes needed for this SubBlock's header. The header
  // consists of the code length, the size of the dictionary.
  std::size_t header_size_bytes = sizeof(std::size_t) << 1;
  *static_cast<std::size_t*>(sub_block_memory_) = code_length_;
  *reinterpret_cast<std::size_t*>(static_cast<char*>(sub_block_memory_) + sizeof(std::size_t))
      = dictionary_size_bytes_;

  if (header_size_bytes + dictionary_size_bytes_ >= sub_block_memory_size_) {
    return false;
  }

  if (dictionary_size_bytes_ > 0) {
    void *dictionary_memory = static_cast<char*>(sub_block_memory_) + header_size_bytes;
    dictionary_builder->buildDictionary(dictionary_memory);
    dictionary_ = new CompressionDictionary(*key_type_, dictionary_memory, dictionary_size_bytes_);

    DCHECK_EQ(dictionary_->codeLengthBits(), dictionary_builder->codeLengthBits());

    *builder = dictionary_builder.release();
  }

  return true;
}

TupleIdSequence* BitWeavingIndexSubBlock::getMatchesForPredicate(const ComparisonPredicate &predicate,
                                                                 const TupleIdSequence *filter) const {
  if (code_length_ == 0) {
    return new TupleIdSequence(tuple_store_.getMaxTupleID() + 1);
  }

  if (!predicate.isAttributeLiteralComparisonPredicate()) {
    // TODO(yinan): Evaluate a comparison between two attributes. The two attributes must
    // use the exactly same dictionary. Thus, we need to use a global dictionary for all
    // BitWeavingIndexSubBlocks in a table.
    LOG(FATAL) << "Can not evaluate predicates other than simple comparisons.";
  }

  const CatalogAttribute *comparison_attribute = NULL;
  if (predicate.getLeftOperand().hasStaticValue()) {
    DCHECK_EQ(predicate.getRightOperand().getDataSource(), Scalar::kAttribute);
    comparison_attribute
        = &(static_cast<const ScalarAttribute&>(predicate.getRightOperand()).getAttribute());
  } else {
    DCHECK_EQ(predicate.getLeftOperand().getDataSource(), Scalar::kAttribute);
    comparison_attribute
        = &(static_cast<const ScalarAttribute&>(predicate.getLeftOperand()).getAttribute());
  }

  if (comparison_attribute->getID() != key_id_) {
    LOG(FATAL) << "Can not evaluate predicates on non-indexed attributes.";
  }

  std::int64_t maximum_truncated_value = static_cast<std::int64_t>((1ULL << code_length_) - 1);
  PredicateTransformResult result
      = CompressedAttributePredicateTransformer::TransformPredicateOnCompressedAttribute(
          relation_,
          predicate,
          dictionary_,
          maximum_truncated_value);

  const TupleIdSequence *exist_filter = nullptr;
  if (!tuple_store_.isPacked()) {
    TupleIdSequence *exist_filter_tmp = tuple_store_.getExistenceMap();
    if (filter) {
      exist_filter_tmp->intersectWith(*filter);
    }
    exist_filter = exist_filter_tmp;
  } else if (filter) {
    exist_filter = filter;
  }

  std::pair<std::uint32_t, std::uint32_t> match_range;
  TupleIdSequence *sequence;
  switch (result.type) {
    case PredicateTransformResultType::kAll:
      // All non-null tuples.
      if (dictionary_ && dictionary_->containsNull()) {
        std::uint32_t null_code = dictionary_->getNullCode();
        sequence = getMatchesForComparison(null_code,
                                           ComparisonID::kNotEqual,
                                           exist_filter);
      } else {
        sequence = new TupleIdSequence(tuple_store_.getMaxTupleID() + 1);
        if (exist_filter) {
          sequence->assignFrom(*exist_filter);
        } else {
          sequence->setRange(0, sequence->length(), true);
        }
      }
      break;
    case PredicateTransformResultType::kNone:
      // No matches.
      sequence = new TupleIdSequence(tuple_store_.getMaxTupleID() + 1);
      break;
    case PredicateTransformResultType::kBasicComparison:
      switch (result.comp) {
        case ComparisonID::kEqual:
        case ComparisonID::kLess:
        case ComparisonID::kGreaterOrEqual:
          sequence = getMatchesForComparison(result.first_literal,
                                             result.comp,
                                             exist_filter);
          break;
        case ComparisonID::kNotEqual:
          if (result.exclude_nulls) {
            DCHECK(dictionary_->containsNull());
            sequence = getMatchesForComparison(result.first_literal,
                                               result.comp,
                                               exist_filter);
            sequence = getMatchesForComparison(result.second_literal,
                                               ComparisonID::kNotEqual,
                                               sequence);
          } else {
            sequence = getMatchesForComparison(result.first_literal,
                                               result.comp,
                                               exist_filter);
          }
          break;
        default:
          LOG(FATAL) << "Unexpected ComparisonID";
      }
      break;
    case PredicateTransformResultType::kRangeComparison:
      sequence = getMatchesForComparison(result.first_literal,
                                         ComparisonID::kGreaterOrEqual,
                                         exist_filter);
      if (dictionary_ && dictionary_->containsNull()) {
        std::uint32_t null_code = dictionary_->getNullCode();
        if (result.second_literal == null_code) {
          // If the second literal is null, we can simply perform
          // a scan with a kNotEqual comparator for the second literal,
          // which is often faster than a kLess comparator.
          // TODO(yinan): We can maintain a null bitmap to further
          // speedup this step.
          sequence = getMatchesForComparison(result.second_literal,
                                             ComparisonID::kNotEqual,
                                             sequence);
        }
      } else {
        sequence = getMatchesForComparison(result.second_literal,
                                           ComparisonID::kLess,
                                           sequence);
      }
      break;
    default:
      LOG(FATAL) << "Unexpected PredicateTransformResultType.";
  }
  return sequence;
}

bool BitWeavingIndexSubBlock::rebuild() {
  if (!initialized_) {
    if (!initializeCommon(!initialized_)) {
      return false;
    }
  }

  if (tuple_store_.isEmpty()) {
    clearIndex();
    return true;
  }

  std::unique_ptr<CompressionDictionaryBuilder> builder;
  if (dictionary_ == internal_dictionary_.get()) {
    CompressionDictionaryBuilder *builder_scratch = nullptr;
    if (!buildInternalDictionary(&builder_scratch)) {
      return false;
    }
    builder.reset(builder_scratch);
  }

  std::size_t header_size = getHeaderSize();
  return buildBitWeavingInternal(static_cast<char*>(sub_block_memory_) + header_size,
                                 sub_block_memory_size_ - header_size,
                                 builder.get());
}

}  // namespace quickstep
