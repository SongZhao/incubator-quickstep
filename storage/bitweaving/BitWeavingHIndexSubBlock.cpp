/**
 *   Copyright 2013-2016, Quickstep Research Group, Computer Sciences Department,
 *     University of Wisconsin—Madison.
 **/

#include "storage/bitweaving/BitWeavingHIndexSubBlock.hpp"

#include <cstddef>
#include <cstdint>
#include <utility>

#include "catalog/CatalogTypedefs.hpp"
#include "compression/CompressionDictionary.hpp"
#include "compression/CompressionDictionaryBuilder.hpp"
#include "expressions/predicate/ComparisonPredicate.hpp"
#include "expressions/predicate/PredicateCost.hpp"
#include "storage/bitweaving/BitWeavingIndexSubBlock.hpp"
#include "storage/CompressedTupleStorageSubBlock.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/StorageConstants.hpp"
#include "storage/SubBlockTypeRegistry.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "types/TypedValue.hpp"
#include "types/operations/comparisons/ComparisonID.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

QUICKSTEP_REGISTER_INDEX(BitWeavingHIndexSubBlock, BITWEAVING_H);

BitWeavingHIndexSubBlock::BitWeavingHIndexSubBlock(const TupleStorageSubBlock &tuple_store,
                                                   const IndexSubBlockDescription &description,
                                                   const bool new_block,
                                                   void *sub_block_memory,
                                                   const std::size_t sub_block_memory_size)
    : BitWeavingIndexSubBlock(tuple_store,
                              description,
                              new_block,
                              sub_block_memory,
                              sub_block_memory_size,
                              31) {
  std::size_t header_size = getHeaderSize();
  initialize(new_block,
             static_cast<char*>(sub_block_memory) + header_size,
             sub_block_memory_size - header_size);
}

bool BitWeavingHIndexSubBlock::DescriptionIsValid(const CatalogRelationSchema &relation,
                                                  const IndexSubBlockDescription &description) {
  // Make sure description is initialized and specifies BitWeaving.
  if (!description.IsInitialized()) {
    return false;
  }

  if (description.sub_block_type() != IndexSubBlockDescription::BITWEAVING_H) {
    return false;
  }

  // Make sure the relation contains a single indexed attribute.
  if (description.indexed_attribute_ids_size() != 1) {
    return false;
  }

  attribute_id indexed_attribute_id = description.indexed_attribute_ids(0);

  if (!relation.hasAttributeWithId(indexed_attribute_id)) {
    return false;
  }

  return true;
}

// TODO(yinan): Make this more accurate by excluding the size of dictionary
// if we use the external dictionary.
std::size_t BitWeavingHIndexSubBlock::EstimateBytesPerTuple(
    const CatalogRelationSchema &relation,
    const IndexSubBlockDescription &description) {
  DCHECK(DescriptionIsValid(relation, description));

  const Type &type =
      relation.getAttributeById(description.indexed_attribute_ids(0))->getType();

  if (!type.isVariableLength()) {
    // If the attribute type is fixed-length, use the type's length
    // plus 2 bytes (conservatively estimate that we have to store every
    // value independently plus 2 bytes for the code).
    return type.maximumByteLength() + 2;
  } else {
    // If the attribute type is variable-length, use the estimated average
    // length plus 4 bytes for its offset storage in the dictionary
    // plus 2 bytes for the code.
    return type.estimateAverageByteLength() + 4 + 2;
  }
}

predicate_cost_t BitWeavingHIndexSubBlock::estimatePredicateEvaluationCost(
    const ComparisonPredicate &predicate) const {
  if (predicate.isAttributeLiteralComparisonPredicate()) {
    std::pair<bool, attribute_id> comparison_attr = predicate.getAttributeFromAttributeLiteralComparison();
    if (comparison_attr.second == key_id_) {
      return predicate_cost::kBitWeavingHScan;
    }
  }
  return predicate_cost::kInfinite;
}

std::size_t BitWeavingHIndexSubBlock::getBytesNeeded(std::size_t code_length,
                                                     tuple_id num_tuples) const {
  std::size_t num_bits_per_code = code_length + 1;
  std::size_t num_codes_per_word = (sizeof(WordUnit) << 3) / num_bits_per_code;
  std::size_t num_words_per_segment = num_bits_per_code;
  std::size_t num_codes_per_segment = num_codes_per_word * num_words_per_segment;
  std::size_t num_segments = (num_tuples + num_codes_per_segment - 1)
      / num_codes_per_segment;
  std::size_t num_words = num_segments * num_words_per_segment;

  return num_words * sizeof(WordUnit);
}

bool BitWeavingHIndexSubBlock::initialize(bool new_block,
                                          void *internal_memory,
                                          std::size_t internal_memory_size) {
  num_bits_per_code_ = code_length_ + 1;
  num_codes_per_word_ = (sizeof(WordUnit) << 3) / num_bits_per_code_;
  num_padding_bits_ = (sizeof(WordUnit) << 3) - num_codes_per_word_ * num_bits_per_code_;
  num_words_per_segment_ = num_bits_per_code_;
  num_codes_per_segment_ = num_codes_per_word_ * num_words_per_segment_;
  num_segments_ = (tuple_store_.getMaxTupleID() + 1 + num_codes_per_segment_ - 1)
      / num_codes_per_segment_;
  num_words_ = num_segments_ * num_words_per_segment_;

  words_ = reinterpret_cast<WordUnit*>(static_cast<char*>(internal_memory));
  std::size_t size_bytes = num_words_ * sizeof(WordUnit);

  if (size_bytes > internal_memory_size) {
    return false;
  }

  return true;
}

bool BitWeavingHIndexSubBlock::buildBitWeavingInternal(
    void *internal_memory,
    std::size_t internal_memory_size,
    const CompressionDictionaryBuilder *builder) {
  CHECK_LE(code_length_, maximum_code_length_) << "BitWeavingHIndexSubBlock encountered long codes.";

  // Do nothing for empty index.
  if (code_length_ == 0) {
    return true;
  }

  if (!initialize(true, internal_memory, internal_memory_size)) {
    return false;
  }

  std::size_t segment_word_offset = 0;
  std::size_t code_id_in_segment = 0;
  std::size_t word_id_in_segment = 0;
  std::size_t code_offset_in_word = 0;
  const std::size_t num_useful_bits_per_word = (num_codes_per_word_ - 1) * num_bits_per_code_;

  // Have to go through all positions, as we need to incrementally update
  // position parameters.
  const bool is_packed = tuple_store_.isPacked();
  for (tuple_id tid = 0; tid <= tuple_store_.getMaxTupleID(); ++tid) {
    if (is_packed || tuple_store_.hasTupleWithID(tid)) {
      TypedValue value(tuple_store_.getAttributeValueTyped(tid, key_id_));
      std::uint32_t code;
      if (builder) {
        code = builder->getCodeForValue(value);
      } else if (dictionary_) {
        // Encode the attribute value using the dictionary.
        code = dictionary_->getCodeForTypedValue(value, *key_type_);
      } else {
        // Encode the attribute value using the truncation compression.
        DCHECK(!value.isNull());
        switch (key_type_->getTypeID()) {
          case kInt:
            code = static_cast<std::uint32_t>(value.getLiteral<int>());
            break;
          case kLong:
            code = static_cast<std::uint32_t>(value.getLiteral<std::int64_t>());
            break;
          default:
            LOG(FATAL) << "Non-integer type encountered in truncated "
                       << "BitWeavingHIndexSubBlock.";
        }
      }

      // Set the code.
      setCode(segment_word_offset + word_id_in_segment,
              num_useful_bits_per_word - code_offset_in_word,
              code);
    }

    // Maintain the code position parameters.
    // We avoid the use the expensive operators like multiplication and modular.
    ++code_id_in_segment;
    segment_word_offset += (code_id_in_segment == num_codes_per_segment_) ?
        num_words_per_segment_ : 0;
    code_id_in_segment = (code_id_in_segment == num_codes_per_segment_) ?
        0 : code_id_in_segment;
    ++word_id_in_segment;
    code_offset_in_word += (word_id_in_segment == num_words_per_segment_) ?
        num_bits_per_code_ : 0;
    code_offset_in_word = (code_offset_in_word == num_codes_per_segment_) ?
        0 : code_offset_in_word;
    word_id_in_segment = (word_id_in_segment == num_words_per_segment_) ?
        0 : word_id_in_segment;
  }

  return true;
}

TupleIdSequence* BitWeavingHIndexSubBlock::getMatchesForComparison(
    const std::uint32_t literal_code,
    const ComparisonID comp,
    const TupleIdSequence *filter) const {
  switch (code_length_) {
    case 0:
      LOG(FATAL) << "BitWeavingHIndexSubBlock does not support 0-bit codes.";
    case 1:
      return getMatchesForComparisonHelper<1>(literal_code, comp, filter);
    case 2:
      return getMatchesForComparisonHelper<2>(literal_code, comp, filter);
    case 3:
      return getMatchesForComparisonHelper<3>(literal_code, comp, filter);
    case 4:
      return getMatchesForComparisonHelper<4>(literal_code, comp, filter);
    case 5:
      return getMatchesForComparisonHelper<5>(literal_code, comp, filter);
    case 6:
      return getMatchesForComparisonHelper<6>(literal_code, comp, filter);
    case 7:
      return getMatchesForComparisonHelper<7>(literal_code, comp, filter);
    case 8:
      return getMatchesForComparisonHelper<8>(literal_code, comp, filter);
    case 9:
      return getMatchesForComparisonHelper<9>(literal_code, comp, filter);
    case 10:
      return getMatchesForComparisonHelper<10>(literal_code, comp, filter);
    case 11:
      return getMatchesForComparisonHelper<11>(literal_code, comp, filter);
    case 12:
      return getMatchesForComparisonHelper<12>(literal_code, comp, filter);
    case 13:
      return getMatchesForComparisonHelper<13>(literal_code, comp, filter);
    case 14:
      return getMatchesForComparisonHelper<14>(literal_code, comp, filter);
    case 15:
      return getMatchesForComparisonHelper<15>(literal_code, comp, filter);
    case 16:
      return getMatchesForComparisonHelper<16>(literal_code, comp, filter);
    case 17:
      return getMatchesForComparisonHelper<17>(literal_code, comp, filter);
    case 18:
      return getMatchesForComparisonHelper<18>(literal_code, comp, filter);
    case 19:
      return getMatchesForComparisonHelper<19>(literal_code, comp, filter);
    case 20:
      return getMatchesForComparisonHelper<20>(literal_code, comp, filter);
    case 21:
      return getMatchesForComparisonHelper<21>(literal_code, comp, filter);
    case 22:
      return getMatchesForComparisonHelper<22>(literal_code, comp, filter);
    case 23:
      return getMatchesForComparisonHelper<23>(literal_code, comp, filter);
    case 24:
      return getMatchesForComparisonHelper<24>(literal_code, comp, filter);
    case 25:
      return getMatchesForComparisonHelper<25>(literal_code, comp, filter);
    case 26:
      return getMatchesForComparisonHelper<26>(literal_code, comp, filter);
    case 27:
      return getMatchesForComparisonHelper<27>(literal_code, comp, filter);
    case 28:
      return getMatchesForComparisonHelper<28>(literal_code, comp, filter);
    case 29:
      return getMatchesForComparisonHelper<29>(literal_code, comp, filter);
    case 30:
      return getMatchesForComparisonHelper<30>(literal_code, comp, filter);
    case 31:
      return getMatchesForComparisonHelper<31>(literal_code, comp, filter);
    default:
      LOG(FATAL) << "BitWeavingHIndexSubBlock does not support codes "
                 << "that are longer than 31-bit.";
  }
}

template <std::size_t CODE_LENGTH>
TupleIdSequence* BitWeavingHIndexSubBlock::getMatchesForComparisonHelper(
    const std::uint32_t literal_code,
    const ComparisonID comp,
    const TupleIdSequence *filter) const {
  switch (comp) {
    case ComparisonID::kEqual:
      return getMatchesForComparisonInstantiation<CODE_LENGTH, ComparisonID::kEqual>(
          literal_code, filter);
    case ComparisonID::kNotEqual:
      return getMatchesForComparisonInstantiation<CODE_LENGTH, ComparisonID::kNotEqual>(
          literal_code, filter);
    case ComparisonID::kLess:
      return getMatchesForComparisonInstantiation<CODE_LENGTH, ComparisonID::kLess>(
          literal_code, filter);
    case ComparisonID::kLessOrEqual:
      return getMatchesForComparisonInstantiation<CODE_LENGTH, ComparisonID::kLessOrEqual>(
          literal_code, filter);
    case ComparisonID::kGreater:
      return getMatchesForComparisonInstantiation<CODE_LENGTH, ComparisonID::kGreater>(
          literal_code, filter);
    case ComparisonID::kGreaterOrEqual:
      return getMatchesForComparisonInstantiation<CODE_LENGTH, ComparisonID::kGreaterOrEqual>(
          literal_code, filter);
    default:
      LOG(FATAL) << "Unknown comparator in BitWeavingHIndexSubBlock";
  }
}

template <std::size_t CODE_LENGTH, ComparisonID COMP>
TupleIdSequence* BitWeavingHIndexSubBlock::getMatchesForComparisonInstantiation(
    const std::uint32_t literal_code,
    const TupleIdSequence *filter) const {
  DEBUG_ASSERT(literal_code < (1ULL << CODE_LENGTH));

  constexpr std::size_t kNumBitsPerWord = sizeof(WordUnit) << 3;
  // We will break down the loop over all bit positions into several
  // fixed-length small loops called groups, making it easy to
  // unroll these loops.
  // Based on experimental results, it is effitient to set the group
  // size to be 7.
  constexpr std::size_t kNumWordsPerGroup = 7;
  constexpr std::size_t kNumFilledGroups = (CODE_LENGTH + 1) / kNumWordsPerGroup;
  constexpr std::size_t kNumWordsInUnfilledGroup = (CODE_LENGTH + 1)
      - kNumWordsPerGroup * kNumFilledGroups;

  // Generate masks
  // k = number of bits per code
  // base_mask: 0^k 1 0^k 1 ... 0^k 1
  // predicate_mask: 0 code 0 code ... 0 code
  // complement_mask: 0 1^k 0 1^k 0 ... 0 1^k
  // result_mask: 1 0^k 1 0^k 1 ... 1 0^k
  WordUnit base_mask = 0;
  for (std::size_t i = 0; i < num_codes_per_word_; ++i) {
    base_mask = (base_mask << num_bits_per_code_) | static_cast<WordUnit>(1);
  }
  WordUnit complement_mask = base_mask * static_cast<WordUnit>((1ULL << CODE_LENGTH) - 1);
  WordUnit result_mask = base_mask << CODE_LENGTH;

  WordUnit less_than_mask = base_mask * literal_code;
  WordUnit greater_than_mask = (base_mask * literal_code) ^ complement_mask;
  WordUnit equal_mask = base_mask
      * (~literal_code & (static_cast<WordUnit>(-1) >> (kNumBitsPerWord - CODE_LENGTH)));
  WordUnit inequal_mask = base_mask * literal_code;

  std::size_t num_tuples = tuple_store_.getMaxTupleID() + 1;
  TupleIdSequence *sequence = new TupleIdSequence(num_tuples);

  WordUnit word_bitvector, output_bitvector = 0;
  std::int64_t output_offset = 0;
  std::size_t output_word_id = 0;
  for (std::size_t segment_offset = 0;
       segment_offset < num_words_;
       segment_offset += num_words_per_segment_) {
    WordUnit segment_bitvector = 0;
    std::size_t word_id = 0;

    // A loop over all words inside a segment.
    // We break down the loop over all bit positions into several
    // fixed-length small loops.
    for (std::size_t bit_group_id = 0; bit_group_id < kNumFilledGroups; ++bit_group_id) {
      // For more details about these bitwise operators, see the BitWeaving paper:
      // http://quickstep.cs.wisc.edu/pubs/bitweaving-sigmod.pdf
      for (std::size_t bit = 0; bit < kNumWordsPerGroup; ++bit) {
        switch (COMP) {
          case ComparisonID::kEqual:
            word_bitvector = ((words_[segment_offset + word_id] ^ equal_mask)
                + base_mask) & result_mask;
            break;
          case ComparisonID::kNotEqual:
            word_bitvector = ((words_[segment_offset + word_id] ^ inequal_mask)
                + complement_mask) & result_mask;
            break;
          case ComparisonID::kGreater:
            word_bitvector = (words_[segment_offset + word_id] + greater_than_mask)
                & result_mask;
            break;
          case ComparisonID::kLess:
            word_bitvector = (less_than_mask + (words_[segment_offset + word_id]
                ^ complement_mask)) & result_mask;
            break;
          case ComparisonID::kGreaterOrEqual:
            word_bitvector = ~(less_than_mask + (words_[segment_offset + word_id]
                ^ complement_mask)) & result_mask;
            break;
          case ComparisonID::kLessOrEqual:
            word_bitvector = ~(words_[segment_offset + word_id] + greater_than_mask)
                & result_mask;
            break;
          default:
            LOG(FATAL) << "Unknown comparator in BitWeavingHIndexSubBlock";
        }
        segment_bitvector |= word_bitvector >> word_id++;
      }
    }

    // A loop on the rest of bit positions
    // For more details about these bitwise operators, see the BitWeaving paper:
    // http://quickstep.cs.wisc.edu/pubs/bitweaving-sigmod.pdf
    for (std::size_t bit = 0; bit < kNumWordsInUnfilledGroup; ++bit) {
      switch (COMP) {
        case ComparisonID::kEqual:
          word_bitvector = ((words_[segment_offset + word_id] ^ equal_mask)
              + base_mask) & result_mask;
          break;
        case ComparisonID::kNotEqual:
          word_bitvector = ((words_[segment_offset + word_id] ^ inequal_mask)
              + complement_mask) & result_mask;
          break;
        case ComparisonID::kGreater:
          word_bitvector = (words_[segment_offset + word_id] + greater_than_mask)
              & result_mask;
          break;
        case ComparisonID::kLess:
          word_bitvector = (less_than_mask + (words_[segment_offset + word_id]
              ^ complement_mask)) & result_mask;
          break;
        case ComparisonID::kGreaterOrEqual:
          word_bitvector = ~(less_than_mask + (words_[segment_offset + word_id]
              ^ complement_mask)) & result_mask;
          break;
        case ComparisonID::kLessOrEqual:
          word_bitvector = ~(words_[segment_offset + word_id] + greater_than_mask)
                  & result_mask;
          break;
        default:
          LOG(FATAL) << "Unknown comparator in BitWeavingHIndexSubBlock";
      }
      segment_bitvector |= word_bitvector >> word_id++;
    }

    output_bitvector |= (segment_bitvector << num_padding_bits_) >> output_offset;
    output_offset += num_codes_per_segment_;

    if (static_cast<std::uint64_t>(output_offset) >= (sizeof(WordUnit) << 3)) {
      WordUnit filter_mask = filter ? filter->getWord(output_word_id) : static_cast<WordUnit>(-1);
      sequence->setWord(output_word_id, output_bitvector & filter_mask);

      output_offset -= kNumBitsPerWord;
      std::size_t output_shift = kNumBitsPerWord - output_offset;
      output_bitvector = segment_bitvector << output_shift;
      // Clear up outputBitvector if outputOffset = kNumBitsPerWord
      output_bitvector &= static_cast<WordUnit>(0) - (output_shift != kNumBitsPerWord);
      ++output_word_id;
    }
  }

  output_offset -= num_words_ * num_codes_per_word_ - num_tuples;
  if (output_offset > 0) {
    WordUnit filter_mask = filter ? filter->getWord(output_word_id) : static_cast<WordUnit>(-1);
    sequence->setWord(output_word_id, output_bitvector & filter_mask);
  }

  return sequence;
}

}  // namespace quickstep
