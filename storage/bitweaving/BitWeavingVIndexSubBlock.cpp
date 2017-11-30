/**
 *   Copyright 2013-2016, Quickstep Research Group, Computer Sciences Department,
 *     University of Wisconsin—Madison.
 **/

#include "storage/bitweaving/BitWeavingVIndexSubBlock.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
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
#include "storage/SubBlockTypeRegistry.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "storage/ValueAccessor.hpp"
#include "storage/ValueAccessorUtil.hpp"
#include "types/TypedValue.hpp"
#include "types/operations/comparisons/ComparisonID.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

QUICKSTEP_REGISTER_INDEX(BitWeavingVIndexSubBlock, BITWEAVING_V);

BitWeavingVIndexSubBlock::BitWeavingVIndexSubBlock(const TupleStorageSubBlock &tuple_store,
                                                   const IndexSubBlockDescription &description,
                                                   const bool new_block,
                                                   void *sub_block_memory,
                                                   const std::size_t sub_block_memory_size)
    : BitWeavingIndexSubBlock(tuple_store,
                              description,
                              new_block,
                              sub_block_memory,
                              sub_block_memory_size,
                              32) {
  std::size_t header_size = getHeaderSize();
  initialize(new_block,
             static_cast<char*>(sub_block_memory) + header_size,
             sub_block_memory_size - header_size);
}

bool BitWeavingVIndexSubBlock::DescriptionIsValid(const CatalogRelationSchema &relation,
                                                  const IndexSubBlockDescription &description) {
  // Make sure description is initialized and specifies BitWeaving.
  if (!description.IsInitialized()) {
    return false;
  }

  if (description.sub_block_type() != IndexSubBlockDescription::BITWEAVING_V) {
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
std::size_t BitWeavingVIndexSubBlock::EstimateBytesPerTuple(
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

predicate_cost_t BitWeavingVIndexSubBlock::estimatePredicateEvaluationCost(
    const ComparisonPredicate &predicate) const {
  if (predicate.isAttributeLiteralComparisonPredicate()) {
    std::pair<bool, attribute_id> comparison_attr = predicate.getAttributeFromAttributeLiteralComparison();
    if (comparison_attr.second == key_id_) {
      return predicate_cost::kBitWeavingVScan;
    }
  }
  return predicate_cost::kInfinite;
}

std::size_t BitWeavingVIndexSubBlock::getBytesNeeded(std::size_t code_length,
                                                     tuple_id num_tuples) const {
  std::size_t num_filled_groups = code_length / kNumBitsPerGroup;
  std::size_t num_bits_in_unfilled_group = code_length
      - kNumBitsPerGroup * num_filled_groups;

  std::size_t num_segments = (num_tuples + kNumCodesPerSegment - 1) / kNumCodesPerSegment;
  std::size_t num_words = num_segments * kNumWordsPerSegment;

  return num_words * sizeof(WordUnit) * num_filled_groups
      + num_segments * num_bits_in_unfilled_group * sizeof(WordUnit);
}

bool BitWeavingVIndexSubBlock::initialize(bool new_block,
                                          void *internal_memory,
                                          std::size_t internal_memory_size) {
  num_groups_ = (code_length_ + kNumBitsPerGroup - 1) / kNumBitsPerGroup;
  num_filled_groups_ = code_length_ / kNumBitsPerGroup;
  num_bits_in_unfilled_group_ = code_length_ - kNumBitsPerGroup * num_filled_groups_;

  num_segments_ = (tuple_store_.getMaxTupleID() + 1 + kNumCodesPerSegment - 1)
      / kNumCodesPerSegment;
  num_words_ = num_segments_ * kNumWordsPerSegment;

  std::size_t offset = 0;
  for (std::size_t group_id = 0; group_id < kMaxNumGroups; ++group_id) {
    if (group_id < num_groups_) {
      group_words_[group_id] =
          reinterpret_cast<WordUnit*>(static_cast<char*>(internal_memory) + offset);
      offset += num_words_ * sizeof(WordUnit);
    } else {
      group_words_[group_id] = nullptr;
    }
  }

  if (offset > internal_memory_size) {
    return false;
  }

  return true;
}

std::uint32_t BitWeavingVIndexSubBlock::getCode(const tuple_id tid) const {
  DCHECK_LE(tid, tuple_store_.getMaxTupleID());

  std::size_t segment_id = tid / kNumCodesPerSegment;
  std::size_t offset_in_segment = kNumCodesPerSegment - 1 - (tid % kNumCodesPerSegment);
  WordUnit mask = static_cast<WordUnit>(1) << offset_in_segment;
  WordUnit code_word = 0;

  std::size_t bit_id = 0;
  for (std::size_t group_id = 0; group_id < num_filled_groups_; ++group_id) {
    std::size_t word_id = segment_id * kNumWordsPerSegment;
    for (std::size_t bit_id_in_group = 0;
        bit_id_in_group < kNumBitsPerGroup;
        ++bit_id_in_group) {
      code_word |= ((group_words_[group_id][word_id] & mask) >> offset_in_segment)
          << (code_length_ - 1 - bit_id);
      ++word_id;
      ++bit_id;
    }
  }

  std::size_t word_id = segment_id * num_bits_in_unfilled_group_;
  for (std::size_t bit_id_in_group = 0;
      bit_id_in_group < num_bits_in_unfilled_group_;
      ++bit_id_in_group) {
    code_word |= ((group_words_[num_filled_groups_][word_id] & mask) >> offset_in_segment)
        << (code_length_ - 1 - bit_id);
    ++word_id;
    ++bit_id;
  }
  return static_cast<std::uint32_t>(code_word);
}

void BitWeavingVIndexSubBlock::setCode(const tuple_id tid, const std::uint32_t code) {
  DCHECK_LE(tid, tuple_store_.getMaxTupleID());

  std::size_t segment_id = tid / kNumCodesPerSegment;
  std::size_t offset_in_segment = kNumCodesPerSegment - 1
      - (tid % kNumCodesPerSegment);
  WordUnit mask = static_cast<WordUnit>(1) << offset_in_segment;
  WordUnit code_word = static_cast<WordUnit>(code);

  std::size_t bit_id = 0;
  for (std::size_t group_id = 0; group_id < num_filled_groups_; ++group_id) {
    std::size_t word_id = segment_id * kNumWordsPerSegment;
    for (std::size_t bit_id_in_group = 0;
        bit_id_in_group < kNumBitsPerGroup;
        ++bit_id_in_group) {
      group_words_[group_id][word_id] &= ~mask;
      group_words_[group_id][word_id] |=
          ((code_word >> (code_length_ - 1 - bit_id)) << offset_in_segment) & mask;
      ++word_id;
      ++bit_id;
    }
  }

  std::size_t word_id = segment_id * num_bits_in_unfilled_group_;
  for (std::size_t bit_id_in_group = 0;
      bit_id_in_group < num_bits_in_unfilled_group_;
      ++bit_id_in_group) {
    group_words_[num_filled_groups_][word_id] &= ~mask;
    group_words_[num_filled_groups_][word_id] |=
        ((code_word >> (code_length_ - 1 - bit_id)) << offset_in_segment) & mask;
    ++word_id;
    ++bit_id;
  }
}

bool BitWeavingVIndexSubBlock::buildBitWeavingInternal(void *internal_memory,
                                                       std::size_t internal_memory_size,
                                                       const CompressionDictionaryBuilder *builder) {
  if (code_length_ > maximum_code_length_) {
    LOG(FATAL) << "BitWeavingVIndexSubBlock encounters long codes.";
  }

  // Do nothing for empty index.
  if (code_length_ == 0) {
    return true;
  }

  if (!initialize(true, internal_memory, internal_memory_size)) {
    return false;
  }

  std::unique_ptr<ValueAccessor> accessor(tuple_store_.createValueAccessor());

  InvokeOnAnyValueAccessor(
      accessor.get(),
      [&](auto *accessor) -> void {  // NOLINT(build/c++11)
    while (accessor->next()) {
      TypedValue value = accessor->getTypedValue(key_id_);
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
                       << "BitWeavingVIndexSubBlock.";
        }
      }

      // Set the code.
      this->setCode(accessor->getCurrentPosition(), code);
    }
  });  // NOLINT(whitespace/parens)

  return true;
}

TupleIdSequence* BitWeavingVIndexSubBlock::getMatchesForComparison(
    const std::uint32_t literal_code,
    const ComparisonID comp,
    const TupleIdSequence *filter) const {
  switch (code_length_) {
    case 0:
      LOG(FATAL) << "BitWeavingVIndexSubBlock does not support 0-bit codes.";
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
    case 32:
      return getMatchesForComparisonHelper<32>(literal_code, comp, filter);
    default:
      LOG(FATAL) << "BitWeavingVIndexSubBlock does not support codes "
                 << "that are longer than 32-bit.";
  }
}

template <std::size_t CODE_LENGTH>
TupleIdSequence* BitWeavingVIndexSubBlock::getMatchesForComparisonHelper(
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
      LOG(FATAL) << "Unknown comparator in BitWeavingVIndexSubBlock";
  }
}

template <std::size_t CODE_LENGTH, ComparisonID COMP>
TupleIdSequence* BitWeavingVIndexSubBlock::getMatchesForComparisonInstantiation(
    const std::uint32_t literal_code,
    const TupleIdSequence *filter) const {
  DCHECK(literal_code < (1LL << CODE_LENGTH));

  static constexpr std::size_t kNumFilledGroups = CODE_LENGTH / kNumBitsPerGroup;
  static constexpr std::size_t kNumBitsInUnfilledGroup = CODE_LENGTH - kNumBitsPerGroup * kNumFilledGroups;

  TupleIdSequence *sequence = new TupleIdSequence(tuple_store_.getMaxTupleID() + 1);

  WordUnit literal_bits[CODE_LENGTH];
  for (std::size_t bit_id = 0; bit_id < CODE_LENGTH; ++bit_id) {
    literal_bits[bit_id] = static_cast<WordUnit>(0)
        - ((literal_code >> (CODE_LENGTH - 1 - bit_id)) & static_cast<WordUnit>(1));
  }

  for (std::size_t segment_offset = 0; segment_offset < num_words_;
      segment_offset += kNumWordsPerSegment) {
    std::size_t segment_id = segment_offset / kNumWordsPerSegment;
    WordUnit mask_less = 0;
    WordUnit mask_greater = 0;
    WordUnit mask_equal;

    // Use the filter bitmap to initialize mask_equal, and to make
    // the early pruning occur early. For more details, see the
    // BitWeaving paper:
    // http://quickstep.cs.wisc.edu/pubs/bitweaving-sigmod.pdf
    WordUnit filter_mask = filter ? filter->getWord(segment_id) : static_cast<WordUnit>(-1);
    mask_equal = filter_mask;

    WordUnit *literal_bit_ptr = literal_bits;
    // The code below needs to be updated if kNumBitsPerGroup is changed.
    // Note: the first comparison in each IF condition can be determined
    // at the compile time.
    if (CODE_LENGTH >= kNumBitsPerGroup && mask_equal != 0) {
      scanGroup<CODE_LENGTH, COMP, 0>(segment_offset, &mask_equal, &mask_less,
                                      &mask_greater, &literal_bit_ptr);
      if (CODE_LENGTH >= kNumBitsPerGroup * 2 && mask_equal != 0) {
        scanGroup<CODE_LENGTH, COMP, 1>(segment_offset, &mask_equal, &mask_less,
                                        &mask_greater, &literal_bit_ptr);
        if (CODE_LENGTH >= kNumBitsPerGroup * 3 && mask_equal != 0) {
          scanGroup<CODE_LENGTH, COMP, 2>(segment_offset, &mask_equal, &mask_less,
                                          &mask_greater, &literal_bit_ptr);
          if (CODE_LENGTH >= kNumBitsPerGroup * 4 && mask_equal != 0) {
            scanGroup<CODE_LENGTH, COMP, 3>(segment_offset, &mask_equal, &mask_less,
                                            &mask_greater, &literal_bit_ptr);
            if (CODE_LENGTH >= kNumBitsPerGroup * 5 && mask_equal != 0) {
              scanGroup<CODE_LENGTH, COMP, 4>(segment_offset, &mask_equal, &mask_less,
                                              &mask_greater, &literal_bit_ptr);
              if (CODE_LENGTH >= kNumBitsPerGroup * 6 && mask_equal != 0) {
                scanGroup<CODE_LENGTH, COMP, 5>(segment_offset, &mask_equal, &mask_less,
                                                &mask_greater, &literal_bit_ptr);
                if (CODE_LENGTH >= kNumBitsPerGroup * 7 && mask_equal != 0) {
                  scanGroup<CODE_LENGTH, COMP, 6>(segment_offset, &mask_equal, &mask_less,
                                                  &mask_greater, &literal_bit_ptr);
                  if (CODE_LENGTH >= kNumBitsPerGroup * 8 && mask_equal != 0) {
                    scanGroup<CODE_LENGTH, COMP, 7>(segment_offset, &mask_equal, &mask_less,
                                                    &mask_greater, &literal_bit_ptr);
                  }
                }
              }
            }
          }
        }
      }
    }

    if (kNumBitsInUnfilledGroup != 0 && mask_equal != 0) {
      std::size_t last_group_segment_offset = segment_id * kNumBitsInUnfilledGroup;
      scanGroup<CODE_LENGTH, COMP, kNumFilledGroups>(last_group_segment_offset, &mask_equal,
                                                     &mask_less, &mask_greater, &literal_bit_ptr);
    }

    WordUnit mask;
    switch (COMP) {
      case ComparisonID::kEqual:
        mask = mask_equal;
        break;
      case ComparisonID::kNotEqual:
        mask = ~mask_equal;
        break;
      case ComparisonID::kLess:
        mask = mask_less;
        break;
      case ComparisonID::kLessOrEqual:
        mask = mask_less | mask_equal;
        break;
      case ComparisonID::kGreater:
        mask = mask_greater;
        break;
      case ComparisonID::kGreaterOrEqual:
        mask = mask_greater | mask_equal;
        break;
      default:
        LOG(FATAL) << "Unknown comparator in BitWeavingVIndexSubBlock";
    }

    sequence->setWord(segment_id, mask & filter_mask);
  }

  return sequence;
}

template<std::size_t CODE_LENGTH, ComparisonID COMP, std::size_t GROUP_ID>
void BitWeavingVIndexSubBlock::scanGroup(const std::size_t segment_offset,
                                         WordUnit *mask_equal,
                                         WordUnit *mask_less,
                                         WordUnit *mask_greater,
                                         WordUnit **literal_bit_ptr) const {
  static constexpr std::size_t kNumFilledGroups = CODE_LENGTH / kNumBitsPerGroup;
  static constexpr std::size_t kNumBitsInUnfilledGroup = CODE_LENGTH - kNumBitsPerGroup * kNumFilledGroups;
  static constexpr std::size_t NUM_BITS =
      GROUP_ID == kNumFilledGroups ? kNumBitsInUnfilledGroup : kNumBitsPerGroup;
  WordUnit *word_ptr = &group_words_[GROUP_ID][segment_offset];

  // For more details about these bitwise operators, see the BitWeaving paper:
  // http://quickstep.cs.wisc.edu/pubs/bitweaving-sigmod.pdf
  for (std::size_t bit_id = 0; bit_id < NUM_BITS; ++bit_id) {
    switch (COMP) {
      case ComparisonID::kEqual:
      case ComparisonID::kNotEqual:
        *mask_equal = *mask_equal & ~(*word_ptr ^ **literal_bit_ptr);
        break;
      case ComparisonID::kLess:
      case ComparisonID::kLessOrEqual:
        *mask_less = *mask_less | (*mask_equal & ~*word_ptr & **literal_bit_ptr);
        *mask_equal = *mask_equal & ~(*word_ptr ^ **literal_bit_ptr);
        break;
      case ComparisonID::kGreater:
      case ComparisonID::kGreaterOrEqual:
        *mask_greater = *mask_greater | (*mask_equal & *word_ptr & ~**literal_bit_ptr);
        *mask_equal = *mask_equal & ~(*word_ptr ^ **literal_bit_ptr);
        break;
    }
    ++word_ptr;
    ++(*literal_bit_ptr);
  }
}

}  // namespace quickstep
