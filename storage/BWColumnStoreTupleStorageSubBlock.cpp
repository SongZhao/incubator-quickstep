/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 **/

#include "storage/BWColumnStoreTupleStorageSubBlock.hpp"

#include <cstddef>
#include <cstring>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogRelationSchema.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "expressions/predicate/ComparisonPredicate.hpp"
#include "expressions/predicate/Predicate.hpp"
#include "expressions/predicate/PredicateCost.hpp"
#include "expressions/scalar/Scalar.hpp"
#include "storage/BWColumnStoreValueAccessor.hpp"
#include "storage/ColumnStoreUtil.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/StorageErrors.hpp"
#include "storage/SubBlockTypeRegistry.hpp"
#include "storage/TupleIdSequence.hpp"
#include "storage/ValueAccessor.hpp"
#include "storage/ValueAccessorUtil.hpp"
#include "types/Type.hpp"
#include "types/TypedValue.hpp"
#include "types/containers/Tuple.hpp"
#include "types/operations/comparisons/Comparison.hpp"
#include "types/operations/comparisons/ComparisonFactory.hpp"
#include "types/operations/comparisons/ComparisonID.hpp"
#include "types/operations/comparisons/ComparisonUtil.hpp"
#include "utility/BitVector.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrMap.hpp"
#include "utility/PtrVector.hpp"
#include "utility/ScopedBuffer.hpp"

#include "glog/logging.h"

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

using std::memcpy;
using std::memmove;
using std::size_t;
using std::vector;

using quickstep::column_store_util::ColumnStripeIterator;
using quickstep::column_store_util::SortColumnPredicateEvaluator;

namespace quickstep {

QUICKSTEP_REGISTER_TUPLE_STORE(BWColumnStoreTupleStorageSubBlock, BW_COLUMN_STORE);

// Hide this helper in an anonymous namespace:
namespace {

class SortColumnValueReference {
 public:
  SortColumnValueReference(const void *value, const tuple_id tuple)
      : value_(value), tuple_(tuple) {
  }

  explicit SortColumnValueReference(const void *value)
      : value_(value), tuple_(-1) {
  }

  inline const void* getDataPtr() const {
    return value_;
  }

  inline tuple_id getTupleID() const {
    return tuple_;
  }

 private:
  const void *value_;
  tuple_id tuple_;
};

}  // anonymous namespace


TupleIdSequence* BWColumnStoreTupleStorageSubBlock::getMatchesForComparison(
    const std::uint32_t literal_code,
    const ComparisonID comp,
    const TupleIdSequence *filter, const attribute_id key_id) const {
  //std::cout << "------ get in getMatchesForComparison" << std::endl;
  switch (code_length_) {
    case 0:
      LOG(FATAL) << "BitWeavingHIndexSubBlock does not support 0-bit codes.";
    case 1:
      return getMatchesForComparisonHelper<1>(literal_code, comp, filter, key_id);
    case 2:
      return getMatchesForComparisonHelper<2>(literal_code, comp, filter, key_id);
    case 3:
      return getMatchesForComparisonHelper<3>(literal_code, comp, filter, key_id);
    case 4:
      return getMatchesForComparisonHelper<4>(literal_code, comp, filter, key_id);
    case 5:
      return getMatchesForComparisonHelper<5>(literal_code, comp, filter, key_id);
    case 6:
      return getMatchesForComparisonHelper<6>(literal_code, comp, filter, key_id);
    case 7:
      return getMatchesForComparisonHelper<7>(literal_code, comp, filter, key_id);
    case 8:
      return getMatchesForComparisonHelper<8>(literal_code, comp, filter, key_id);
    case 9:
      return getMatchesForComparisonHelper<9>(literal_code, comp, filter, key_id);
    case 10:
      return getMatchesForComparisonHelper<10>(literal_code, comp, filter, key_id);
    case 11:
      return getMatchesForComparisonHelper<11>(literal_code, comp, filter, key_id);
    case 12:
      return getMatchesForComparisonHelper<12>(literal_code, comp, filter, key_id);
    case 13:
      return getMatchesForComparisonHelper<13>(literal_code, comp, filter, key_id);
    case 14:
      return getMatchesForComparisonHelper<14>(literal_code, comp, filter, key_id);
    case 15:
      return getMatchesForComparisonHelper<15>(literal_code, comp, filter, key_id);
    case 16:
      return getMatchesForComparisonHelper<16>(literal_code, comp, filter, key_id);
    case 17:
      return getMatchesForComparisonHelper<17>(literal_code, comp, filter, key_id);
    case 18:
      return getMatchesForComparisonHelper<18>(literal_code, comp, filter, key_id);
    case 19:
      return getMatchesForComparisonHelper<19>(literal_code, comp, filter, key_id);
    case 20:
      return getMatchesForComparisonHelper<20>(literal_code, comp, filter, key_id);
    case 21:
      return getMatchesForComparisonHelper<21>(literal_code, comp, filter, key_id);
    case 22:
      return getMatchesForComparisonHelper<22>(literal_code, comp, filter, key_id);
    case 23:
      return getMatchesForComparisonHelper<23>(literal_code, comp, filter, key_id);
    case 24:
      return getMatchesForComparisonHelper<24>(literal_code, comp, filter, key_id);
    case 25:
      return getMatchesForComparisonHelper<25>(literal_code, comp, filter, key_id);
    case 26:
      return getMatchesForComparisonHelper<26>(literal_code, comp, filter, key_id);
    case 27:
      return getMatchesForComparisonHelper<27>(literal_code, comp, filter, key_id);
    case 28:
      return getMatchesForComparisonHelper<28>(literal_code, comp, filter, key_id);
    case 29:
      return getMatchesForComparisonHelper<29>(literal_code, comp, filter, key_id);
    case 30:
      return getMatchesForComparisonHelper<30>(literal_code, comp, filter, key_id);
    case 31:
      return getMatchesForComparisonHelper<31>(literal_code, comp, filter, key_id);
    default:
      LOG(FATAL) << "BitWeavingHIndexSubBlock does not support codes "
                 << "that are longer than 31-bit.";
  }
}

template <std::size_t CODE_LENGTH>
TupleIdSequence* BWColumnStoreTupleStorageSubBlock::getMatchesForComparisonHelper(
    const std::uint32_t literal_code,
    const ComparisonID comp,
    const TupleIdSequence *filter, const attribute_id key_id) const {
  
  //std::cout << "------ get in getMatchesForComparisonHelper" << std::endl;
  switch (comp) {
    case ComparisonID::kEqual:
      return getMatchesForComparisonInstantiation<CODE_LENGTH, ComparisonID::kEqual>(
          literal_code, filter, key_id);
    case ComparisonID::kNotEqual:
      return getMatchesForComparisonInstantiation<CODE_LENGTH, ComparisonID::kNotEqual>(
          literal_code, filter, key_id);
    case ComparisonID::kLess:
      return getMatchesForComparisonInstantiation<CODE_LENGTH, ComparisonID::kLess>(
          literal_code, filter, key_id);
    case ComparisonID::kLessOrEqual:
      return getMatchesForComparisonInstantiation<CODE_LENGTH, ComparisonID::kLessOrEqual>(
          literal_code, filter, key_id);
    case ComparisonID::kGreater:
      return getMatchesForComparisonInstantiation<CODE_LENGTH, ComparisonID::kGreater>(
          literal_code, filter, key_id);
    case ComparisonID::kGreaterOrEqual:
      return getMatchesForComparisonInstantiation<CODE_LENGTH, ComparisonID::kGreaterOrEqual>(
          literal_code, filter, key_id);
    default:
      LOG(FATAL) << "Unknown comparator in BitWeavingHIndexSubBlock";
  }
}

template <std::size_t CODE_LENGTH, ComparisonID COMP>
TupleIdSequence* BWColumnStoreTupleStorageSubBlock::getMatchesForComparisonInstantiation(
    const std::uint32_t literal_code,
    const TupleIdSequence *filter, const attribute_id key_id) const {
  DEBUG_ASSERT(literal_code < (1ULL << CODE_LENGTH));
  //std::cout << "@ getMatchesForComparisonInstantiation and literal code is " << literal_code << "attr id is " << key_id << std::endl;
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
  //std::cout << "num_code/word is " << num_codes_per_word_[key_id] << std::endl; 
  for (std::size_t i = 0; i < num_codes_per_word_[key_id]; ++i) {
    base_mask = (base_mask << num_bits_per_code_[key_id]) | static_cast<WordUnit>(1);
  }
  //std::cout << "1" << std::endl;
  WordUnit complement_mask = base_mask * static_cast<WordUnit>((1ULL << CODE_LENGTH) - 1);
  WordUnit result_mask = base_mask << CODE_LENGTH;
  //std::cout << "2" << std::endl;

  WordUnit less_than_mask = base_mask * literal_code;
  WordUnit greater_than_mask = (base_mask * literal_code) ^ complement_mask;
  WordUnit equal_mask = base_mask
      * (~literal_code & (static_cast<WordUnit>(-1) >> (kNumBitsPerWord - CODE_LENGTH)));
  WordUnit inequal_mask = base_mask * literal_code;
  //std::cout << "3" << std::endl;

  std::size_t num_tuples = getMaxTupleID() + 1;
  //std::size_t num_tuples = tuple_store_.getMaxTupleID() + 1;
  TupleIdSequence *sequence = new TupleIdSequence(num_tuples);
  //std::cout << "4" << std::endl;

  WordUnit word_bitvector, output_bitvector = 0;
  std::int64_t output_offset = 0;
  std::size_t output_word_id = 0;
  // Kan: TODO
  //std::cout << "num codes per segment is " << num_codes_per_segment_[key_id] << std::endl;
  std::size_t num_words_ = (num_tuples + num_codes_per_segment_[key_id] - 1)/ num_codes_per_segment_[key_id];
  // ADD:
  
  WordUnit *words_ = (WordUnit*)column_stripes_[key_id];
  //std::cout << "for each segment in this columstripe" << std::endl;
  for (std::size_t segment_offset = 0;
       //segment_offset < num_words_;
       segment_offset < num_words_;
       segment_offset += num_words_per_segment_[key_id]) {
    WordUnit segment_bitvector = 0;
    std::size_t word_id = 0;
    //std::cout << "segment offset is " << segment_offset << std::endl;
    // A loop over all words inside a segment.
    // We break down the loop over all bit positions into several
    // fixed-length small loops.
    for (std::size_t bit_group_id = 0; bit_group_id < kNumFilledGroups; ++bit_group_id) {
      // For more details about these bitwise operators, see the BitWeaving paper:
      // http://quickstep.cs.wisc.edu/pubs/bitweaving-sigmod.pdf
      //std::cout << "current bit_group_id is " << bit_group_id << " kNumFilledGroup is " << kNumFilledGroups << std::endl;
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
	    //std::cout << "before less "<< std::endl;
            word_bitvector = (less_than_mask + (words_[segment_offset + word_id]
                ^ complement_mask)) & result_mask;
	    //std::cout << "after less "<< std::endl;
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

    output_bitvector |= (segment_bitvector << num_padding_bits_[key_id]) >> output_offset;
    output_offset += num_codes_per_segment_[key_id];   //TODO???

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

  output_offset -= num_words_ * num_codes_per_word_[key_id] - num_tuples;
  //output_offset = 1111;
  if (output_offset > 0) {
    WordUnit filter_mask = filter ? filter->getWord(output_word_id) : static_cast<WordUnit>(-1);
    sequence->setWord(output_word_id, output_bitvector & filter_mask);
  }


  return sequence;
}


// =============================================
BWColumnStoreTupleStorageSubBlock::BWColumnStoreTupleStorageSubBlock(
    const CatalogRelationSchema &relation,
    const TupleStorageSubBlockDescription &description,
    const bool new_block,
    void *sub_block_memory,
    const std::size_t sub_block_memory_size)
    : BWTupleStorageSubBlock(relation,
    //: TupleStorageSubBlock(relation,
                           description,
                           new_block,
                           sub_block_memory,
                           sub_block_memory_size),
      sort_specified_(description_.HasExtension(
          BWColumnStoreTupleStorageSubBlockDescription::sort_attribute_id)),
      sorted_(true),
      header_(static_cast<BWColumnStoreHeader*>(sub_block_memory)) {
  if (!DescriptionIsValid(relation_, description_)) {
    FATAL_ERROR("Attempted to construct a BWColumnStoreTupleStorageSubBlock from an invalid description.");
  }

  if (sort_specified_) {
    sort_column_id_ = description_.GetExtension(
        BWColumnStoreTupleStorageSubBlockDescription::sort_attribute_id);
    sort_column_type_ = &(relation_.getAttributeById(sort_column_id_)->getType());
  }

  if (sub_block_memory_size < sizeof(BWColumnStoreHeader)) {
    throw BlockMemoryTooSmall("BWColumnStoreTupleStorageSubBlock", sub_block_memory_size_);
  }
  //col and row for column_stripes
  num_codes_per_word_.resize(relation_.getMaxAttributeId() + 1);
  num_padding_bits_.resize(relation_.getMaxAttributeId() + 1); 
  num_codes_per_segment_.resize(relation_.getMaxAttributeId() + 1);
  num_words_per_segment_.resize(relation_.getMaxAttributeId() + 1);
  num_words_per_code_.resize(relation_.getMaxAttributeId() + 1);
  num_bits_per_code_.resize(relation_.getMaxAttributeId() + 1);
  int size = 0;
  //std::cout << "wordunit is " << (sizeof(WordUnit)<<3) << std::endl; 
  for (const CatalogAttribute &attr : relation_) { 
        num_codes_per_word_[attr.getID()] = (sizeof(WordUnit)<<3)/((attr.getType().maximumByteLength()<<3)+1);
     	//std::cout << "current attr is " << attr.getID() << std::endl;
        //std::cout << "attr length is " << attr.getType().maximumByteLength()*8 + 1 << std::endl; 
	num_bits_per_code_[attr.getID()] = attr.getType().maximumByteLength()*8 + 1;
        if(num_codes_per_word_[attr.getID()] == 0)
	{
		num_words_per_code_[attr.getID()] = ((attr.getType().maximumByteLength()<<3)+1)/(sizeof(WordUnit)<<3) + 1;
     		num_codes_per_segment_[attr.getID()] =  (attr.getType().maximumByteLength()<<3); //some sort of hard coding for now
     		num_padding_bits_[attr.getID()] = (sizeof(WordUnit)<<3) * num_words_per_code_[attr.getID()] - (attr.getType().maximumByteLength()<<3);   
		size += num_words_per_code_[attr.getID()];	
	}
	else
	{
                num_words_per_code_[attr.getID()] = 1;
                num_codes_per_segment_[attr.getID()] = num_codes_per_word_[attr.getID()] *(attr.getType().maximumByteLength()<<3); 
                num_padding_bits_[attr.getID()] = (sizeof(WordUnit)<<3) - num_codes_per_word_[attr.getID()] * (attr.getType().maximumByteLength()<<3);   
		size += 1;
	}
  //std::cout << "#word/code for attr " << attr.getID() << " is " << num_words_per_code_[attr.getID()] << std::endl;
  //std::cout << "#codes/word for attr " << attr.getID() << " is " << num_codes_per_word_[attr.getID()] << std::endl;
  //std::cout << "#codes/segment for attr " << attr.getID() << " is " << num_codes_per_segment_[attr.getID()] << std::endl;
  //std::cout << "#of padding bits" << num_padding_bits_[attr.getID()] << std::endl;	 
  num_words_per_segment_[attr.getID()] = ((attr.getType().maximumByteLength() << 3) * num_words_per_code_[attr.getID()]); 
  //std::cout << "#word/segment for attr " << attr.getID() << " is " << num_words_per_segment_[attr.getID()] << std::endl;
	 // row_array_[attr.getID()] = max_tuples_ / col_array_[attr.getID()];
  }
	  //std::cout << "this loop 2" << num_codes_per_word_.size() << std::endl;
  //std::cout << "total size of a single tuple is " << size << std::endl;


  // Determine the amount of tuples this sub-block can hold. Compute on the
  // order of bits to account for null bitmap storage.
  max_tuples_ = ((sub_block_memory_size_ - sizeof(BWColumnStoreHeader)) << 3)
                / ((size << 3) + relation_.numNullableAttributes());
  if (max_tuples_ == 0) {
    throw BlockMemoryTooSmall("BWColumnStoreTupleStorageSubBlock", sub_block_memory_size_);
  }

  // BitVector's storage requirements "round up" to sizeof(size_t), so now redo
  // the calculation accurately.
  std::size_t bitmap_required_bytes = BitVector<false>::BytesNeeded(max_tuples_);
  max_tuples_ = (sub_block_memory_size_
                     - sizeof(BWColumnStoreHeader)
                     - (relation_.numNullableAttributes() * bitmap_required_bytes))
                / relation_.getFixedByteLength();
  if (max_tuples_ == 0) {
    throw BlockMemoryTooSmall("BWColumnStoreTupleStorageSubBlock", sub_block_memory_size_);
  }
  bitmap_required_bytes = BitVector<false>::BytesNeeded(max_tuples_);

  {
    //std::cout << "Constructing the bw class" << ", Max tuples: " << max_tuples_ << std::endl;
    //std::size_t header_size = getHeaderSize();
    //initialize(new_block,
    //           static_cast<char*>(sub_block_memory) + header_size,
    //           sub_block_memory_size - header_size);
    // BWColumnStoreHeader?????? TODO
  }

  // Allocate memory for this sub-block's structures, starting with the header.
  char *memory_location = static_cast<char*>(sub_block_memory_) + sizeof(BWColumnStoreHeader);

  // Per-column NULL bitmaps.
  for (attribute_id attr_id = 0;
       attr_id <= relation_.getMaxAttributeId();
       ++attr_id) {
    if (relation_.hasAttributeWithId(attr_id)
        && relation_.getAttributeById(attr_id)->getType().isNullable()) {
      column_null_bitmaps_.push_back(
          new BitVector<false>(memory_location, max_tuples_));
      if (new_block) {
        column_null_bitmaps_.back().clear();
      }
      memory_location += bitmap_required_bytes;
    } else {
      column_null_bitmaps_.push_back(nullptr);
    }
  }

  // Column stripes.
  column_stripes_.resize(relation_.getMaxAttributeId() +  1, nullptr);
  for (const CatalogAttribute &attr : relation_) {
    
    column_stripes_[attr.getID()] = memory_location;
    memory_location += max_tuples_ * attr.getType().maximumByteLength();
	  //std::cout << "this loop1" <<std::endl;
  }/*
  //col and row for column_stripes
  num_codes_per_word_.resize(relation_.getMaxAttributeId() + 1);
  num_padding_bits_.resize(relation_.getMaxAttributeId() + 1); 
  num_codes_per_segment_.resize(relation_.getMaxAttributeId() + 1);
  num_words_per_segment_.resize(relation_.getMaxAttributeId() + 1);
  num_words_per_code_.resize(relation_.getMaxAttributeId() + 1);

  std::cout << "wordunit is " << (sizeof(WordUnit)<<3) << std::endl; 
  for (const CatalogAttribute &attr : relation_) { 
        num_codes_per_word_[attr.getID()] = (sizeof(WordUnit)<<3)/((attr.getType().maximumByteLength()<<3)+1);
     	std::cout << "current attr is " << attr.getID() << std::endl;
        std::cout << "attr length is " << attr.getType().maximumByteLength()<<3 + 1 << std::endl; 
	if(num_codes_per_word_[attr.getID()] == 0)
	{
		num_words_per_code_[attr.getID()] = ((attr.getType().maximumByteLength()<<3)+1)/(sizeof(WordUnit)<<3) + 1;
     		num_codes_per_segment_[attr.getID()] =  (attr.getType().maximumByteLength()<<3); //some sort of hard coding for now
     		num_padding_bits_[attr.getID()] = (sizeof(WordUnit)<<3) * num_words_per_code_[attr.getID()] - (attr.getType().maximumByteLength()<<3);   
		
	}
	else
	{
                num_words_per_code_[attr.getID()] = 1;
                num_codes_per_segment_[attr.getID()] = num_codes_per_word_[attr.getID()] *(attr.getType().maximumByteLength()<<3); 
                num_padding_bits_[attr.getID()] = (sizeof(WordUnit)<<3) - num_codes_per_word_[attr.getID()] * (attr.getType().maximumByteLength()<<3);   
	}
  std::cout << "#word/code for attr " << attr.getID() << " is " << num_words_per_code_[attr.getID()] << std::endl;
  std::cout << "#codes/word for attr " << attr.getID() << " is " << num_codes_per_word_[attr.getID()] << std::endl;
  std::cout << "#codes/segment for attr " << attr.getID() << " is " << num_codes_per_segment_[attr.getID()] << std::endl;
  std::cout << "#of padding bits" << num_padding_bits_[attr.getID()] << std::endl;	 
  num_words_per_segment_[attr.getID()] = ((attr.getType().maximumByteLength() << 3) * num_words_per_code_[attr.getID()]); 
  std::cout << "#word/segment for attr " << attr.getID() << " is " << num_words_per_segment_[attr.getID()] << std::endl;
	 // row_array_[attr.getID()] = max_tuples_ / col_array_[attr.getID()];
  }
	  std::cout << "this loop 2" << num_codes_per_word_.size() << std::endl;

*/

	
	  //std::cout << "here 1" <<std::endl;

	  DEBUG_ASSERT(memory_location
		       <= static_cast<const char*>(sub_block_memory_) + sub_block_memory_size_);
	  //std::cout << "here 2" <<std::endl;

	  if (new_block) {
	    header_->num_tuples = 0;
	    header_->nulls_in_sort_column = 0;
	  }
	}

	bool BWColumnStoreTupleStorageSubBlock::DescriptionIsValid(
	    const CatalogRelationSchema &relation,
	    const TupleStorageSubBlockDescription &description) {
	  // Make sure description is initialized and specifies BWColumnStore.
	  //std::cout << "Get in checking Description!!!!!!" <<std::endl;
	  if (!description.IsInitialized()) {
	    return false;
	  }
	  if (description.sub_block_type() != TupleStorageSubBlockDescription::BW_COLUMN_STORE) {
	    return false;
	  }

	  // Make sure relation is not variable-length.
	  if (relation.isVariableLength()) {
	    return false;
	  }

	  // If a sort attribute is specified, check that it exists and can be ordered
	  // by LessComparison.
	  if (description.HasExtension(
		  BWColumnStoreTupleStorageSubBlockDescription::sort_attribute_id)) {
	    const attribute_id sort_attribute_id = description.GetExtension(
		BWColumnStoreTupleStorageSubBlockDescription::sort_attribute_id);
	    if (!relation.hasAttributeWithId(sort_attribute_id)) {
	      return false;
	    }
	    const Type &sort_attribute_type =
		relation.getAttributeById(sort_attribute_id)->getType();
	    if (!ComparisonFactory::GetComparison(ComparisonID::kLess).canCompareTypes(
		    sort_attribute_type, sort_attribute_type)) {
	      return false;
	    }
	  }

	  return true;
	}

	std::size_t BWColumnStoreTupleStorageSubBlock::EstimateBytesPerTuple(
	    const CatalogRelationSchema &relation,
	    const TupleStorageSubBlockDescription &description) {
	  DEBUG_ASSERT(DescriptionIsValid(relation, description));

	  // NOTE(chasseur): We round-up the number of bytes needed in the NULL bitmaps
	  // to avoid estimating 0 bytes needed for a relation with less than 8
	  // attributes which are all NullType.
	  return relation.getFixedByteLength()
		 + ((relation.numNullableAttributes() + 7) >> 3);
	}

	TupleStorageSubBlock::InsertResult BWColumnStoreTupleStorageSubBlock::insertTuple(
	    const Tuple &tuple) {
	#ifdef QUICKSTEP_DEBUG
	  paranoidInsertTypeCheck(tuple);
	#endif
	  if (!hasSpaceToInsert(1)) {
	    return InsertResult(-1, false);
	  }

	  tuple_id insert_position = header_->num_tuples;
	  // If this column store is unsorted, or the value of the sort column is NULL,
	  // skip the search and put the new tuple at the end of everything else.
	  if (sort_specified_ && !tuple.getAttributeValue(sort_column_id_).isNull()) {
	    // Binary search for the appropriate insert location.
	    ColumnStripeIterator begin_it(column_stripes_[sort_column_id_],
					  relation_.getAttributeById(sort_column_id_)->getType().maximumByteLength(),
					  0);
	    ColumnStripeIterator end_it(column_stripes_[sort_column_id_],
					relation_.getAttributeById(sort_column_id_)->getType().maximumByteLength(),
                                header_->num_tuples - header_->nulls_in_sort_column);
    insert_position = GetBoundForUntypedValue<ColumnStripeIterator, UpperBoundFunctor>(
        *sort_column_type_,
        begin_it,
        end_it,
        tuple.getAttributeValue(sort_column_id_).getDataPtr()).getTuplePosition();
  }

  InsertResult retval(insert_position, insert_position != header_->num_tuples);
  insertTupleAtPosition(tuple, insert_position);

  return retval;
}

bool BWColumnStoreTupleStorageSubBlock::insertTupleInBatch(const Tuple &tuple) {
#ifdef QUICKSTEP_DEBUG
  paranoidInsertTypeCheck(tuple);
#endif
  if (!hasSpaceToInsert(1)) {
    return false;
  }

  insertTupleAtPosition(tuple, header_->num_tuples);
  sorted_ = false;
  return true;
}

tuple_id BWColumnStoreTupleStorageSubBlock::bulkInsertTuples(ValueAccessor *accessor) {
  const tuple_id original_num_tuples = header_->num_tuples;
  //std::cout << "Get in bulkInsertTuples!" << "Get Here11!" << std::endl;
  InvokeOnAnyValueAccessor(
      accessor,
      [&](auto *accessor) -> void {  // NOLINT(build/c++11)
    if (relation_.hasNullableAttributes()) {
      while (this->hasSpaceToInsert(1) && accessor->next()) {
        attribute_id accessor_attr_id = 0;
        // TODO(chasseur): Column-wise copy is probably preferable to
        // tuple-at-a-time, but will require some API changes.
        for (CatalogRelationSchema::const_iterator attr_it = relation_.begin();
             attr_it != relation_.end();
             ++attr_it) {
  //std::cout << "attr_it is " << attr_it->getID() <<  std::endl;
          const attribute_id attr_id = attr_it->getID();
          const std::size_t attr_size = attr_it->getType().maximumByteLength();
          if (attr_it->getType().isNullable()) {
            const void *attr_value
                = accessor->template getUntypedValue<true>(accessor_attr_id);
  //std::cout << "attr_value is " << attr_value << std::endl;
            if (attr_value == nullptr) {
              column_null_bitmaps_[attr_id].setBit(header_->num_tuples, true);
            } else {
              memcpy(static_cast<char*>(column_stripes_[attr_id])
                         + header_->num_tuples * attr_size,
                     attr_value,
                     attr_size);
            }
          } else {
  //std::cout << "else attr value is " << accessor->template getUntypedValue<false>(accessor_attr_id) << std::endl;
            memcpy(static_cast<char*>(column_stripes_[attr_id])
                       + header_->num_tuples * attr_size,
                   accessor->template getUntypedValue<false>(accessor_attr_id),
                   attr_size);
          }
          ++accessor_attr_id;
        }
        ++(header_->num_tuples);
      }
    } else {
      while (this->hasSpaceToInsert(1) && accessor->next()) {
        attribute_id accessor_attr_id = 0;
        for (CatalogRelationSchema::const_iterator attr_it = relation_.begin();
             attr_it != relation_.end();
             ++attr_it) {
          const std::size_t attr_size = attr_it->getType().maximumByteLength();
  	  /*std::cout << "attr_it->getID()" <<attr_it->getID() << std::endl;
  	  std::cout << "column stripe " << column_stripes_[attr_it->getID()] << std::endl;
  	  std::cout << "offset" << header_->num_tuples << std::endl;
  	  std::cout << "attr size" << attr_size << std::endl;
  	  std::cout << "accessor id" << accessor_attr_id << std::endl;
	  */num_tuple_in_current_segment = header_->num_tuples % num_codes_per_segment_[attr_it->getID()];
          nth_segment = header_->num_tuples / num_codes_per_segment_[attr_it->getID()];
	  if(num_codes_per_word_[attr_it->getID()] != 0)
		{
		// int row_size = ((attr_it->getType().maximumByteLength()<<3)*num_words_per_code_[attr_it->getID()]);
	 	// std::cout << "row size for this attr's segment is " << row_size << std::endl;
           	// col = num_tuple_in_current_segment / row_size;
           	 row = num_tuple_in_current_segment;
		}
	  else
		{
	        // col = 0;
		 row = num_tuple_in_current_segment;
		}		
  		 //std::cout << "offset1 is  " << col * attr_size + row * sizeof(WordUnit) * num_words_per_code_[attr_it->getID()] << std::endl;
 		 /*std::cout << "offset2 is  " << nth_segment * num_words_per_segment_[attr_it->getID()] << std::endl;
 	 	 std::cout << "addr is " << (column_stripes_[attr_it->getID()]
                  + col * attr_size + row * sizeof(WordUnit) * num_words_per_code_[attr_it->getID()]
		   + nth_segment * num_words_per_segment_[attr_it->getID()]) << std::endl;
 	         */
          memcpy(static_cast<char*>(column_stripes_[attr_it->getID()]
                   +  row * (sizeof(WordUnit)<<3) * num_words_per_code_[attr_it->getID()]
		   + nth_segment * num_words_per_segment_[attr_it->getID()] * (sizeof(WordUnit)<<3)),
                 accessor->template getUntypedValue<false>(accessor_attr_id),
                 attr_size);
          ++accessor_attr_id;
        }
	//print table
	 /*
  	//std::cout << "max # tuple " << max_tuples_ << std::endl;
	for(int  i=0; i < header_->num_tuples; i++)
	{
		for(CatalogRelationSchema::const_iterator attr_it = relation_.begin(); attr_it != relation_.end(); ++attr_it)
		{
	  	 num_tuple_in_current_segment = i  % num_codes_per_segment_[attr_it->getID()];
          	 nth_segment = i  / num_codes_per_segment_[attr_it->getID()];
			
           	 //std::cout << "item#     " << i << std::endl;
  		 //std::cout << "attr#     " <<attr_it->getID() << std::endl;
		 if(num_codes_per_word_[attr_it->getID()] != 0)
		 {
		 	int row_size = ((attr_it->getType().maximumByteLength()<<3)*num_words_per_code_[attr_it->getID()]);
		 	col = i / row_size;
                 	row = i  % row_size;
  		 }
		 else
		 {
			col = 0;
			row = num_tuple_in_current_segment; 
		 }
		 		
                 const std::size_t attr_size = attr_it->getType().maximumByteLength();
		 std::cout << "col is     " << col << "row is " << row << std::endl;
  		 std::cout << "offset1 is  " << col * attr_size + row * sizeof(WordUnit) * num_words_per_code_[attr_it->getID()] << std::endl;
 		 std::cout << "offset2 is  " << nth_segment * num_words_per_segment_[attr_it->getID()] << std::endl;
	   	 std::cout << "addr is  "  <<(column_stripes_[attr_it->getID()] + 
			col * attr_size + row * sizeof(WordUnit) * num_words_per_code_[attr_it->getID()]
			+ nth_segment * num_words_per_segment_[attr_it->getID()]) << std::endl;
  		 std::cout << "column value is   " <<static_cast<char*>( column_stripes_[attr_it->getID()] + 
			col * attr_size + row * sizeof(WordUnit) * num_words_per_code_[attr_it->getID()]
			+ nth_segment * num_words_per_segment_[attr_it->getID()]) << std::endl;
		
		}
	}
        */
        ++(header_->num_tuples);
      }
    }
  });
  
  const tuple_id num_inserted = header_->num_tuples - original_num_tuples;
  sorted_ = sorted_ && (num_inserted == 0);
  return num_inserted;
}

tuple_id BWColumnStoreTupleStorageSubBlock::bulkInsertTuplesWithRemappedAttributes(
    const std::vector<attribute_id> &attribute_map,
    ValueAccessor *accessor) {
  DEBUG_ASSERT(attribute_map.size() == relation_.size());
  const tuple_id original_num_tuples = header_->num_tuples;

  InvokeOnAnyValueAccessor(
      accessor,
      [&](auto *accessor) -> void {  // NOLINT(build/c++11)
    if (relation_.hasNullableAttributes()) {
      while (this->hasSpaceToInsert(1) && accessor->next()) {
        std::vector<attribute_id>::const_iterator attribute_map_it = attribute_map.begin();
        // TODO(chasseur): Column-wise copy is probably preferable to
        // tuple-at-a-time, but will require some API changes.
        for (CatalogRelationSchema::const_iterator attr_it = relation_.begin();
             attr_it != relation_.end();
             ++attr_it) {
          const attribute_id attr_id = attr_it->getID();
          const std::size_t attr_size = attr_it->getType().maximumByteLength();
          if (attr_it->getType().isNullable()) {
            const void *attr_value
                = accessor->template getUntypedValue<true>(*attribute_map_it);
            if (attr_value == nullptr) {
              column_null_bitmaps_[attr_id].setBit(header_->num_tuples, true);
            } else {
              memcpy(static_cast<char*>(column_stripes_[attr_id])
                         + header_->num_tuples * attr_size,
                     attr_value,
                     attr_size);
            }
          } else {
            memcpy(static_cast<char*>(column_stripes_[attr_id])
                       + header_->num_tuples * attr_size,
                   accessor->template getUntypedValue<false>(*attribute_map_it),
                   attr_size);
          }
          ++attribute_map_it;
        }
        ++(header_->num_tuples);
      }
    } else {
      while (this->hasSpaceToInsert(1) && accessor->next()) {
        std::vector<attribute_id>::const_iterator attribute_map_it = attribute_map.begin();
        for (CatalogRelationSchema::const_iterator attr_it = relation_.begin();
             attr_it != relation_.end();
             ++attr_it) {
          const std::size_t attr_size = attr_it->getType().maximumByteLength();
          memcpy(static_cast<char*>(column_stripes_[attr_it->getID()])
                     + header_->num_tuples * attr_size,
                 accessor->template getUntypedValue<false>(*attribute_map_it),
                 attr_size);
          ++attribute_map_it;
        }
        ++(header_->num_tuples);
      }
    }
  });

  const tuple_id num_inserted = header_->num_tuples - original_num_tuples;
  sorted_ = sorted_ && (num_inserted == 0);
  return num_inserted;
}

const void* BWColumnStoreTupleStorageSubBlock::getAttributeValue(
    const tuple_id tuple,
    const attribute_id attr) const {
  DEBUG_ASSERT(hasTupleWithID(tuple));
  DEBUG_ASSERT(relation_.hasAttributeWithId(attr));
  //std::cout << "Try to get attribute " << attr << " of tuple " << tuple << std::endl;
  if ((!column_null_bitmaps_.elementIsNull(attr))
      && column_null_bitmaps_[attr].getBit(tuple)) {
    return nullptr;
  }

  return static_cast<const char*>(column_stripes_[attr])
         + (tuple * relation_.getAttributeById(attr)->getType().maximumByteLength());
}

TypedValue BWColumnStoreTupleStorageSubBlock::getAttributeValueTyped(
    const tuple_id tuple,
    const attribute_id attr) const {   //override?

  //std::cout << "get attribute value typed from BWColumnStore" << std::endl;
  const Type &attr_type = relation_.getAttributeById(attr)->getType();
  const void *untyped_value = getAttributeValue(tuple, attr);
  return (untyped_value == nullptr)
      ? attr_type.makeNullValue()
      : attr_type.makeValue(untyped_value, attr_type.maximumByteLength());
}

ValueAccessor* BWColumnStoreTupleStorageSubBlock::createValueAccessor(
    const TupleIdSequence *sequence) const {
  //std::cout << "---- create a value accessor for BWsubblock" << std::endl;
  BWColumnStoreValueAccessor *base_accessor
      = new BWColumnStoreValueAccessor(relation_,
                                          relation_,
                                          header_->num_tuples,
                                          column_stripes_,
                                          column_null_bitmaps_);
  if (sequence == nullptr) {
    return base_accessor;
  } else {
    return new TupleIdSequenceAdapterValueAccessor<BWColumnStoreValueAccessor>(
        base_accessor,
        *sequence);
  }
}

bool BWColumnStoreTupleStorageSubBlock::canSetAttributeValuesInPlaceTyped(
    const tuple_id tuple,
    const std::unordered_map<attribute_id, TypedValue> &new_values) const {
  DEBUG_ASSERT(hasTupleWithID(tuple));
  if (!sort_specified_) {
    return true;
  }
  for (std::unordered_map<attribute_id, TypedValue>::const_iterator it
           = new_values.begin();
       it != new_values.end();
       ++it) {
    DEBUG_ASSERT(relation_.hasAttributeWithId(it->first));
    // TODO(chasseur): Could check if the new value for sort column would
    // remain in the same position in the stripe.
    if (it->first == sort_column_id_) {
      return false;
    }
  }
  return true;
}

void BWColumnStoreTupleStorageSubBlock::setAttributeValueInPlaceTyped(
    const tuple_id tuple,
    const attribute_id attr,
    const TypedValue &value) {
  DCHECK(!sort_specified_ || (attr != sort_column_id_));

  const Type &attr_type = relation_.getAttributeById(attr)->getType();
  void *value_position = static_cast<char*>(column_stripes_[attr])
                         + tuple * attr_type.maximumByteLength();
  if (!column_null_bitmaps_.elementIsNull(attr)) {
    if (value.isNull()) {
      column_null_bitmaps_[attr].setBit(tuple, true);
      return;
    } else {
      column_null_bitmaps_[attr].setBit(tuple, false);
    }
  }

  value.copyInto(value_position);
}

bool BWColumnStoreTupleStorageSubBlock::deleteTuple(const tuple_id tuple) {
  DEBUG_ASSERT(hasTupleWithID(tuple));

  if (sort_specified_
      && !column_null_bitmaps_.elementIsNull(sort_column_id_)
      && column_null_bitmaps_[sort_column_id_].getBit(tuple)) {
    --(header_->nulls_in_sort_column);
  }

  if (tuple == header_->num_tuples - 1) {
    // If deleting the last tuple, simply truncate.
    --(header_->num_tuples);

    // Clear any null bits for the tuple.
    for (PtrVector<BitVector<false>, true>::iterator it = column_null_bitmaps_.begin();
         it != column_null_bitmaps_.end();
         ++it) {
      if (!it.isNull()) {
        it->setBit(header_->num_tuples, false);
      }
    }

    return false;
  } else {
    // Pack each column stripe.
    shiftTuples(tuple, tuple + 1, header_->num_tuples - tuple - 1);
    shiftNullBitmaps(tuple, -1);
    --(header_->num_tuples);
    return true;
  }
}

bool BWColumnStoreTupleStorageSubBlock::bulkDeleteTuples(TupleIdSequence *tuples) {
  if (tuples->empty()) {
    // Nothing to do.
    return false;
  }

  const tuple_id front = tuples->front();
  const tuple_id back = tuples->back();
  const tuple_id num_tuples = tuples->numTuples();

  if ((back == header_->num_tuples - 1)
       && (back - front == num_tuples - 1)) {
    // Just truncate the back.
    header_->num_tuples = front;

    // Clear any null bits.
    for (PtrVector<BitVector<false>, true>::iterator it = column_null_bitmaps_.begin();
         it != column_null_bitmaps_.end();
         ++it) {
      if (!it.isNull()) {
        it->setBitRange(header_->num_tuples, num_tuples, false);
      }
    }

    return false;
  }

  // Pack the non-deleted tuples.
  tuple_id dest_position = front;
  tuple_id src_tuple = dest_position;
  TupleIdSequence::const_iterator it = tuples->begin();
  tuple_id tail_shifted_distance = 0;
  for (tuple_id current_id = tuples->front();
       current_id < header_->num_tuples;
       ++current_id, ++src_tuple) {
    if (current_id == *it) {
      // Don't copy a deleted tuple.
      shiftNullBitmaps(*it - tail_shifted_distance, -1);
      ++tail_shifted_distance;
      ++it;
      if (it == tuples->end()) {
        // No more to delete, so copy all the remaining tuples in one go.
        shiftTuples(dest_position,
                    src_tuple + 1,
                    header_->num_tuples - back - 1);
        break;
      }
    } else {
      // Shift the next tuple forward.
      shiftTuples(dest_position, src_tuple, 1);
      ++dest_position;
    }
  }

  header_->num_tuples -= static_cast<tuple_id>(num_tuples);

  return true;
}

predicate_cost_t BWColumnStoreTupleStorageSubBlock::estimatePredicateEvaluationCost(
    const ComparisonPredicate &predicate) const {
  //std::cout << "Get in estimateEvaluationCost" << std::endl;
  if (sort_specified_ && predicate.isAttributeLiteralComparisonPredicate()) {
    std::pair<bool, attribute_id> comparison_attr = predicate.getAttributeFromAttributeLiteralComparison();
    if (comparison_attr.second == sort_column_id_) {
      return predicate_cost::kBinarySearch;
    }
  }
  return predicate_cost::kBitWeavingHScan;
}

/*TupleIdSequence* BWColumnStoreTupleStorageSubBlock::getMatchesForPredicate(
    const ComparisonPredicate &predicate,
    const TupleIdSequence *filter) const {
  std::cout << "------ Get in getMatchesForPredicate of BW" << std::endl;

  DCHECK(sort_specified_) <<
      "Called BWColumnStoreTupleStorageSubBlock::getMatchesForPredicate() "
      "for an unsorted column store (predicate should have been evaluated "
      "with a scan instead).";

  TupleIdSequence *matches = SortColumnPredicateEvaluator::EvaluatePredicateForUncompressedSortColumn(
      predicate,
      relation_,
      sort_column_id_,
      column_stripes_[sort_column_id_],
      header_->num_tuples - header_->nulls_in_sort_column);

  if (matches == nullptr) {
    FATAL_ERROR("Called BWColumnStoreTupleStorageSubBlock::getMatchesForPredicate() "
                "with a predicate that can only be evaluated with a scan.");
  } else {
    if (filter != nullptr) {
      matches->intersectWith(*filter);
    }
    return matches;
  }
}
*/
void BWColumnStoreTupleStorageSubBlock::insertTupleAtPosition(
    const Tuple &tuple,
    const tuple_id position) {
  DEBUG_ASSERT(hasSpaceToInsert(1));
  DEBUG_ASSERT(position >= 0);
  DEBUG_ASSERT(position < max_tuples_);

  if (position != header_->num_tuples) {
    // If not inserting in the last position, shift subsequent tuples back.
    shiftTuples(position + 1, position, header_->num_tuples - position);
    shiftNullBitmaps(position, 1);
  }

  // Copy attribute values into place in the column stripes.
  Tuple::const_iterator value_it = tuple.begin();
  CatalogRelationSchema::const_iterator attr_it = relation_.begin();

  while (value_it != tuple.end()) {
    const attribute_id attr_id = attr_it->getID();
    const Type &attr_type = attr_it->getType();
    if (value_it->isNull()) {
      column_null_bitmaps_[attr_id].setBit(position, true);
      if (attr_id == sort_column_id_) {
        ++(header_->nulls_in_sort_column);

        // Copy in a special value that always compares last.
        GetLastValueForType(attr_type).copyInto(
            static_cast<char*>(column_stripes_[attr_id])
                + position * attr_type.maximumByteLength());
      }
    } else {
      value_it->copyInto(static_cast<char*>(column_stripes_[attr_id])
                         + position * attr_type.maximumByteLength());
    }

    ++value_it;
    ++attr_it;
  }

  ++(header_->num_tuples);
}

void BWColumnStoreTupleStorageSubBlock::shiftTuples(
    const tuple_id dest_position,
    const tuple_id src_tuple,
    const tuple_id num_tuples) {
  for (CatalogRelationSchema::const_iterator attr_it = relation_.begin();
       attr_it != relation_.end();
       ++attr_it) {
    size_t attr_length = attr_it->getType().maximumByteLength();
    memmove(static_cast<char*>(column_stripes_[attr_it->getID()]) + dest_position * attr_length,
            static_cast<const char*>(column_stripes_[attr_it->getID()]) + src_tuple * attr_length,
            attr_length * num_tuples);
  }
}

void BWColumnStoreTupleStorageSubBlock::shiftNullBitmaps(
    const tuple_id from_position,
    const tuple_id distance) {
  if (relation_.hasNullableAttributes()) {
    for (PtrVector<BitVector<false>, true>::size_type idx = 0;
         idx < column_null_bitmaps_.size();
         ++idx) {
      if (!column_null_bitmaps_.elementIsNull(idx)) {
        if (distance < 0) {
          column_null_bitmaps_[idx].shiftTailForward(from_position, -distance);
        } else {
          column_null_bitmaps_[idx].shiftTailBackward(from_position, distance);
        }
      }
    }
  }
}

// TODO(chasseur): This implementation uses out-of-band memory up to the
// total size of tuples contained in this sub-block. It could be done with
// less memory, although the implementation would be more complex.
bool BWColumnStoreTupleStorageSubBlock::rebuildInternal() {
  DCHECK(sort_specified_);

  const tuple_id num_tuples = header_->num_tuples;
  // Immediately return if 1 or 0 tuples.
  if (num_tuples <= 1) {
    sorted_ = true;
    return false;
  }

  // Determine the properly-sorted order of tuples.
  vector<SortColumnValueReference> sort_column_values;
  const char *sort_value_position
      = static_cast<const char*>(column_stripes_[sort_column_id_]);
  const std::size_t sort_value_size = sort_column_type_->maximumByteLength();
  for (tuple_id tid = 0; tid < num_tuples; ++tid) {
    // NOTE(chasseur): The insert methods put a special last-comparing
    // substitute value for NULLs into the sorted column stripe.
    sort_column_values.emplace_back(sort_value_position, tid);
    sort_value_position += sort_value_size;
  }
  SortWrappedValues<SortColumnValueReference, vector<SortColumnValueReference>::iterator>(
      *sort_column_type_,
      sort_column_values.begin(),
      sort_column_values.end());

  if (header_->nulls_in_sort_column > 0) {
    // If any NULL values exist in the sort column, make sure they are ordered
    // after all the "real" values which compare as the same.
    vector<SortColumnValueReference>::iterator tail_it =
        GetBoundForWrappedValues<SortColumnValueReference,
                                 vector<SortColumnValueReference>::iterator,
                                 LowerBoundFunctor>(
            *sort_column_type_,
            sort_column_values.begin(),
            sort_column_values.end(),
            GetLastValueForType(*sort_column_type_).getDataPtr());

    if (sort_column_values.end() - tail_it > header_->nulls_in_sort_column) {
      vector<SortColumnValueReference> non_nulls;
      vector<SortColumnValueReference> true_nulls;
      for (vector<SortColumnValueReference>::const_iterator tail_sort_it = tail_it;
           tail_sort_it != sort_column_values.end();
           ++tail_sort_it) {
        if (column_null_bitmaps_[sort_column_id_].getBit(tail_sort_it->getTupleID())) {
          true_nulls.push_back(*tail_sort_it);
        } else {
          non_nulls.push_back(*tail_sort_it);
        }
      }

      sort_column_values.erase(tail_it, sort_column_values.end());
      sort_column_values.insert(sort_column_values.end(),
                                non_nulls.begin(),
                                non_nulls.end());
      sort_column_values.insert(sort_column_values.end(),
                                true_nulls.begin(),
                                true_nulls.end());
    }
  }

  // If a prefix of the total order of tuples is already sorted, don't bother
  // copying it around.
  tuple_id ordered_prefix_tuples = 0;
  for (vector<SortColumnValueReference>::const_iterator it = sort_column_values.begin();
       it != sort_column_values.end();
       ++it) {
    if (it->getTupleID() != ordered_prefix_tuples) {
      break;
    }
    ++ordered_prefix_tuples;
  }

  if (ordered_prefix_tuples == num_tuples) {
    // Already sorted.
    sorted_ = true;
    return false;
  }

  // Allocate buffers for each resorted column stripe which can hold exactly as
  // many values as needed.
  PtrVector<ScopedBuffer, true> column_stripe_buffers;
  PtrVector<ScopedBuffer, true> null_bitmap_buffers;
  PtrVector<BitVector<false>, true> new_null_bitmaps;

  const std::size_t bitmap_required_bytes = BitVector<false>::BytesNeeded(max_tuples_);
  char *bitmap_location = static_cast<char*>(sub_block_memory_)
                          + sizeof(BWColumnStoreHeader);

  for (attribute_id stripe_id = 0; stripe_id <= relation_.getMaxAttributeId(); ++stripe_id) {
    if (relation_.hasAttributeWithId(stripe_id)) {
      column_stripe_buffers.push_back(
          new ScopedBuffer((num_tuples - ordered_prefix_tuples)
                           * relation_.getAttributeById(stripe_id)->getType().maximumByteLength()));
      if (column_null_bitmaps_.elementIsNull(stripe_id)) {
        null_bitmap_buffers.push_back(nullptr);
        new_null_bitmaps.push_back(nullptr);
      } else {
        // Make a copy of the null bitmap for this column.
        null_bitmap_buffers.push_back(new ScopedBuffer(bitmap_required_bytes));
        new_null_bitmaps.push_back(new BitVector<false>(null_bitmap_buffers.back().get(), max_tuples_));
        bitmap_location += bitmap_required_bytes;

        // Clear out the tail, which will be reorganized.
        if (ordered_prefix_tuples > 0) {
          memcpy(null_bitmap_buffers.back().get(), bitmap_location, bitmap_required_bytes);
          new_null_bitmaps.back().setBitRange(ordered_prefix_tuples,
                                              num_tuples - ordered_prefix_tuples,
                                              false);
        } else {
          new_null_bitmaps.back().clear();
        }
      }
    } else {
      column_stripe_buffers.push_back(nullptr);
      null_bitmap_buffers.push_back(nullptr);
      new_null_bitmaps.push_back(nullptr);
    }
  }

  // Copy attribute values into the column stripe buffers in properly-sorted
  // order.
  for (CatalogRelationSchema::const_iterator attr_it = relation_.begin();
       attr_it != relation_.end();
       ++attr_it) {
    const size_t attr_length = attr_it->getType().maximumByteLength();
    const attribute_id attr_id = attr_it->getID();
    for (tuple_id unordered_tuple_num = 0;
         unordered_tuple_num < num_tuples - ordered_prefix_tuples;
         ++unordered_tuple_num) {
      memcpy(static_cast<char*>(column_stripe_buffers[attr_id].get())
                 + unordered_tuple_num * attr_length,
             static_cast<char*>(column_stripes_[attr_id]) +
                 sort_column_values[ordered_prefix_tuples + unordered_tuple_num].getTupleID() * attr_length,
             attr_length);
    }

    if (!new_null_bitmaps.elementIsNull(attr_id)) {
      for (tuple_id tuple_num = ordered_prefix_tuples;
           tuple_num < num_tuples;
           ++tuple_num) {
        new_null_bitmaps[attr_id].setBit(
            tuple_num,
            column_null_bitmaps_[attr_id].getBit(sort_column_values[tuple_num].getTupleID()));
      }
    }
  }

  // Overwrite the unsorted tails of the column stripes in this block with the
  // sorted values from the buffers. Also copy back any null bitmaps.
  bitmap_location = static_cast<char*>(sub_block_memory_)
                    + sizeof(BWColumnStoreHeader);
  for (CatalogRelationSchema::const_iterator attr_it = relation_.begin();
       attr_it != relation_.end();
       ++attr_it) {
    size_t attr_length = attr_it->getType().maximumByteLength();
    memcpy(static_cast<char*>(column_stripes_[attr_it->getID()]) + ordered_prefix_tuples * attr_length,
           column_stripe_buffers[attr_it->getID()].get(),
           (num_tuples - ordered_prefix_tuples) * attr_length);

    if (!null_bitmap_buffers.elementIsNull(attr_it->getID())) {
      std::memcpy(bitmap_location,
                  null_bitmap_buffers[attr_it->getID()].get(),
                  bitmap_required_bytes);
      bitmap_location += bitmap_required_bytes;
    }
  }

  sorted_ = true;
  return true;
}

}  // namespace quickstep
