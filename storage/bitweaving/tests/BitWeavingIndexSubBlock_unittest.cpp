/**
 *   Copyright 2013-2016, Quickstep Research Group, Computer Sciences Department,
 *     University of Wisconsin—Madison.
 **/

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "glog/logging.h"

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogRelation.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "expressions/predicate/ComparisonPredicate.hpp"
#include "expressions/scalar/Scalar.hpp"
#include "expressions/scalar/ScalarAttribute.hpp"
#include "expressions/scalar/ScalarLiteral.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/StorageErrors.hpp"
#include "storage/StorageManager.hpp"
#include "storage/TupleIdSequence.hpp"
#include "storage/bitweaving/BitWeavingHIndexSubBlock.hpp"
#include "storage/bitweaving/BitWeavingIndexSubBlock.hpp"
#include "storage/bitweaving/BitWeavingVIndexSubBlock.hpp"
#include "storage/tests/MockTupleStorageSubBlock.hpp"
#include "types/CharType.hpp"
#include "types/FloatType.hpp"
#include "types/LongType.hpp"
#include "types/Type.hpp"
#include "types/TypeFactory.hpp"
#include "types/TypeID.hpp"
#include "types/TypedValue.hpp"
#include "types/VarCharType.hpp"
#include "types/containers/Tuple.hpp"
#include "types/operations/comparisons/Comparison.hpp"
#include "types/operations/comparisons/ComparisonFactory.hpp"
#include "types/operations/comparisons/ComparisonID.hpp"
#include "types/operations/comparisons/ComparisonUtil.hpp"
#include "utility/Macros.hpp"
#include "utility/ScopedBuffer.hpp"

namespace quickstep {

template <typename T>
class BitWeavingIndexSubBlockTest : public ::testing::Test {
 protected:
  static const std::size_t kIndexSubBlockSize = 0x100000;  // 1 MB.
  // Hack: replace long_attr values equal to this with NULL.
  static const int64_t kLongAttrNullValue = -55555;
  static const char kCharAttrNullValue[];
  static const int kNumSampleTuples = 100;

  virtual void SetUp() {
    storage_manager_.reset(new StorageManager("./test_data/"));

    // Create a sample relation with a variety of attribute types.
    relation_.reset(new CatalogRelation(NULL, "TestRelation"));

    CatalogAttribute *long_attr = new CatalogAttribute(relation_.get(),
                                                       "long_attr",
                                                       TypeFactory::GetType(kLong, false));
    ASSERT_EQ(0, relation_->addAttribute(long_attr));

    CatalogAttribute *negative_long_attr = new CatalogAttribute(relation_.get(),
                                                                "negative_long_attr",
                                                                TypeFactory::GetType(kLong, false));
    ASSERT_EQ(1, relation_->addAttribute(negative_long_attr));

    CatalogAttribute *nullable_long_attr = new CatalogAttribute(relation_.get(),
                                                                "nullable_long_attr",
                                                                TypeFactory::GetType(kLong, true));
    ASSERT_EQ(2, relation_->addAttribute(nullable_long_attr));

    CatalogAttribute *float_attr = new CatalogAttribute(relation_.get(),
                                                        "float_attr",
                                                        TypeFactory::GetType(kFloat, false));
    ASSERT_EQ(3, relation_->addAttribute(float_attr));

    CatalogAttribute *char_attr = new CatalogAttribute(relation_.get(),
                                                       "char_attr",
                                                       TypeFactory::GetType(kChar, 4, false));
    ASSERT_EQ(4, relation_->addAttribute(char_attr));

    CatalogAttribute *nullable_char_attr = new CatalogAttribute(relation_.get(),
                                                                "nullable_char_attr",
                                                                TypeFactory::GetType(kChar, 4, true));
    ASSERT_EQ(5, relation_->addAttribute(nullable_char_attr));

    CatalogAttribute *big_char_attr = new CatalogAttribute(relation_.get(),
                                                           "big_char_attr",
                                                           TypeFactory::GetType(kChar, 80, false));
    ASSERT_EQ(6, relation_->addAttribute(big_char_attr));

    CatalogAttribute *varchar_attr = new CatalogAttribute(relation_.get(),
                                                          "varchar_attr",
                                                          TypeFactory::GetType(kVarChar, 8, false));
    ASSERT_EQ(7, relation_->addAttribute(varchar_attr));

    maximum_attribute_id_ = 7;

    integer_array_ = {{12, -34, 57, 25, -98, -23, 13, 12, 58, 97, -36, -82, 73,
                       18, 8, -39, -18, 48, 29, 85, 91, -3, -47, -92, 91, 69,
                       82, -28, 41, 11, 39, 15, -32, -17, -38, 52, 39, 84, 92,
                       63, 37, 82, 65, 62, -32, -48, -26, -19, -62, 23, 92, 87,
                       29, 17, -54, -56, 13, -18, -92, 18, 82, 75, 62, 37, 12,
                       8, -23, -38, -17, 9, 93, -82, -75, -18, -34, 83, 62, 38,
                       29, -54, 83, 72, -28, -49, -18, 9, 2, 42, 83, 72, 7, -1,
                       0, 82, 93, -82, -42, -98, 78, 2}};

    integer_factor_ = 4;
  }

  virtual void TearDown() {
    if (storage_manager_->blockOrBlobIsLoadedAndDirty(block_id_)) {
      storage_manager_->deleteBlockOrBlobFile(block_id_);
    }
  }

  StorageBlockLayout* createStorageLayout(const CatalogRelation &relation) {
    StorageBlockLayout *layout = new StorageBlockLayout(relation);
    StorageBlockLayoutDescription *layout_desc = layout->getDescriptionMutable();

    layout_desc->set_num_slots(1);

    layout_desc->mutable_tuple_store_description()->set_sub_block_type(
        TupleStorageSubBlockDescription::COMPRESSED_PACKED_ROW_STORE);

    // Attempt to compress variable-length columns.
    for (CatalogRelation::const_iterator attr_it = relation.begin();
         attr_it != relation.end();
         ++attr_it) {
      if (attr_it->getType().isVariableLength()) {
        layout_desc->mutable_tuple_store_description()->AddExtension(
            CompressedPackedRowStoreTupleStorageSubBlockDescription::compressed_attribute_id,
            attr_it->getID());
      }
    }

    for (attribute_id attr_id = 0; attr_id <= maximum_attribute_id_; ++attr_id) {
      IndexSubBlockDescription *index_description = layout_desc->add_index_description();
      buildIndexSubBlockDescription(index_description, attr_id);
    }

    layout->finalize();
    return layout;
  }

  void buildIndexSubBlockDescription(IndexSubBlockDescription *description,
                                     const attribute_id index_attribute) {
    // Make the IndexSubBlockDescription.
    if (std::is_same<T, BitWeavingHIndexSubBlock>::value) {
      description->set_sub_block_type(IndexSubBlockDescription::BITWEAVING_H);
      description->add_indexed_attribute_ids(index_attribute);
      DCHECK(BitWeavingHIndexSubBlock::DescriptionIsValid(*(relation_.get()),
                                                          *description));
    } else if (std::is_same<T, BitWeavingVIndexSubBlock>::value) {
      description->set_sub_block_type(IndexSubBlockDescription::BITWEAVING_V);
      description->add_indexed_attribute_ids(index_attribute);
      DCHECK(BitWeavingVIndexSubBlock::DescriptionIsValid(*(relation_.get()),
                                                          *description));
    } else {
      LOG(FATAL) << "Unknown index type in BitWeavingIndexSubBlockTest.";
    }
  }

  void setIntegerFactor(const std::size_t integer_scale) {
    integer_factor_ = integer_scale;
  }

  std::int64_t getLongValue(const std::int64_t base_value) {
    return base_value * integer_factor_ + 1;
  }

  // Insert a tuple with the specified attribute values into tuple_store_.
  bool insertTupleInTupleStore(const std::int64_t long_val,
                               const std::int64_t negative_long_val,
                               const std::int64_t nullable_long_val,
                               const float float_val,
                               const std::string &char_val,
                               const std::string &nullable_char_val,
                               const std::string &big_char_val,
                               const std::string &varchar_val) {
    std::vector<TypedValue> attrs;

    attrs.emplace_back(LongType::InstanceNonNullable().makeValue(&long_val));

    attrs.emplace_back(LongType::InstanceNonNullable().makeValue(&negative_long_val));

    if (nullable_long_val == kLongAttrNullValue) {
      attrs.emplace_back(LongType::InstanceNullable().makeNullValue());
    } else {
      attrs.emplace_back(LongType::InstanceNullable().makeValue(&nullable_long_val));
    }

    attrs.emplace_back(FloatType::InstanceNonNullable().makeValue(&float_val));

    attrs.emplace_back(CharType::InstanceNonNullable(4).makeValue(
        char_val.c_str(),
        char_val.size() >= 4 ? 4 : char_val.size() + 1).ensureNotReference());

    if (nullable_char_val == kCharAttrNullValue) {
      attrs.emplace_back(CharType::InstanceNullable(4).makeNullValue());
    } else {
      attrs.emplace_back(CharType::InstanceNonNullable(4).makeValue(
          nullable_char_val.c_str(),
          nullable_char_val.size() >= 4 ? 4 : nullable_char_val.size() + 1).ensureNotReference());
    }

    attrs.emplace_back(CharType::InstanceNonNullable(80).makeValue(
        big_char_val.c_str(),
        big_char_val.size() >= 80 ? 80 : big_char_val.size() + 1).ensureNotReference());

    TypedValue varchar_typed_value
        = VarCharType::InstanceNonNullable(varchar_val.size()).makeValue(
            varchar_val.c_str(),
            varchar_val.size() + 1);
    // Test strings are sometimes longer than 8 characters, so truncate if
    // needed.
    varchar_typed_value = VarCharType::InstanceNonNullable(8).coerceValue(
        varchar_typed_value,
        VarCharType::InstanceNonNullable(varchar_val.size()));
    varchar_typed_value.ensureNotReference();
    attrs.emplace_back(std::move(varchar_typed_value));

    MutableBlockReference storage_block(
        storage_manager_->getBlockMutable(block_id_, *relation_));
    Tuple new_tuple(std::move(attrs));
    bool success = storage_block->insertTupleInBatch(new_tuple);
    DCHECK(success);
    return success;
  }

  // Generate a sample tuple based on 'base_value' and insert in into
  // tuple_store_. The sample tuple will have long_attr assigned by getLongValue()
  // method, negative_long_attr chosen from the integer_array_,
  // float_attr equal to 0.25 * base_value, and each of char_attr,
  // big_char_attr, and varchar_attr equal to the string representation of
  // 'base_value' with 'string_suffix' appended on to it. If 'generate_nulls'
  // is true, then both nullable_long_attr and nullable_char_attr will be NULL,
  // otherwise nullable_long_attr will be equal to 'base_value' and
  // nullable_char_attr will be equal to the other string values. Returns true
  // if the tuple is successfully inserted.
  bool generateAndInsertTuple(const std::int64_t base_value,
                              const bool generate_nulls,
                              const std::string &string_suffix) {
    std::ostringstream string_value_buffer;
    string_value_buffer << base_value << string_suffix;
    if (generate_nulls) {
      return insertTupleInTupleStore(getLongValue(base_value),
                                     integer_array_.at(base_value),
                                     kLongAttrNullValue,
                                     0.25 * base_value,
                                     string_value_buffer.str(),
                                     kCharAttrNullValue,
                                     string_value_buffer.str(),
                                     string_value_buffer.str());
    } else {
      return insertTupleInTupleStore(getLongValue(base_value),
                                     integer_array_.at(base_value),
                                     base_value,
                                     0.25 * base_value,
                                     string_value_buffer.str(),
                                     string_value_buffer.str(),
                                     string_value_buffer.str(),
                                     string_value_buffer.str());
    }
  }

  // Put some sample tuples in the test relation.
  void insertSampleData() {
    // Create StorageLayout.
    std::unique_ptr<StorageBlockLayout> layout(createStorageLayout(*relation_));

    block_id_ = storage_manager_->createBlock(*relation_, *layout);
    MutableBlockReference storage_block(storage_manager_->getBlockMutable(block_id_,
                                                                          *relation_));
    relation_->addBlock(block_id_);

    for (int tnum = 0; tnum < kNumSampleTuples; ++tnum) {
      bool is_null = (tnum % 2 == 0);
      ASSERT_TRUE(generateAndInsertTuple(tnum, is_null, "aa"));
    }

    storage_block->rebuild();
  }

  std::uint32_t getIndexCode(const tuple_id tid, const attribute_id attr_id) {
    BlockReference block(storage_manager_->getBlock(block_id_, *relation_));
    const IndexSubBlock &index = block->getIndexSubBlock(attr_id);

    std::uint32_t code;
    if (std::is_same<T, BitWeavingHIndexSubBlock>::value) {
      code = static_cast<const BitWeavingHIndexSubBlock&>(index).getCode(tid);
    } else if (std::is_same<T, BitWeavingVIndexSubBlock>::value) {
      code = static_cast<const BitWeavingVIndexSubBlock&>(index).getCode(tid);
    } else {
      LOG(FATAL) << "Unknown index type in BitWeavingIndexSubBlockTest.";
    }

    return code;
  }

  template <typename AttributeType>
  void evaluateNumericScanWithAllComparisons(const attribute_id attribute,
                                             const typename AttributeType::cpptype literal,
                                             const TupleIdSequence *filter) {
    // Equal predicate.
    evaluateNumericScan<AttributeType>(ComparisonID::kEqual,
                                       attribute, literal, filter);

    // Not equal predicate.
    evaluateNumericScan<AttributeType>(ComparisonID::kNotEqual,
                                       attribute, literal, filter);

    // Less predicate.
    evaluateNumericScan<AttributeType>(ComparisonID::kLess,
                                       attribute, literal, filter);

    // LessOrEqual predicate.
    evaluateNumericScan<AttributeType>(ComparisonID::kLessOrEqual,
                                       attribute, literal, filter);

    // Greater predicate.
    evaluateNumericScan<AttributeType>(ComparisonID::kGreater,
                                       attribute, literal, filter);

    // GreaterOrEqual predicate.
    evaluateNumericScan<AttributeType>(ComparisonID::kGreaterOrEqual,
                                       attribute, literal, filter);
  }

  void evaluateStringScanWithAllComparisons(const attribute_id attribute,
                                            const std::string literal,
                                            const TupleIdSequence *filter) {
    // Equal predicate.
    this->evaluateStringScan(ComparisonID::kEqual,
                             attribute, literal, nullptr);

    // Not equal predicate.
    this->evaluateStringScan(ComparisonID::kNotEqual,
                             attribute, literal, nullptr);

    // Less predicate.
    this->evaluateStringScan(ComparisonID::kLess,
                             attribute, literal, nullptr);

    // LessOrEqual predicate.
    this->evaluateStringScan(ComparisonID::kLessOrEqual,
                             attribute, literal, nullptr);

    // Greater predicate.
    this->evaluateStringScan(ComparisonID::kGreater,
                             attribute, literal, nullptr);

    // GreaterOrEqual predicate.
    this->evaluateStringScan(ComparisonID::kGreaterOrEqual,
                             attribute, literal, nullptr);
  }

  template <typename AttributeType>
  void evaluateNumericScan(const ComparisonID comp,
                           const attribute_id attribute,
                           const typename AttributeType::cpptype literal,
                           const TupleIdSequence *filter) {
    std::unique_ptr<ComparisonPredicate> predicate;
    predicate.reset(generateNumericComparisonPredicate<AttributeType>(comp,
                                                                      attribute,
                                                                      literal));

    BlockReference block(storage_manager_->getBlock(block_id_, *relation_));
    const TupleStorageSubBlock &tuple_store = block->getTupleStorageSubBlock();
    const IndexSubBlock &index = block->getIndexSubBlock(attribute);

    TupleIdSequence *sequence = index.getMatchesForPredicate(*predicate, filter);
    ASSERT_TRUE(kNumSampleTuples == sequence->length());
    for (tuple_id tid = 0; tid < kNumSampleTuples; ++tid) {
      if (filter && !filter->get(tid)) {
        EXPECT_FALSE(sequence->get(tid));
        continue;
      }
      TypedValue typed_value = tuple_store.getAttributeValueTyped(tid, attribute);
      if (typed_value.isNull()) {
        ASSERT_FALSE(sequence->get(tid));
        continue;
      }
      typename AttributeType::cpptype value
          = typed_value.getLiteral<typename AttributeType::cpptype>();
      bool is_match;
      switch (comp) {
        case ComparisonID::kEqual:
          is_match = (value == literal);
          break;
        case ComparisonID::kNotEqual:
          is_match = (value != literal);
          break;
        case ComparisonID::kLess:
          is_match = (value < literal);
          break;
        case ComparisonID::kLessOrEqual:
          is_match = (value <= literal);
          break;
        case ComparisonID::kGreater:
          is_match = (value > literal);
          break;
        case ComparisonID::kGreaterOrEqual:
          is_match = (value >= literal);
          break;
        default:
          LOG(FATAL) << "Unknown comparator in evaluateNumericScan.";
      }

      if (is_match) {
        ASSERT_TRUE(sequence->get(tid));
      } else {
        ASSERT_FALSE(sequence->get(tid));
      }
    }
  }

  void evaluateStringScan(const ComparisonID comp,
                          const attribute_id attribute,
                          const std::string literal,
                          const TupleIdSequence *filter) {
    std::unique_ptr<ComparisonPredicate> predicate;
    predicate.reset(generateStringComparisonPredicate(comp,
                                                      attribute,
                                                      literal));

    BlockReference block(storage_manager_->getBlock(block_id_, *relation_));
    const TupleStorageSubBlock &tuple_store = block->getTupleStorageSubBlock();
    const IndexSubBlock &index = block->getIndexSubBlock(attribute);

    TupleIdSequence *sequence = index.getMatchesForPredicate(*predicate, filter);
    ASSERT_TRUE(kNumSampleTuples == sequence->length());
    for (tuple_id tid = 0; tid < kNumSampleTuples; ++tid) {
      if (filter && !filter->get(tid)) {
        ASSERT_FALSE(sequence->get(tid));
        continue;
      }
      TypedValue typed_value = tuple_store.getAttributeValueTyped(tid, attribute);
      ASSERT_TRUE(typed_value.getTypeID() == kChar || typed_value.getTypeID() == kVarChar);
      if (typed_value.isNull()) {
        ASSERT_FALSE(sequence->get(tid));
        continue;
      }
      std::string value(static_cast<const char*>(typed_value.getDataPtr()),
                        typed_value.getAsciiStringLength());
      bool is_match;
      switch (comp) {
        case ComparisonID::kEqual:
          is_match = (value.compare(literal) == 0);
          break;
        case ComparisonID::kNotEqual:
          is_match = (value.compare(literal) != 0);
          break;
        case ComparisonID::kLess:
          is_match = (value.compare(literal) < 0);
          break;
        case ComparisonID::kLessOrEqual:
          is_match = (value.compare(literal) <= 0);
          break;
        case ComparisonID::kGreater:
          is_match = (value.compare(literal) > 0);
          break;
        case ComparisonID::kGreaterOrEqual:
          is_match = (value.compare(literal) >= 0);
          break;
        default:
          LOG(FATAL) << "Unknown comparator in evaluateStringScan.";
      }

      if (is_match) {
        ASSERT_TRUE(sequence->get(tid));
      } else {
        ASSERT_FALSE(sequence->get(tid));
      }
    }
  }

  // Create a ComparisonPredicate of the form "attribute comp literal".
  template <typename AttributeType>
  ComparisonPredicate* generateNumericComparisonPredicate(const ComparisonID comp,
                                                const attribute_id attribute,
                                                const typename AttributeType::cpptype literal) {
    ScalarAttribute *scalar_attribute = new ScalarAttribute(*relation_->getAttributeById(attribute));
    ScalarLiteral *scalar_literal
        = new ScalarLiteral(AttributeType::InstanceNonNullable().makeValue(&literal),
                            AttributeType::InstanceNonNullable());
    return new ComparisonPredicate(ComparisonFactory::GetComparison(comp), scalar_attribute, scalar_literal);
  }

  ComparisonPredicate* generateStringComparisonPredicate(const ComparisonID comp,
                                               const attribute_id attribute,
                                               const std::string &literal) {
    ScalarAttribute *scalar_attribute = new ScalarAttribute(*relation_->getAttributeById(attribute));
    ScalarLiteral *scalar_literal = new ScalarLiteral(
        VarCharType::InstanceNonNullable(literal.size()).makeValue(
            literal.c_str(),
            literal.size() + 1).ensureNotReference(),
        VarCharType::InstanceNonNullable(80));
    return new ComparisonPredicate(ComparisonFactory::GetComparison(comp), scalar_attribute, scalar_literal);
  }

  std::unique_ptr<StorageManager> storage_manager_;
  std::unique_ptr<CatalogRelation> relation_;
  std::unique_ptr<StorageBlockLayout> layout_;
  attribute_id maximum_attribute_id_;
  block_id block_id_;
  std::array<std::int32_t, kNumSampleTuples> integer_array_;
  std::size_t integer_factor_;
};

template <typename T>
const char BitWeavingIndexSubBlockTest<T>::kCharAttrNullValue[] = "_NULLSTRING";

typedef ::testing::Types<BitWeavingHIndexSubBlock, BitWeavingVIndexSubBlock> BitWeavingIndexSubBlockTestTypes;
TYPED_TEST_CASE(BitWeavingIndexSubBlockTest, BitWeavingIndexSubBlockTestTypes);

TYPED_TEST(BitWeavingIndexSubBlockTest, BlockTooSmallTest) {
  // Create a MockTupleStorageSubBlock to hold tuples for testing.
  std::unique_ptr<MockTupleStorageSubBlock> tuple_store(
      new MockTupleStorageSubBlock(*this->relation_));
  std::unique_ptr<IndexSubBlockDescription> index_description(
      new IndexSubBlockDescription());
  this->buildIndexSubBlockDescription(index_description.get(), 0);
  std::unique_ptr<TypeParam> index;
  ScopedBuffer index_memory;

  std::size_t index_memory_size = 16;
  index_memory.reset(index_memory_size);
  EXPECT_THROW(index.reset(new TypeParam(*tuple_store,
                           *index_description,
                           true,
                           index_memory.get(),
                           index_memory_size)),
               BlockMemoryTooSmall);
}

TYPED_TEST(BitWeavingIndexSubBlockTest, EmptyBlockTest) {
  attribute_id indexed_attribute = 0;  // long_attr

  this->insertSampleData();

  std::unique_ptr<ComparisonPredicate> predicate;
  const std::int64_t literal = 0;

  ScalarAttribute *scalar_attribute
      = new ScalarAttribute(*this->relation_->getAttributeById(indexed_attribute));
  ScalarLiteral *scalar_literal
      = new ScalarLiteral(LongType::InstanceNonNullable().makeValue(&literal),
                          LongType::InstanceNonNullable());
  predicate.reset(new ComparisonPredicate(ComparisonFactory::GetComparison(ComparisonID::kEqual),
                                          scalar_attribute,
                                          scalar_literal));

  BlockReference block(this->storage_manager_->getBlock(this->block_id_, *this->relation_));
  const IndexSubBlock &index = block->getIndexSubBlock(indexed_attribute);
  TupleIdSequence *sequence = index.getMatchesForPredicate(*predicate, nullptr);
  EXPECT_EQ(0u, sequence->numTuples());
}

TYPED_TEST(BitWeavingIndexSubBlockTest, GetCodeTest) {
  this->insertSampleData();

  attribute_id indexed_attribute = 0;  // long_attr
  for (tuple_id tid = 0; tid < TestFixture::kNumSampleTuples; ++tid) {
    std::uint32_t code = this->getIndexCode(tid, indexed_attribute);
    EXPECT_EQ(this->getLongValue(tid), code);
  }

  indexed_attribute = 1;  // negative_long_attr
  BlockReference block(this->storage_manager_->getBlock(this->block_id_, *this->relation_));
  const IndexSubBlock &long_index = block->getIndexSubBlock(indexed_attribute);
  const CompressionDictionary *long_dictionary
      = static_cast<const BitWeavingIndexSubBlock&>(long_index).getCompressionDictionary();
  ASSERT_NE(long_dictionary, nullptr);
  for (tuple_id tid = 0; tid < TestFixture::kNumSampleTuples; ++tid) {
    std::uint32_t code = this->getIndexCode(tid, indexed_attribute);
    // Decode the value.
    TypedValue value = long_dictionary->getTypedValueForCode(code);
    EXPECT_EQ(this->integer_array_.at(tid), value.getLiteral<std::int64_t>());
  }

  // Check the external dictionary.
  indexed_attribute = 7;  // varchar_attr
  const IndexSubBlock &varchar_index = block->getIndexSubBlock(indexed_attribute);
  const CompressionDictionary *varchar_dictionary
      = static_cast<const BitWeavingIndexSubBlock&>(varchar_index).getCompressionDictionary();
  ASSERT_NE(varchar_dictionary, nullptr);
  for (tuple_id tid = 0; tid < TestFixture::kNumSampleTuples; ++tid) {
    std::uint32_t code = this->getIndexCode(tid, indexed_attribute);
    // Decode the value.
    TypedValue typed_value = varchar_dictionary->getTypedValueForCode(code);
    std::string value(static_cast<const char*>(typed_value.getDataPtr()),
                      typed_value.getAsciiStringLength());
    std::ostringstream string_value_buffer;
    string_value_buffer << tid << "aa";
    EXPECT_EQ(string_value_buffer.str(), value);
  }
}

TYPED_TEST(BitWeavingIndexSubBlockTest, LongTypeScanTest) {
  this->insertSampleData();

  attribute_id indexed_attribute = 0;  // long_attr
  constexpr std::size_t num_literals = 10;
  std::array<std::int64_t, num_literals> literals = {{12, 34, 26, 200, 1, 38, 21, 49, -10, 96}};

  for (std::int64_t literal : literals) {
    // Check all comparisons.
    this->template evaluateNumericScanWithAllComparisons<LongType>(indexed_attribute,
                                                                   literal,
                                                                   nullptr);
  }
}

TYPED_TEST(BitWeavingIndexSubBlockTest, NegativeLongTypeScanTest) {
  this->insertSampleData();

  attribute_id indexed_attribute = 1;  // negative_long_attr
  constexpr std::size_t num_literals = 10;
  std::array<std::int64_t, num_literals> literals = {{11, 39, -62, 38, -3, 93, 11, 91, -92, -32}};

  for (std::int64_t literal : literals) {
    // Check all comparisons.
    this->template evaluateNumericScanWithAllComparisons<LongType>(indexed_attribute,
                                                                   literal,
                                                                   nullptr);
  }
}

TYPED_TEST(BitWeavingIndexSubBlockTest, NullableLongTypeScanTest) {
  this->insertSampleData();

  attribute_id indexed_attribute = 2;  // nullable_long_attr
  constexpr std::size_t num_literals = 10;
  std::array<std::int64_t, num_literals> literals = {{12, 34, 26, 83, 1, 38, 21, 49, 23, 96}};

  for (std::int64_t literal : literals) {
    // Check all comparisons.
    this->template evaluateNumericScanWithAllComparisons<LongType>(indexed_attribute,
                                                                   literal,
                                                                   nullptr);
  }
}

TYPED_TEST(BitWeavingIndexSubBlockTest, FloatTypeScanTest) {
  this->insertSampleData();

  attribute_id indexed_attribute = 3;  // float_attr
  constexpr std::size_t num_literals = 10;
  std::array<float, num_literals> literals =
      {{-7.4, 0.0, 0.3, 7.248, 10.2, 11.4, 20.6, 22.7384, 25.0, 26.1}};

  for (float literal : literals) {
    // Check all comparisons.
    this->template evaluateNumericScanWithAllComparisons<FloatType>(indexed_attribute,
                                                                    literal,
                                                                    nullptr);
  }
}

TYPED_TEST(BitWeavingIndexSubBlockTest, StringScanTest) {
  this->insertSampleData();

  constexpr std::size_t num_literals = 10;
  std::array<std::string, num_literals> literals =
      {{"31aa", "132", "521", "0", "78", "42ab", "2", "89cd", "zz", "99ab"}};

  constexpr std::size_t num_attributes = 3;
  std::array<attribute_id, num_attributes> attributes = {{4, 6, 7}};

  for (attribute_id indexed_attribute : attributes) {
    for (std::string literal : literals) {
      // Check all comparisons.
      this->evaluateStringScanWithAllComparisons(indexed_attribute,
                                                 literal,
                                                 nullptr);
    }
  }
}

TYPED_TEST(BitWeavingIndexSubBlockTest, NullableStringScanTest) {
  this->insertSampleData();

  constexpr std::size_t num_literals = 10;
  std::array<std::string, num_literals> literals =
      {{"31aa", "132", "521", "0", "78", "42ab", "2", "89cd", "zz", "99ab"}};

  attribute_id indexed_attribute = 5;

  for (std::string literal : literals) {
    // Check all comparisons.
    this->evaluateStringScanWithAllComparisons(indexed_attribute,
                                               literal,
                                               nullptr);
  }
}

TYPED_TEST(BitWeavingIndexSubBlockTest, CodeLengthTest) {
  for (std::size_t bits = 0; bits < 24; ++bits) {
    this->SetUp();
    this->setIntegerFactor(1ULL << bits);
    this->insertSampleData();

    attribute_id indexed_attribute = 0;  // long_attr
    constexpr std::size_t num_literals = 10;
    std::array<std::uint32_t, num_literals> literals =
        {{12, 34, 26, 83, 1, 38, 21, 49, 23, 96}};

    for (std::int64_t literal : literals) {
      literal *= (1ULL << bits);

      // Check all comparisons.
      this->template evaluateNumericScanWithAllComparisons<LongType>(indexed_attribute,
                                                                     literal,
                                                                     nullptr);
    }
    // Required to evict created blocks, and therefore hush warnings generated
    // by StorageManager.
    this->TearDown();
  }
}

TYPED_TEST(BitWeavingIndexSubBlockTest, FilterLongTest) {
  this->insertSampleData();

  attribute_id indexed_attribute = 0;  // long_attr

  BlockReference block(this->storage_manager_->getBlock(this->block_id_, *this->relation_));
  const TupleStorageSubBlock &tuple_store = block->getTupleStorageSubBlock();

  const tuple_id num_tuples = tuple_store.getMaxTupleID() + 1;
  std::unique_ptr<TupleIdSequence> filter(new TupleIdSequence(num_tuples));
  // Set the filter bitmap
  for (tuple_id tid = 0; tid < num_tuples; tid += 3) {
    filter->set(tid, true);
  }

  constexpr std::size_t num_literals = 10;
  std::array<std::int64_t, num_literals> literals =
      {{12, 34, 26, 200, 1, 38, 21, 49, -10, 96}};

  for (std::int64_t literal : literals) {
    // Check all comparisons.
    this->template evaluateNumericScanWithAllComparisons<LongType>(indexed_attribute,
                                                                   literal,
                                                                   filter.get());
  }
}

TYPED_TEST(BitWeavingIndexSubBlockTest, FilterNullableLongTest) {
  this->insertSampleData();

  attribute_id indexed_attribute = 2;  // nullable_long_attr

  BlockReference block(this->storage_manager_->getBlock(this->block_id_, *this->relation_));
  const TupleStorageSubBlock &tuple_store = block->getTupleStorageSubBlock();

  const tuple_id num_tuples = tuple_store.getMaxTupleID() + 1;
  std::unique_ptr<TupleIdSequence> filter(new TupleIdSequence(num_tuples));
  // Set the filter bitmap
  for (tuple_id tid = 0; tid < num_tuples; tid += 3) {
    filter->set(tid, true);
  }

  constexpr std::size_t num_literals = 10;
  std::array<std::int64_t, num_literals> literals =
      {{12, 34, 26, 83, 1, 38, 21, 49, 23, 96}};

  for (std::int64_t literal : literals) {
    // Check all comparisons.
    this->template evaluateNumericScanWithAllComparisons<LongType>(indexed_attribute,
                                                                   literal,
                                                                   filter.get());
  }
}

}  // namespace quickstep
