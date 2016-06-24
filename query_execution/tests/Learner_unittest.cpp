/**
 *   Copyright 2016, Quickstep Research Group, Computer Sciences Department,
 *     University of Wisconsin—Madison.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 **/

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <memory>
#include <random>
#include <vector>

#include "gtest/gtest.h"

#include "query_execution/Learner.hpp"
#include "query_execution/QueryExecutionMessages.pb.h"
#include "query_execution/QueryExecutionTypedefs.hpp"
#include "query_optimizer/QueryHandle.hpp"

namespace quickstep {

class LearnerTest : public ::testing::Test {
 protected:
  serialization::NormalWorkOrderCompletionMessage createMockCompletionMessage(
      const std::size_t query_id, const std::size_t operator_id) {
    serialization::NormalWorkOrderCompletionMessage mock_proto_message;
    mock_proto_message.set_operator_index(operator_id);
    mock_proto_message.set_query_id(query_id);
    mock_proto_message.set_worker_thread_index(0);
    mock_proto_message.set_execution_time_in_microseconds(10);

    return mock_proto_message;
  }
};

TEST_F(LearnerTest, AddAndRemoveQueryTest) {
  Learner learner;
  std::unique_ptr<QueryHandle> handle;
  const std::size_t kPriorityLevel = 1;
  handle.reset(new QueryHandle(1, kPriorityLevel));

  EXPECT_FALSE(learner.hasActiveQueries());
  learner.addQuery(*handle);
  EXPECT_EQ(1u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(1u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel));
  EXPECT_TRUE(learner.hasActiveQueries());

  learner.removeQuery(handle->query_id());
  EXPECT_EQ(0u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(0u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel));
  EXPECT_FALSE(learner.hasActiveQueries());

  handle.reset(new QueryHandle(2, kPriorityLevel));
  learner.addQuery(*handle);
  EXPECT_EQ(1u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(1u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel));
  EXPECT_TRUE(learner.hasActiveQueries());

  learner.removeQuery(handle->query_id());
  EXPECT_EQ(0u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(0u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel));
  EXPECT_FALSE(learner.hasActiveQueries());
}

TEST_F(LearnerTest, MultipleQueriesSamePriorityAddRemoveTest) {
  Learner learner;
  std::unique_ptr<QueryHandle> handle1, handle2;
  const std::size_t kPriorityLevel = 1;
  handle1.reset(new QueryHandle(1, kPriorityLevel));
  handle2.reset(new QueryHandle(2, kPriorityLevel));

  EXPECT_FALSE(learner.hasActiveQueries());
  EXPECT_EQ(0u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(0u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel));

  learner.addQuery(*handle1);
  EXPECT_TRUE(learner.hasActiveQueries());
  EXPECT_EQ(1u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(1u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel));
  learner.addQuery(*handle2);
  EXPECT_TRUE(learner.hasActiveQueries());
  EXPECT_EQ(2u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(2u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel));

  learner.removeQuery(handle1->query_id());
  EXPECT_TRUE(learner.hasActiveQueries());
  EXPECT_EQ(1u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(1u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel));

  learner.removeQuery(handle2->query_id());

  EXPECT_FALSE(learner.hasActiveQueries());
  EXPECT_EQ(0u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(0u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel));
}

TEST_F(LearnerTest, MultipleQueriesDifferentPrioritiesAddRemoveTest) {
  Learner learner;
  std::unique_ptr<QueryHandle> handle1, handle2;
  const std::size_t kPriorityLevel1 = 1;
  const std::size_t kPriorityLevel2 = 2;
  handle1.reset(new QueryHandle(1, kPriorityLevel1));
  handle2.reset(new QueryHandle(2, kPriorityLevel2));

  EXPECT_FALSE(learner.hasActiveQueries());
  EXPECT_EQ(0u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(0u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel1));
  EXPECT_EQ(0u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel2));

  learner.addQuery(*handle1);
  EXPECT_TRUE(learner.hasActiveQueries());
  EXPECT_EQ(1u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(1u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel1));
  EXPECT_EQ(0u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel2));

  learner.addQuery(*handle2);
  EXPECT_TRUE(learner.hasActiveQueries());
  EXPECT_EQ(2u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(1u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel1));
  EXPECT_EQ(1u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel2));

  learner.removeQuery(handle2->query_id());
  EXPECT_TRUE(learner.hasActiveQueries());
  EXPECT_EQ(1u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(0u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel2));
  EXPECT_EQ(1u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel1));

  learner.removeQuery(handle1->query_id());
  EXPECT_FALSE(learner.hasActiveQueries());
  EXPECT_EQ(0u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(0u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel1));
  EXPECT_EQ(0u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel2));
}

TEST_F(LearnerTest, AddCompletionFeedbackSamePriorityLevelTest) {
  Learner learner;
  std::unique_ptr<QueryHandle> handle1, handle2;
  const std::size_t kPriorityLevel = 1;
  handle1.reset(new QueryHandle(1, kPriorityLevel));

  EXPECT_FALSE(learner.hasActiveQueries());
  EXPECT_EQ(0u, learner.getTotalNumActiveQueries());
  learner.addQuery(*handle1);
  serialization::NormalWorkOrderCompletionMessage completion_message =
      createMockCompletionMessage(handle1->query_id(), 0);

  learner.addCompletionFeedback(completion_message);
  EXPECT_TRUE(learner.hasActiveQueries());
  EXPECT_EQ(1u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(1u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel));

  handle2.reset(new QueryHandle(2, kPriorityLevel));
  learner.addQuery(*handle2);
  completion_message = createMockCompletionMessage(handle2->query_id(), 0);
  learner.addCompletionFeedback(completion_message);

  EXPECT_TRUE(learner.hasActiveQueries());
  EXPECT_EQ(2u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(2u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel));
}

TEST_F(LearnerTest, AddCompletionFeedbackMultiplePriorityLevelsTest) {
  Learner learner;
  std::unique_ptr<QueryHandle> handle1, handle2;
  const std::size_t kPriorityLevel1 = 1;
  const std::size_t kPriorityLevel2 = 2;
  handle1.reset(new QueryHandle(1, kPriorityLevel1));

  EXPECT_FALSE(learner.hasActiveQueries());
  EXPECT_EQ(0u, learner.getTotalNumActiveQueries());
  learner.addQuery(*handle1);

  handle2.reset(new QueryHandle(2, kPriorityLevel2));
  learner.addQuery(*handle2);

  EXPECT_EQ(2u, learner.getTotalNumActiveQueries());
  EXPECT_EQ(1u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel1));
  EXPECT_EQ(1u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel2));

  const std::size_t kNumIterations = 10;
  std::vector<QueryHandle*> handles;
  handles.emplace_back(handle1.get());
  handles.emplace_back(handle2.get());
  for (std::size_t iter_num = 0; iter_num < kNumIterations; ++iter_num) {
    for (std::size_t index = 0; index < handles.size(); ++index) {
      EXPECT_TRUE(learner.hasActiveQueries());
      serialization::NormalWorkOrderCompletionMessage completion_message =
        createMockCompletionMessage(handles[index]->query_id(), 0);
      learner.addCompletionFeedback(completion_message);
      EXPECT_EQ(2u, learner.getTotalNumActiveQueries());
      EXPECT_EQ(1u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel1));
      EXPECT_EQ(1u, learner.getNumActiveQueriesInPriorityLevel(kPriorityLevel2));
    }
  }
}

TEST_F(LearnerTest, HighestPriorityLevelTest) {
  std::vector<std::size_t> priorities_insertion_order;
  std::vector<std::size_t> priorities_removal_order;
  const std::size_t kNumPrioritiesToTest = 20;
  for (std::size_t priority_num = 1;
       priority_num <= kNumPrioritiesToTest;
       ++priority_num) {
    // Note: Priority level should be non-zero, hence we begin from 1.
    priorities_insertion_order.emplace_back(priority_num);
    priorities_removal_order.emplace_back(priority_num);
  }

  // Randomize the orders.
  std::random_device rd;
  std::mt19937 g(rd());

  std::shuffle(priorities_insertion_order.begin(),
               priorities_insertion_order.end(),
               g);

  std::shuffle(priorities_removal_order.begin(),
               priorities_removal_order.end(),
               g);

  Learner learner;
  EXPECT_EQ(kInvalidPriorityLevel, learner.getHighestPriorityLevel());

  std::unique_ptr<QueryHandle> handle;
  // First insert the queries in the order of priorities as defined by
  // priorities_insertion_order.
  for (auto it = priorities_insertion_order.begin();
       it != priorities_insertion_order.end();
       ++it) {
    // Note that the query ID is kept the same as priority level for simplicity.
    handle.reset(new QueryHandle(*it, *it));
    learner.addQuery(*handle);
    const std::size_t max_priority_so_far =
        *(std::max_element(priorities_insertion_order.begin(), it + 1));
    EXPECT_EQ(static_cast<int>(max_priority_so_far),
              learner.getHighestPriorityLevel());
  }
  // Now remove the queries in the order of priorities as defined by
  // priorities_removal_order.
  for (auto it = priorities_removal_order.begin();
       it != priorities_removal_order.end();
       ++it) {
    // Recall that the query ID is the same as priority level.
    const std::size_t max_priority_so_far =
        *(std::max_element(it, priorities_removal_order.end()));
    EXPECT_EQ(static_cast<int>(max_priority_so_far),
              learner.getHighestPriorityLevel());
    learner.removeQuery(*it);
  }
  EXPECT_FALSE(learner.hasActiveQueries());
  EXPECT_EQ(kInvalidPriorityLevel, learner.getHighestPriorityLevel());
}

}  // namespace quickstep