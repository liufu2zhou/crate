/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.data;

import io.crate.testing.BatchIteratorTester;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.TestingBatchIterators;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SkippingBatchIteratorTest {

    private int offset = 3;
    private List<Object[]> expectedResult = IntStream.range(3, 10)
        .mapToObj(i -> new Object[]{i})
        .collect(Collectors.toList());

    @Test
    public void testSkippingBatchIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> new SkippingBatchIterator(TestingBatchIterators.range(0, 10), offset)
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testSkippingBatchIteratorWithBatchedSource() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> {
                BatchIterator source = TestingBatchIterators.range(0, 10);
                source = new CloseAssertingBatchIterator(new BatchSimulatingIterator(source, 2, 5, null));
                return new SkippingBatchIterator(source, offset);
            }
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }
}
