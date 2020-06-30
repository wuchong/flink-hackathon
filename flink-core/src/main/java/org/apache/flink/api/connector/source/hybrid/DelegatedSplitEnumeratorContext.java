/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.connector.source.hybrid;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.MetricGroup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * A SplitEnumeratorContext that delegates all the actions to the underlying SplitEnumeratorContext.
 */
public abstract class DelegatedSplitEnumeratorContext<SplitT extends SourceSplit, SplitT1 extends SourceSplit, SplitT2 extends SourceSplit>
		implements SplitEnumeratorContext<SplitT> {

	final SplitEnumeratorContext<HybridSourceSplit<SplitT1, SplitT2>> underlyingContext;

	protected DelegatedSplitEnumeratorContext(SplitEnumeratorContext<HybridSourceSplit<SplitT1, SplitT2>> underlyingContext) {
		this.underlyingContext = underlyingContext;
	}

	@Override
	public MetricGroup metricGroup() {
		return underlyingContext.metricGroup();
	}

	@Override
	public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
		underlyingContext.sendEventToSourceReader(subtaskId, event);
	}

	@Override
	public int currentParallelism() {
		return underlyingContext.currentParallelism();
	}

	@Override
	public Map<Integer, ReaderInfo> registeredReaders() {
		return underlyingContext.registeredReaders();
	}

	@Override
	public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
		underlyingContext.callAsync(callable, handler);
	}

	@Override
	public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler, long initialDelay, long period) {
		underlyingContext.callAsync(callable, handler, initialDelay, period);
	}

	/**
	 * A SplitEnumeratorContext for first source.
	 */
	public static final class FirstSourceSplitEnumeratorContext<SplitT1 extends SourceSplit, SplitT2 extends SourceSplit> extends DelegatedSplitEnumeratorContext<SplitT1, SplitT1, SplitT2> {

		private final Map<Integer, Boolean> readerIdToCompletion;
		public FirstSourceSplitEnumeratorContext(
				SplitEnumeratorContext<HybridSourceSplit<SplitT1, SplitT2>> underlyingContext,
				Map<Integer, Boolean> readerIdToCompletion) {
			super(underlyingContext);
			this.readerIdToCompletion = readerIdToCompletion;
		}

		@Override
		public void assignSplits(SplitsAssignment<SplitT1> newSplitAssignments) {
			Map<Integer, List<SplitT1>> assignment = newSplitAssignments.assignment();
			Map<Integer, List<HybridSourceSplit<SplitT1, SplitT2>>> newAssignment = new HashMap<>();
			assignment.forEach((owner, splits) -> {
				List<HybridSourceSplit<SplitT1, SplitT2>> newSplits = new ArrayList<>();
				for (SplitT1 split : splits) {
					newSplits.add(HybridSourceSplit.forFirstSource(split));
				}
				newAssignment.put(owner, newSplits);
				readerIdToCompletion.put(owner, false);
			});
			underlyingContext.assignSplits(new SplitsAssignment<>(newAssignment));
		}
	}

	/**
	 * A SplitEnumeratorContext for second source.
	 */
	public static final class SecondSourceSplitEnumeratorContext<SplitT1 extends SourceSplit, SplitT2 extends SourceSplit> extends DelegatedSplitEnumeratorContext<SplitT2, SplitT1, SplitT2> {

		public SecondSourceSplitEnumeratorContext(SplitEnumeratorContext<HybridSourceSplit<SplitT1, SplitT2>> underlyingContext) {
			super(underlyingContext);
		}

		@Override
		public void assignSplits(SplitsAssignment<SplitT2> newSplitAssignments) {
			Map<Integer, List<SplitT2>> assignment = newSplitAssignments.assignment();
			Map<Integer, List<HybridSourceSplit<SplitT1, SplitT2>>> newAssignment = new HashMap<>();
			assignment.forEach((owner, splits) -> {
				List<HybridSourceSplit<SplitT1, SplitT2>> newSplits = new ArrayList<>();
				for (SplitT2 split : splits) {
					newSplits.add(HybridSourceSplit.forSecondSource(split));
				}
				newAssignment.put(owner, newSplits);
			});
			underlyingContext.assignSplits(new SplitsAssignment<>(newAssignment));
		}
	}
}
