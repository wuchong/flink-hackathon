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

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.event.SplitsFinishedEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The implementation of {@link SplitEnumerator} for {@link HybridSource}.
 */
public class HybridSplitEnumerator<SplitT1 extends SourceSplit, SplitT2 extends SourceSplit, EnumChkT1, EnumChkT2>
		implements SplitEnumerator<HybridSourceSplit<SplitT1, SplitT2>, HybridSourceEnumState<EnumChkT1, EnumChkT2>> {

	private static final Logger LOG = LoggerFactory.getLogger(HybridSplitEnumerator.class);

	private final SplitEnumeratorContext<HybridSourceSplit<SplitT1, SplitT2>> context;
	private final SplitEnumerator<SplitT1, EnumChkT1> firstEnumerator;
	private final SplitEnumerator<SplitT2, EnumChkT2> secondEnumerator;
	private final Map<Integer, Boolean> readerIdToCompletion;
	private boolean inFirstSourceMode;

	public HybridSplitEnumerator(
			SplitEnumeratorContext<HybridSourceSplit<SplitT1, SplitT2>> context,
			SplitEnumerator<SplitT1, EnumChkT1> firstEnumerator,
			SplitEnumerator<SplitT2, EnumChkT2> secondEnumerator,
			Map<Integer, Boolean> readerIdToCompletion) {
		this(context, firstEnumerator, secondEnumerator, readerIdToCompletion, true);
	}

	public HybridSplitEnumerator(
			SplitEnumeratorContext<HybridSourceSplit<SplitT1, SplitT2>> context,
			SplitEnumerator<SplitT1, EnumChkT1> firstEnumerator,
			SplitEnumerator<SplitT2, EnumChkT2> secondEnumerator,
			Map<Integer, Boolean> readerIdToCompletion,
			boolean inFirstSourceMode) {
		this.context = context;
		this.firstEnumerator = firstEnumerator;
		this.secondEnumerator = secondEnumerator;
		this.readerIdToCompletion = readerIdToCompletion;
		this.inFirstSourceMode = inFirstSourceMode;
	}

	@Override
	public void start() {
		if (inFirstSourceMode) {
			firstEnumerator.start();
		} else {
			secondEnumerator.start();
		}
	}

	@Override
	public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
		if (inFirstSourceMode) {
			if (sourceEvent instanceof SplitsFinishedEvent) {
				readerIdToCompletion.put(subtaskId, true);
				// all readers are finished
				if (readerIdToCompletion.values().stream().allMatch(Boolean::booleanValue)) {
					switchEnumerator();
				}
				return;
			}
			firstEnumerator.handleSourceEvent(subtaskId, sourceEvent);
		} else {
			secondEnumerator.handleSourceEvent(subtaskId, sourceEvent);
		}
	}

	private void switchEnumerator() {
		try {
			firstEnumerator.close();
			// TODO: switch offset to incremental enumerator
			secondEnumerator.start();
			// notify second enumerator all the registered readers
			context.registeredReaders().forEach((subtaskId, readerInfo) -> {
				secondEnumerator.addReader(subtaskId);
			});
			inFirstSourceMode = false;
		} catch (IOException e) {
			LOG.error("Switching source enumerator failed", e);
			throw new RuntimeException("Switching source enumerator failed.", e);
		}
	}

	@Override
	public void addSplitsBack(List<HybridSourceSplit<SplitT1, SplitT2>> splits, int subtaskId) {
		if (inFirstSourceMode) {
			List<SplitT1> firstSourceSplits = new ArrayList<>();
			for (HybridSourceSplit<SplitT1, SplitT2> split : splits) {
				firstSourceSplits.add(split.getFirstSourceSplit());
			}
			firstEnumerator.addSplitsBack(firstSourceSplits, subtaskId);
		} else {
			List<SplitT2> secondSourceSplits = new ArrayList<>();
			for (HybridSourceSplit<SplitT1, SplitT2> split : splits) {
				secondSourceSplits.add(split.getSecondSourceSplit());
			}
			secondEnumerator.addSplitsBack(secondSourceSplits, subtaskId);
		}
	}

	@Override
	public void addReader(int subtaskId) {
		if (inFirstSourceMode) {
			firstEnumerator.addReader(subtaskId);
		} else {
			secondEnumerator.addReader(subtaskId);
		}
	}

	@Override
	public HybridSourceEnumState<EnumChkT1, EnumChkT2> snapshotState() throws Exception {
		if (inFirstSourceMode) {
			EnumChkT1 firstEnumState = firstEnumerator.snapshotState();
			return HybridSourceEnumState.forFirstSource(firstEnumState);
		} else {
			EnumChkT2 secondEnumState = secondEnumerator.snapshotState();
			return HybridSourceEnumState.forSecondSource(secondEnumState);
		}
	}

	@Override
	public void close() throws IOException {
		firstEnumerator.close();
		secondEnumerator.close();
	}
}
