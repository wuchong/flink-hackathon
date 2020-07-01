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

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.event.SplitsFinishedEvent;
import org.apache.flink.core.io.InputStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The implementation of {@link SourceReader} for {@link HybridSource}.
 */
public class HybridSourceReader<T, SplitT1 extends SourceSplit, SplitT2 extends SourceSplit>
	implements SourceReader<T, HybridSourceSplit<SplitT1, SplitT2>> {

	private final SourceReaderContext context;
	private final SourceReader<T, SplitT1> firstReader;
	private final SourceReader<T, SplitT2> secondReader;
	private boolean inFirstSourceMode = true;

	public HybridSourceReader(
		SourceReaderContext context,
		SourceReader<T, SplitT1> firstReader,
		SourceReader<T, SplitT2> secondReader) {
		this.context = context;
		this.firstReader = firstReader;
		this.secondReader = secondReader;
	}

	@Override
	public void start() {
		// eagerly initialize the readers, because we may not know
		// what's the current mode now if is from restore
		firstReader.start();
		secondReader.start();
	}

	@Override
	public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
		if (inFirstSourceMode) {
			InputStatus status = firstReader.pollNext(output);
			if (status == InputStatus.END_OF_INPUT) {
				// switch to next source reader
				switchSourceReader();
				// still return available because we have next readers
				return InputStatus.NOTHING_AVAILABLE;
			}
			return status;
		} else {
			return secondReader.pollNext(output);
		}
	}

	private void switchSourceReader() throws Exception {
		// TODO: do we need to postpone the close() after enumerator switching?
		firstReader.close();
		// TODO: handover offsets
		secondReader.start();
		// change the mode to second source mode
		inFirstSourceMode = false;
		// notify HybridSplitEnumerator that the SourceReader finishes reading.
		context.sendSourceEventToCoordinator(new SplitsFinishedEvent());
	}

	@Override
	public List<HybridSourceSplit<SplitT1, SplitT2>> snapshotState() {
		List<HybridSourceSplit<SplitT1, SplitT2>> splits = new ArrayList<>();
		if (inFirstSourceMode) {
			List<SplitT1> firstSourceSplits = firstReader.snapshotState();
			for (SplitT1 split : firstSourceSplits) {
				splits.add(HybridSourceSplit.forFirstSource(split));
			}
		} else {
			List<SplitT2> secondSourceSplits = secondReader.snapshotState();
			for (SplitT2 split : secondSourceSplits) {
				splits.add(HybridSourceSplit.forSecondSource(split));
			}
		}
		return splits;
	}

	@Override
	public CompletableFuture<Void> isAvailable() {
		if (inFirstSourceMode) {
			return firstReader.isAvailable();
		} else {
			return secondReader.isAvailable();
		}
	}

	@Override
	public void addSplits(List<HybridSourceSplit<SplitT1, SplitT2>> splits) {
		// restored from state, reset the source mode
		inFirstSourceMode = splits.stream().allMatch(HybridSourceSplit::isFirstSourceSplit);

		if (inFirstSourceMode) {
			List<SplitT1> underlyingSplits = new ArrayList<>();
			for (HybridSourceSplit<SplitT1, SplitT2> split : splits) {
				checkArgument(split.isFirstSourceSplit());
				underlyingSplits.add(split.getFirstSourceSplit());
			}
			firstReader.addSplits(underlyingSplits);
		} else {
			List<SplitT2> underlyingSplits = new ArrayList<>();
			for (HybridSourceSplit<SplitT1, SplitT2> split : splits) {
				checkArgument(split.isSecondSourceSplit());
				underlyingSplits.add(split.getSecondSourceSplit());
			}
			secondReader.addSplits(underlyingSplits);
		}
	}

	@Override
	public void handleSourceEvents(SourceEvent sourceEvent) {
		if (inFirstSourceMode) {
			firstReader.handleSourceEvents(sourceEvent);
		} else {
			secondReader.handleSourceEvents(sourceEvent);
		}
	}

	@Override
	public void close() throws Exception {
		firstReader.close();
		secondReader.close();
	}
}
