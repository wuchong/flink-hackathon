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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The source implementation for hybrid two or more sources.
 */
public class HybridSource<T, SplitT1 extends SourceSplit, SplitT2 extends SourceSplit, EnumChkT1, EnumChkT2,
	SwitchStateT>
	implements Source<T, HybridSourceSplit<SplitT1, SplitT2>, HybridSourceEnumState<EnumChkT1, EnumChkT2>> {

	private static final long serialVersionUID = 3590970584442381319L;

	private final SwitchableSource<T, SplitT1, EnumChkT1, ?, SwitchStateT> firstSource;
	private final SwitchableSource<T, SplitT2, EnumChkT2, SwitchStateT, ?> secondSource;

	HybridSource(
		SwitchableSource<T, SplitT1, EnumChkT1, ?, SwitchStateT> firstSource,
		SwitchableSource<T, SplitT2, EnumChkT2, SwitchStateT, ?> secondSource) {
		checkArgument(firstSource.getBoundedness() == Boundedness.BOUNDED);
		this.firstSource = firstSource;
		this.secondSource = secondSource;
	}

	/**
	 * Get a HybridSourceBuilder to build a {@link HybridSource}.
	 *
	 * @return a hybrid source builder.
	 */
	public static <OUT, SplitT1 extends SourceSplit, SplitT2 extends SourceSplit, EnumChkT1, EnumChkT2, SwitchT>
	HybridSourceBuilder<OUT, SplitT1, SplitT2, EnumChkT1, EnumChkT2, SwitchT> builder() {
		return new HybridSourceBuilder<>();
	}

	@Override
	public Boundedness getBoundedness() {
		return secondSource.getBoundedness();
	}

	@Override
	public SourceReader<T, HybridSourceSplit<SplitT1, SplitT2>> createReader(SourceReaderContext readerContext) {
		return new HybridSourceReader<>(
			readerContext,
			firstSource.createReader(readerContext),
			secondSource.createReader(readerContext));
	}

	@Override
	public SplitEnumerator<HybridSourceSplit<SplitT1, SplitT2>, HybridSourceEnumState<EnumChkT1, EnumChkT2>> createEnumerator(SplitEnumeratorContext<HybridSourceSplit<SplitT1, SplitT2>> enumContext) {
		Map<Integer, Boolean> readerIdToCompletion = new HashMap<>();
		SplitEnumeratorContext<SplitT1> firstEnumContext = new DelegatedSplitEnumeratorContext.FirstSourceSplitEnumeratorContext<>(enumContext, readerIdToCompletion);
		SplitEnumeratorContext<SplitT2> secondEnumContext = new DelegatedSplitEnumeratorContext.SecondSourceSplitEnumeratorContext<>(enumContext);
		SwitchableSplitEnumerator<SplitT1, EnumChkT1, ?, SwitchStateT> firstEnumerator = firstSource.createEnumerator(firstEnumContext);
		SwitchableSplitEnumerator<SplitT2, EnumChkT2, SwitchStateT, ?> secondEnumerator = secondSource.createEnumerator(secondEnumContext);
		return new HybridSplitEnumerator<>(enumContext, firstEnumerator, secondEnumerator, readerIdToCompletion);
	}

	@Override
	public SplitEnumerator<HybridSourceSplit<SplitT1, SplitT2>, HybridSourceEnumState<EnumChkT1, EnumChkT2>> restoreEnumerator(
		SplitEnumeratorContext<HybridSourceSplit<SplitT1, SplitT2>> enumContext,
		HybridSourceEnumState<EnumChkT1, EnumChkT2> checkpoint) throws IOException {
		Map<Integer, Boolean> readerIdToCompletion = new HashMap<>();
		SplitEnumeratorContext<SplitT1> firstEnumContext = new DelegatedSplitEnumeratorContext.FirstSourceSplitEnumeratorContext<>(enumContext, readerIdToCompletion);
		SplitEnumeratorContext<SplitT2> secondEnumContext = new DelegatedSplitEnumeratorContext.SecondSourceSplitEnumeratorContext<>(enumContext);
		boolean inFirstSourceMode = checkpoint.isFirstSourceEnumState();
		EnumChkT1 firstEnumState = checkpoint.getFirstSourceEnumState();
		EnumChkT2 secondEnumState = checkpoint.getSecondSourceEnumState();

		SwitchableSplitEnumerator<SplitT1, EnumChkT1, ?, SwitchStateT> firstEnumerator;
		SwitchableSplitEnumerator<SplitT2, EnumChkT2, SwitchStateT, ?> secondEnumerator;
		if (inFirstSourceMode) {
			firstEnumerator = firstSource.restoreEnumerator(firstEnumContext, firstEnumState);
			// do not need to restore second enumerator
			secondEnumerator = secondSource.createEnumerator(secondEnumContext);
		} else {
			// do not need to restore first enumerator
			firstEnumerator = firstSource.createEnumerator(firstEnumContext);
			secondEnumerator = secondSource.restoreEnumerator(secondEnumContext, secondEnumState);
		}
		return new HybridSplitEnumerator<>(enumContext, firstEnumerator, secondEnumerator, readerIdToCompletion, inFirstSourceMode);
	}

	@Override
	public SimpleVersionedSerializer<HybridSourceSplit<SplitT1, SplitT2>> getSplitSerializer() {
		SimpleVersionedSerializer<SplitT1> firstSplitSerDe = firstSource.getSplitSerializer();
		SimpleVersionedSerializer<SplitT2> secondSplitSerDe = secondSource.getSplitSerializer();
		return new HybridSourceSplitSerializer<>(firstSplitSerDe, secondSplitSerDe);
	}

	@Override
	public SimpleVersionedSerializer<HybridSourceEnumState<EnumChkT1, EnumChkT2>> getEnumeratorCheckpointSerializer() {
		SimpleVersionedSerializer<EnumChkT1> firstSplitSerDe = firstSource.getEnumeratorCheckpointSerializer();
		SimpleVersionedSerializer<EnumChkT2> secondSplitSerDe = secondSource.getEnumeratorCheckpointSerializer();
		return new HybridSourceEnumStateSerializer<>(firstSplitSerDe, secondSplitSerDe);
	}
}
