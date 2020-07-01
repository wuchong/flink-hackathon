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

package org.apache.flink.connector.file.src;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.event.RequestSplitEvent;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * A {@link SourceReader} that read records from {@link FileSourceSplit}.
 */
public class FileSourceReader<T> extends SingleThreadMultiplexSourceReaderBase<Tuple2<T, Long>, T, FileSourceSplit, FileSourceSplitState> {

	public FileSourceReader(
			FutureNotifier futureNotifier,
			FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple2<T, Long>>> elementsQueue,
			Supplier<SplitReader<Tuple2<T, Long>, FileSourceSplit>> splitReaderSupplier,
			RecordEmitter<Tuple2<T, Long>, T, FileSourceSplitState> recordEmitter,
			Configuration config,
			SourceReaderContext context) {
		super(futureNotifier, elementsQueue, splitReaderSupplier, recordEmitter, config, context);
	}

	private void requestSplit() {
		// TODO So, where can get hostname?
		context.sendSourceEventToCoordinator(new RequestSplitEvent(""));
	}

	@Override
	public void start() {
		requestSplit();
	}

	@Override
	protected void onSplitFinished(Collection<String> finishedSplitIds) {
		requestSplit();
	}

	@Override
	protected FileSourceSplitState initializedState(FileSourceSplit split) {
		return new FileSourceSplitState(split);
	}

	@Override
	protected FileSourceSplit toSplitType(String splitId, FileSourceSplitState splitState) {
		return splitState.toFileSourceSplit();
	}
}
