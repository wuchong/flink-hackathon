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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * A {@link SplitReader} to read file split.
 *
 * <p>NOTE:
 * TODO Current implementation is fetching per record, very low performance.
 * TODO Collection (Can be iterator?) in RecordsWithSplitIds makes hard to providing a batch view for vectorization format like parquet.
 */
public class FileSourceSplitReader<T> implements SplitReader<Tuple2<T, Long>, FileSourceSplit> {

	private static final Logger LOG = LoggerFactory.getLogger(FileSourceSplitReader.class);

	private final Configuration config;
	private final FormatSplitReader.Factory<T> factory;

	private final Queue<FileSourceSplit> splits;

	/** Single Reader Fields **/

	private FormatSplitReader<T> currentReader;
	private FileSourceSplit currentSplit;
	private long currentReadCount = 0L;

	public FileSourceSplitReader(Configuration config, FormatSplitReader.Factory<T> factory) {
		this.config = config;
		this.factory = factory;
		this.splits = new LinkedList<>();
	}

	private boolean nextSplit() throws IOException {
		if (this.currentReader != null) {
			// TODO where to close last split?
			this.currentReader.close();
		}

		FileSourceSplit split = splits.poll();
		if (split == null) {
			return false;
		}

		this.currentReader = factory.createReader(config, split.path(), split.offset(), split.length());
		if (split.currentRowCount() != 0) {
			this.currentReader.seekToRow(split.currentRowCount());
		}
		this.currentReadCount = split.currentRowCount();
		this.currentSplit = split;
		return true;
	}

	@Override
	public RecordsWithSplitIds<Tuple2<T, Long>> fetch() throws IOException {
		FileSplitRecords<Tuple2<T, Long>> recordsBySplits = new FileSplitRecords<>();
		while (true) {
			if (currentReader == null && !nextSplit()) {
				break;
			}
			T record;
			if ((record = currentReader.read()) == null) {
				recordsBySplits.addFinishedSplit(currentSplit.splitId());
				if (!nextSplit()) {
					break;
				}
				continue;
			}
			recordsBySplits.recordsForSplit(currentSplit.splitId())
					.add(new Tuple2<>(record, currentReadCount));
			currentReadCount++;
			return recordsBySplits;
		}
		return recordsBySplits;
	}

	@Override
	public void handleSplitsChanges(Queue<SplitsChange<FileSourceSplit>> splitsChanges) {
		SplitsChange<FileSourceSplit> splitChange;

		// Get all the partition assignments and stopping offsets.
		while ((splitChange = splitsChanges.poll()) != null) {
			if (!(splitChange instanceof SplitsAddition)) {
				throw new UnsupportedOperationException(String.format(
						"The SplitChange type of %s is not supported.", splitChange.getClass()));
			}
			LOG.debug("Handling split change {}", splitChange);
			splits.addAll(splitChange.splits());
		}
	}

	@Override
	public void wakeUp() {
	}

	// ---------------- private helper class ------------------------

	private static class FileSplitRecords<T> implements RecordsWithSplitIds<T> {

		private final Map<String, Collection<T>> recordsBySplits;
		private final Set<String> finishedSplits;

		private FileSplitRecords() {
			this.recordsBySplits = new HashMap<>();
			this.finishedSplits = new HashSet<>();
		}

		private Collection<T> recordsForSplit(String splitId) {
			return recordsBySplits.computeIfAbsent(splitId, id -> new ArrayList<>());
		}

		private void addFinishedSplit(String splitId) {
			finishedSplits.add(splitId);
		}

		@Override
		public Collection<String> splitIds() {
			return recordsBySplits.keySet();
		}

		@Override
		public Map<String, Collection<T>> recordsBySplits() {
			return recordsBySplits;
		}

		@Override
		public Set<String> finishedSplits() {
			return finishedSplits;
		}
	}
}
