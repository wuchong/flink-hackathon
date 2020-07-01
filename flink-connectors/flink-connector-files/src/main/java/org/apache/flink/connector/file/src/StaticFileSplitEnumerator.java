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

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.hybrid.SwitchableSplitEnumerator;
import org.apache.flink.connector.base.source.event.NoMoreSplitsEvent;
import org.apache.flink.connector.base.source.event.RequestSplitEvent;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A SplitEnumerator implementation for bounded / batch {@link FileSource} input.
 *
 * <p>This enumerator takes all files that are present in the configured input directories and assigns
 * them to the readers. Once all files are processed, the source is finished.
 *
 * <p>The implementation of this class is rather thin. The actual logic for creating the set of
 * FileSourceSplits to process, and the logic to decide which reader gets what split, are in
 * {@link FileEnumerator} and in {@link FileSplitAssigner}, respectively.
 *
 * <p>TODO For traditional batch jobs, it is better to assign all splits at the beginning.
 * This due to batch failover mechanism:
 * - Failover mechanism: Re-launch tasks when batch jobs tasks failed.
 * - Large parallelism to have many small tasks, they can be run very quickly, and can have a good
 * failover experience. And may have less slots, can not use dynamic split enumerator, otherwise,
 * many tasks will starve to death.
 */
public class StaticFileSplitEnumerator implements
	SwitchableSplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint, Void, Long> {

	private static final Logger LOG = LoggerFactory.getLogger(StaticFileSplitEnumerator.class);

	private final SplitEnumeratorContext<FileSourceSplit> context;

	private final FileSplitAssigner splitAssigner;

	// ------------------------------------------------------------------------

	public StaticFileSplitEnumerator(
		SplitEnumeratorContext<FileSourceSplit> context,
		FileSplitAssigner splitAssigner) {
		this.context = checkNotNull(context);
		this.splitAssigner = checkNotNull(splitAssigner);
	}

	@Override
	public void start() {
		// no resources to start
	}

	@Override
	public void close() throws IOException {
		// no resources to close
	}

	@Override
	public void addReader(int subtaskId) {
		// this source is purely lazy-pull-based, nothing to do upon registration
	}

	@Override
	public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
		if (sourceEvent instanceof RequestSplitEvent) {
			final RequestSplitEvent requestSplitEvent = (RequestSplitEvent) sourceEvent;
			assignNextEvents(subtaskId, requestSplitEvent.hostName());
		} else {
			LOG.error("Received unrecognized event: {}", sourceEvent);
		}
	}

	@Override
	public void addSplitsBack(List<FileSourceSplit> splits, int subtaskId) {
		LOG.debug("File Source Enumerator adds splits back: {}", splits);
		splitAssigner.addSplits(splits);
	}

	@Override
	public PendingSplitsCheckpoint snapshotState() throws Exception {
		return PendingSplitsCheckpoint.fromCollectionSnapshot(splitAssigner.remainingSplits());
	}

	// ------------------------------------------------------------------------

	private void assignNextEvents(int subtask, @Nullable String hostname) {
		if (LOG.isInfoEnabled()) {
			final String hostInfo = hostname == null ? "(no host locality info)" : "(on host '" + hostname + "')";
			LOG.info("Subtask {} {} is requesting a file source split", subtask, hostInfo);
		}

		final Optional<FileSourceSplit> nextSplit = splitAssigner.getNext(hostname);
		if (nextSplit.isPresent()) {
			final FileSourceSplit split = nextSplit.get();
			context.assignSplit(split, subtask);
			LOG.info("Assigned split to subtask {} : {}", subtask, split);
		} else {
			context.sendEventToSourceReader(subtask, new NoMoreSplitsEvent());
			LOG.info("No more splits available for subtask {}", subtask);
		}
	}

	@Override
	public void setStartState(Void startState) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long getEndState() {
		return splitAssigner.switchEnd();
	}
}
