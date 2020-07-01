/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.connector.file.src;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.hybrid.SwitchableSource;
import org.apache.flink.api.connector.source.hybrid.SwitchableSplitEnumerator;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.Collection;

/**
 * The Source implementation of Kafka.
 *
 * @param <OUT> the end state type of the source.
 */
public class FileSwitchableSource<OUT> extends FileSource<OUT>
	implements SwitchableSource<OUT, FileSourceSplit, PendingSplitsCheckpoint, Void, Long> {

	public FileSwitchableSource(Path... inputPaths) {
		super(inputPaths);
	}

	public FileSwitchableSource(Path[] inputPaths, FileEnumerator.Provider fileEnumerator, FileSplitAssigner.Provider splitAssigner) {
		super(inputPaths, fileEnumerator, splitAssigner);
	}

	@Override
	public SwitchableSplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint, Void, Long> createEnumerator(SplitEnumeratorContext<FileSourceSplit> enumContext) {
		final FileEnumerator enumerator = enumeratorFactory.create();
		final Collection<FileSourceSplit> splits;
		try {
			splits = enumerator.enumerateSplits(inputPaths, enumContext.currentParallelism());
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not enumerate file splits", e);
		}

		return createSwitchableSplitEnumerator(enumContext, splits);
	}

	@Override
	public SwitchableSplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint, Void, Long> restoreEnumerator(SplitEnumeratorContext<FileSourceSplit> enumContext, PendingSplitsCheckpoint checkpoint) throws IOException {
		return createSwitchableSplitEnumerator(enumContext, checkpoint.getSplits());
	}

	private SwitchableSplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint, Void, Long> createSwitchableSplitEnumerator(
		SplitEnumeratorContext<FileSourceSplit> context,
		Collection<FileSourceSplit> splits) {

		final FileSplitAssigner splitAssigner = assignerFactory.create(splits);
		return new StaticFileSwitchableSplitEnumerator(context, splitAssigner);
	}
}
