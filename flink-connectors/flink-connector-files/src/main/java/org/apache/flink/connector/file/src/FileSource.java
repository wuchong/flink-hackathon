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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * WIP of the file source.
 *
 * @param <T> The type of the events/records produced by this source.
 */
public class FileSource<T> implements Source<T, FileSourceSplit, PendingSplitsCheckpoint> {

	private static final long serialVersionUID = 1L;

	protected final Path[] inputPaths;

	protected final FileEnumerator.Provider enumeratorFactory;

	protected final FileSplitAssigner.Provider assignerFactory;

	private Configuration config = new Configuration();

	private FormatSplitReader.Factory<T> factory;

	// ------------------------------------------------------------------------
	//  (Convenience) Constructors
	// ------------------------------------------------------------------------

	/**
	 * Simple convenience constructor to create a simple FileSource with default behavior.
	 * For any source instantiation that configures more settings and components, use the
	 * {@link FileSourceBuilder}.
	 *
	 * <p>The "default behavior" is the same as when using the {@code FileSourceBuilder} and not
	 * specifying anything outside the necessary parameters.
	 *
	 * <p>This constructor is mainly here for discoverability.
	 */
	public FileSource(final Path... inputPaths) {
		this(
			inputPaths,
			FileSourceBuilder.DEFAULT_FILE_ENUMERATOR,
			FileSourceBuilder.DEFAULT_SPLIT_ASSIGNER);
	}

	FileSource(
			final Path[] inputPaths,
			final FileEnumerator.Provider fileEnumerator,
			final FileSplitAssigner.Provider splitAssigner) {

		this.inputPaths = checkNotNull(inputPaths);
		checkArgument(inputPaths.length > 0, "Source must have non-empty input paths list");

		this.enumeratorFactory = checkNotNull(fileEnumerator);
		this.assignerFactory = checkNotNull(splitAssigner);
	}

	// ------------------------------------------------------------------------
	//  Source API Methods
	// ------------------------------------------------------------------------

	public void setFormatSplitReader(FormatSplitReader.Factory<T> factory) {
		this.factory = factory;
	}

	public void setConfig(Configuration config) {
		this.config = config;
	}

	@Override
	public Boundedness getBoundedness() {
		// the first version is bounded only
		return Boundedness.BOUNDED;
	}

	public Path[] getInputPaths() {
		return inputPaths;
	}

	@Override
	public SourceReader<T, FileSourceSplit> createReader(SourceReaderContext readerContext) {
		FutureNotifier futureNotifier = new FutureNotifier();
		FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple2<T, Long>>> elementsQueue =
				new FutureCompletingBlockingQueue<>(futureNotifier);
		Supplier<SplitReader<Tuple2<T, Long>, FileSourceSplit>> splitReaderSupplier =
				() -> new FileSourceSplitReader<>(config, factory);
		FileSourceRecordEmitter<T> recordEmitter = new FileSourceRecordEmitter<>();

		return new FileSourceReader<>(
				futureNotifier,
				elementsQueue,
				splitReaderSupplier,
				recordEmitter,
				config,
				readerContext);
	}

	@Override
	public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> createEnumerator(
			SplitEnumeratorContext<FileSourceSplit> enumContext) {

		final FileEnumerator enumerator = enumeratorFactory.create();
		final Collection<FileSourceSplit> splits;

		// TODO - in the next cleanup pass, we should try to remove the need to "wrap unchecked" here
		try {
			splits = enumerator.enumerateSplits(inputPaths, enumContext.currentParallelism());
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not enumerate file splits", e);
		}

		return createSplitEnumerator(enumContext, splits);
	}

	@Override
	public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
			SplitEnumeratorContext<FileSourceSplit> enumContext,
			PendingSplitsCheckpoint checkpoint) throws IOException {

		return createSplitEnumerator(enumContext, checkpoint.getSplits());
	}

	@Override
	public SimpleVersionedSerializer<FileSourceSplit> getSplitSerializer() {
		return FileSourceSplitSerializer.INSTANCE;
	}

	@Override
	public SimpleVersionedSerializer<PendingSplitsCheckpoint> getEnumeratorCheckpointSerializer() {
		return PendingSplitsCheckpointSerializer.INSTANCE;
	}

	// ------------------------------------------------------------------------
	//  helpers
	// ------------------------------------------------------------------------

	private SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> createSplitEnumerator(
			SplitEnumeratorContext<FileSourceSplit> context,
			Collection<FileSourceSplit> splits) {

		final FileSplitAssigner splitAssigner = assignerFactory.create(splits);
		return new StaticFileSplitEnumerator(context, splitAssigner);
	}
}
