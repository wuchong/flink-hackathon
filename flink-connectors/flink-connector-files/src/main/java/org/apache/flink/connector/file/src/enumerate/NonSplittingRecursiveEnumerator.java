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

package org.apache.flink.connector.file.src.enumerate;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * This {@code FileEnumerator} enumerates all files under the given paths recursively.
 * Each file becomes one split; this enumerator does not split files into smaller "block" units.
 */
public class NonSplittingRecursiveEnumerator implements FileEnumerator {

	private int currentId = 0;

	@Override
	public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits) throws IOException {
		final ArrayList<FileSourceSplit> splits = new ArrayList<>();

		for (Path path : paths) {
			final FileSystem fs = path.getFileSystem();
			final FileStatus status = fs.getFileStatus(path);
			addSplitsForPath(status, fs, splits);
		}

		return splits;
	}

	private void addSplitsForPath(FileStatus fileStatus, FileSystem fs, ArrayList<FileSourceSplit> target) throws IOException {
		if (!fileStatus.isDir()) {
			target.add(convertToSourceSplit(fileStatus));
			return;
		}

		final FileStatus[] containedFiles = fs.listStatus(fileStatus.getPath());
		for (FileStatus containedStatus : containedFiles) {
			addSplitsForPath(containedStatus, fs, target);
		}
	}

	private FileSourceSplit convertToSourceSplit(FileStatus file) {
		final String id = toStringId(currentId++);
		return new FileSourceSplit(id, file.getPath(), 0, file.getLen());
	}

	private static String toStringId(int numericId) {
		return String.format("%06d", numericId);
	}
}
