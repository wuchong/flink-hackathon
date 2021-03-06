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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

/**
 * Split reader to read record from files. The reader is only responsible for reading the data
 * of a single split.
 */
public interface FormatSplitReader<T> extends Closeable {

	@Nullable
	T read() throws IOException;

	/**
	 * Seek to a particular row number.
	 */
	default void seekToRow(long rowCount) throws IOException {
		for (int i = 0; i < rowCount; i++) {
			T t = read();
			if (t == null) {
				throw new RuntimeException("Seek too many rows.");
			}
		}
	}

	interface Factory<T> extends Serializable {
		FormatSplitReader<T> createReader(Configuration config, Path filePath, long offset, long length) throws IOException;
	}
}
