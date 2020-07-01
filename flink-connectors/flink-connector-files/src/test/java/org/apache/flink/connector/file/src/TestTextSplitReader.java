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

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/**
 */
public class TestTextSplitReader implements FormatSplitReader<String> {

	private TextInputFormat format;

	public TestTextSplitReader(
			Configuration config,
			Path filePath,
			long offset,
			long length) throws IOException {
		this.format = new TextInputFormat(filePath);
		this.format.configure(config);
		this.format.open(new FileInputSplit(0, filePath, offset, length, null));
	}

	@Override
	public String read() throws IOException {
		if (this.format.reachedEnd()) {
			return null;
		}

		return this.format.nextRecord("");
	}

	@Override
	public void close() throws IOException {
		this.format.close();
	}

	/**
	 */
	public static class Factory implements FormatSplitReader.Factory<String> {

		@Override
		public FormatSplitReader<String> createReader(
				Configuration config,
				Path filePath,
				long offset,
				long length) throws IOException {
			return new TestTextSplitReader(config, filePath, offset, length);
		}
	}
}
