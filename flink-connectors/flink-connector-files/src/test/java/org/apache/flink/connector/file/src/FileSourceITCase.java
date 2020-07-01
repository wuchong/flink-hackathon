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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.FileUtils;

import org.apache.commons.collections.IteratorUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 */
public class FileSourceITCase extends AbstractTestBase {

	private static final TableSchema TEST_SCHEMA = TableSchema.builder()
			.field("f0", DataTypes.STRING())
			.field("f1", DataTypes.BIGINT())
			.field("f2", DataTypes.BIGINT())
			.build();

	private Path path;

	@Before
	public void before() throws IOException {
		String[] inputData = new String[] {"A", "B", "C", "D", "E"};
		File file = TEMPORARY_FOLDER.newFile();
		FileUtils.writeFileUtf8(file, String.join("\n", inputData));
		this.path = new Path(file.toURI().toString());
	}

	@Test
	public void test() throws Exception {
		FileSource<String> source = new FileSource<>(path);
		source.setFormatSplitReader(new TestTextSplitReader.Factory());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> stream = env.fromSource(
				source,
				WatermarkStrategy.noWatermarks(),
				"file-source")
				.returns(String.class);
		Iterator<String> iter = DataStreamUtils.collect(stream);
		System.out.println(IteratorUtils.toList(iter));
	}

	@Test
	public void testTable() throws Exception {


		Map<String, String> properties = new HashMap<>();
		properties.put("connector", "filesystem");
		properties.put("path", "filesystem");

		DynamicTableSink sink = FactoryUtil.createTableSink(
				null,
				ObjectIdentifier.of("", "", ""),
				new CatalogTableImpl(TEST_SCHEMA, properties, ""),
				new Configuration(),
				Thread.currentThread().getContextClassLoader());
	}
}
