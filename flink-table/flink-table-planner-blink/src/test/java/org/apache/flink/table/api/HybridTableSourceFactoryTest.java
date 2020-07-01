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

package org.apache.flink.table.api;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.HybridTableSource;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;

import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class HybridTableSourceFactoryTest {

	private static final String NAME = "name";
	private static final String COUNT = "count";
	private static final String TIME = "time";

	private static final TableSchema SOURCE_SCHEMA = TableSchema.builder()
		.field(NAME, DataTypes.STRING())
		.field(COUNT, DataTypes.BIGINT())
		.field(TIME, DataTypes.INT())
		.build();

	@Ignore
	@Test
	public void testFactory() {
		//Construct table source using options and table source factory
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
			"default",
			"default",
			"scanTable");
		Map<String, String> tableOptions = getAllOptions();
		CatalogTable catalogTable = new CatalogTableImpl(SOURCE_SCHEMA, tableOptions, "scanTable");
		final DynamicTableSource tableSource = FactoryUtil.createTableSource(null,
			objectIdentifier,
			catalogTable,
			new Configuration(),
			Thread.currentThread().getContextClassLoader());

		// Test commitOnCheckpoints flag should be false when do not set consumer group.
		assertThat(tableSource, instanceOf(HybridTableSource.class));
		ScanTableSource.ScanRuntimeProvider providerWithoutGroupId = ((HybridTableSource) tableSource)
			.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
		assertThat(providerWithoutGroupId, instanceOf(SourceProvider.class));
		final Source source = ((SourceProvider) providerWithoutGroupId).createSource();
		System.out.println(source);
	}

	private Map<String, String> getAllOptions() {
		Map<String, String> options = new HashMap<>();
		options.put("connector", "hybrid");
		options.put("historical.connector", "datagen");
		options.put("historical.datagen.rows-per-second", "1000");
		options.put("incremental.connector", "values");
		options.put("incremental.values.bounded", "false");
		return options;
	}

}
