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

package org.apache.flink.table.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.connector.source.DynamicTableSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

public class HybridTableSourceFactory implements DynamicTableSourceFactory {

	private static final String HISTORICAL = "historical";
	private static final String INCREMENTAL = "incremental";

	public static final ConfigOption<String> HISTORICAL_CONNECTOR = key(HISTORICAL + ".connector")
		.stringType()
		.noDefaultValue();

	public static final ConfigOption<String> INCREMENTAL_CONNECTOR = key(INCREMENTAL + ".connector")
		.stringType()
		.noDefaultValue();

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		ReadableConfig options = helper.getOptions();
		String historicalId = options.get(HISTORICAL_CONNECTOR);
		String incrementalId = options.get(INCREMENTAL_CONNECTOR);
		DynamicTableSource historicalSource = getSwitchTableSource(
			context,
			historicalId,
			HISTORICAL + "." + historicalId);
		DynamicTableSource incrementalSource = getSwitchTableSource(
			context,
			incrementalId,
			INCREMENTAL + "." + incrementalId);
		return new HybridTableSource(
			historicalSource,
			incrementalSource
		);
	}

	public DynamicTableSource getSwitchTableSource(Context context, String connectorIdentifier, String prefix) {
		CatalogTable table = context.getCatalogTable();
		Map<String, String> options = getPrefixOptions(table.getOptions(), prefix + ".");
		options.put(FactoryUtil.CONNECTOR.key(), connectorIdentifier);
		CatalogTableImpl switchTable = new CatalogTableImpl(
			table.getSchema(),
			table.getPartitionKeys(),
			options,
			table.getComment());
		return FactoryUtil.createTableSource(
			null,
			context.getObjectIdentifier(),
			switchTable,
			context.getConfiguration(),
			context.getClassLoader());
	}

	public Map<String, String> getPrefixOptions(Map<String, String> options, String prefix) {
		Map<String, String> newOptions = new HashMap<>();
		options.forEach((k, v) -> {
			if (k.startsWith(prefix)) {
				String newKey = k.replace(prefix, "");
				newOptions.put(newKey, v);
			}
		});
		return newOptions;
	}

	@Override
	public String factoryIdentifier() {
		return "hybrid";
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(HISTORICAL_CONNECTOR);
		options.add(INCREMENTAL_CONNECTOR);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		return new HashSet<>();
	}
}
