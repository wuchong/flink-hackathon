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

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.hybrid.HybridSource;
import org.apache.flink.api.connector.source.hybrid.SwitchableSource;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;

public class HybridTableSource implements ScanTableSource {

	private final ScanTableSource historicalSource;
	private final ScanTableSource incrementalSource;

	public HybridTableSource(DynamicTableSource historicalSource, DynamicTableSource incrementalSource) {
		this.historicalSource = (ScanTableSource) historicalSource;
		this.incrementalSource = (ScanTableSource) incrementalSource;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return incrementalSource.getChangelogMode();
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		SourceProvider historicalProvider = (SourceProvider) historicalSource.getScanRuntimeProvider(runtimeProviderContext);
		SourceProvider incrementalProvider = (SourceProvider) incrementalSource.getScanRuntimeProvider(runtimeProviderContext);
		Source source = HybridSource.builder()
			.addFirstSource((SwitchableSource) historicalProvider.createSource())
			.addSecondSource((SwitchableSource) incrementalProvider.createSource())
			.build();
		return SourceProvider.of(source);
	}

	@Override
	public DynamicTableSource copy() {
		return new HybridTableSource(
			historicalSource.copy(),
			incrementalSource.copy()
		);
	}

	@Override
	public String asSummaryString() {
		return String.format("Hybrid[%s, %s]", historicalSource.asSummaryString(), incrementalSource.asSummaryString());
	}
}
