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

package org.apache.flink.api.connector.source.hybrid;

import org.apache.flink.api.connector.source.SourceSplit;

/**
 * The base builder class for {@link HybridSource} to make it easier for the users to construct.
 */
public class HybridSourceBuilder<OUT, SplitT1 extends SourceSplit, SplitT2 extends SourceSplit, EnumChkT1, EnumChkT2, SwitchStateT> {

	private SwitchableSource<OUT, SplitT1, EnumChkT1, ?, SwitchStateT> firstSource;
	private SwitchableSource<OUT, SplitT2, EnumChkT2, SwitchStateT, ?> secondSource;

	public HybridSourceBuilder<OUT, SplitT1, SplitT2, EnumChkT1, EnumChkT2, SwitchStateT> addFirstSource(SwitchableSource<OUT, SplitT1, EnumChkT1, ?, SwitchStateT> firstSource) {
		this.firstSource = firstSource;
		return this;
	}

	public HybridSourceBuilder<OUT, SplitT1, SplitT2, EnumChkT1, EnumChkT2, SwitchStateT> addSecondSource(SwitchableSource<OUT, SplitT2, EnumChkT2, SwitchStateT, ?> secondSource) {
		this.secondSource = secondSource;
		return this;
	}

	public HybridSource<OUT, SplitT1, SplitT2, EnumChkT1, EnumChkT2, SwitchStateT> build() {
		return new HybridSource<>(firstSource, secondSource);
	}
}
