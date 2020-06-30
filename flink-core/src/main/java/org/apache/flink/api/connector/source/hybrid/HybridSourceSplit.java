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

import javax.annotation.Nullable;

/**
 * The source split for {@link HybridSource}.
 */
public class HybridSourceSplit<SplitT1 extends SourceSplit, SplitT2 extends SourceSplit> implements SourceSplit {

	public static <T1 extends SourceSplit, T2 extends SourceSplit> HybridSourceSplit<T1, T2> forFirstSource(T1 split) {
		return new HybridSourceSplit<>(split, null);
	}

	public static <T1 extends SourceSplit, T2 extends SourceSplit> HybridSourceSplit<T1, T2> forSecondSource(T2 split) {
		return new HybridSourceSplit<>(null, split);
	}

	@Nullable
	private final SplitT1 firstSourceSplit;

	@Nullable
	private final SplitT2 secondSourceSplit;

	HybridSourceSplit(@Nullable SplitT1 firstSourceSplit, @Nullable SplitT2 secondSourceSplit) {
		this.firstSourceSplit = firstSourceSplit;
		this.secondSourceSplit = secondSourceSplit;
	}

	public boolean isFirstSourceSplit() {
		return firstSourceSplit != null;
	}

	public boolean isSecondSourceSplit() {
		return secondSourceSplit != null;
	}

	public SplitT1 getFirstSourceSplit() {
		return firstSourceSplit;
	}

	public SplitT2 getSecondSourceSplit() {
		return secondSourceSplit;
	}

	@Override
	public String splitId() {
		if (firstSourceSplit != null) {
			return firstSourceSplit.splitId();
		} else if (secondSourceSplit != null) {
			return secondSourceSplit.splitId();
		} else {
			throw new RuntimeException("firstSourceSplit and secondSourceSplit should not both be null.");
		}
	}
}
