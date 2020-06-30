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

import javax.annotation.Nullable;

/**
 * The enumerator checkpoint state for {@link HybridSource}.
 */
public class HybridSourceEnumState<EnumChkT1, EnumChkT2> {

	public static <T1, T2> HybridSourceEnumState<T1, T2> forFirstSource(T1 enumState) {
		return new HybridSourceEnumState<>(enumState, null);
	}

	public static <T1, T2> HybridSourceEnumState<T1, T2> forSecondSource(T2 enumState) {
		return new HybridSourceEnumState<>(null, enumState);
	}

	@Nullable
	private final EnumChkT1 firstSourceEnumState;
	@Nullable
	private final EnumChkT2 secondSourceEnumState;

	private HybridSourceEnumState(EnumChkT1 firstSourceEnumState, EnumChkT2 secondSourceEnumState) {
		this.firstSourceEnumState = firstSourceEnumState;
		this.secondSourceEnumState = secondSourceEnumState;
	}

	public boolean isFirstSourceEnumState() {
		return firstSourceEnumState != null;
	}

	public boolean isSecondSourceEnumState() {
		return secondSourceEnumState != null;
	}

	@Nullable
	public EnumChkT1 getFirstSourceEnumState() {
		return firstSourceEnumState;
	}

	@Nullable
	public EnumChkT2 getSecondSourceEnumState() {
		return secondSourceEnumState;
	}
}
