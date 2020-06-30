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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A checkpoint containing the currently pending splits that are not yet assigned.
 */
public final class PendingSplitsCheckpoint {

	/** The splits in the checkpoint. */
	private final Collection<FileSourceSplit> splits;

	/** The cached byte representation from the last serialization step. This helps to avoid
	 * paying repeated serialization cost for the same checkpoint object. This field is used
	 * by {@link PendingSplitsCheckpointSerializer}. */
	@Nullable
	byte[] serializedFormCache;

	private PendingSplitsCheckpoint(Collection<FileSourceSplit> splits) {
		this.splits = Collections.unmodifiableCollection(splits);
	}

	// ------------------------------------------------------------------------

	public Collection<FileSourceSplit> getSplits() {
		return splits;
	}

	@Override
	public String toString() {
		return super.toString();
	}

	// ------------------------------------------------------------------------
	//  factories
	// ------------------------------------------------------------------------

	public static PendingSplitsCheckpoint fromCollectionSnapshot(Collection<FileSourceSplit> splits) {
		checkNotNull(splits);

		// create a copy of the collection to make sure this checkpoint is immutable
		final Collection<FileSourceSplit> copy = new ArrayList<>(splits);
		return new PendingSplitsCheckpoint(copy);
	}

	static PendingSplitsCheckpoint reusingCollection(Collection<FileSourceSplit> splits) {
		return new PendingSplitsCheckpoint(splits);
	}
}
