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
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The serializer for {@link SourceSplit}.
 */
public class HybridSourceSplitSerializer<SplitT1 extends SourceSplit, SplitT2 extends SourceSplit>
	implements SimpleVersionedSerializer<HybridSourceSplit<SplitT1, SplitT2>> {

	private static final int CURRENT_VERSION = 0;
	private final SimpleVersionedSerializer<SplitT1> firstSourceSplitSerializer;
	private final SimpleVersionedSerializer<SplitT2> secondSourceSplitSerializer;

	public HybridSourceSplitSerializer(
		SimpleVersionedSerializer<SplitT1> firstSourceSplitSerializer,
		SimpleVersionedSerializer<SplitT2> secondSourceSplitSerializer) {
		this.firstSourceSplitSerializer = firstSourceSplitSerializer;
		this.secondSourceSplitSerializer = secondSourceSplitSerializer;
	}

	@Override
	public int getVersion() {
		// TODO: should consider first source version and second source version
		return CURRENT_VERSION;
	}

	@Override
	public byte[] serialize(HybridSourceSplit<SplitT1, SplitT2> split) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
			 DataOutputStream out = new DataOutputStream(baos)) {
			out.writeBoolean(split.isFirstSourceSplit());
			if (split.isFirstSourceSplit()) {
				out.write(firstSourceSplitSerializer.serialize(split.getFirstSourceSplit()));
			} else {
				out.write(secondSourceSplitSerializer.serialize(split.getSecondSourceSplit()));
			}
			out.flush();
			return baos.toByteArray();
		}
	}

	@Override
	public HybridSourceSplit<SplitT1, SplitT2> deserialize(int version, byte[] serialized) throws IOException {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
			 DataInputStream in = new DataInputStream(bais)) {
			boolean isFirstSourceSplit = in.readBoolean();
			byte[] bytes = new byte[in.available()];
			if (isFirstSourceSplit) {
				// TODO: use the correct version
				SplitT1 split = firstSourceSplitSerializer.deserialize(version, bytes);
				return HybridSourceSplit.forFirstSource(split);
			} else {
				// TODO: use the correct version
				SplitT2 split = secondSourceSplitSerializer.deserialize(version, bytes);
				return HybridSourceSplit.forSecondSource(split);
			}
		}
	}
}
