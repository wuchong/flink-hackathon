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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The serializer for {@link HybridSourceEnumState}.
 */
public class HybridSourceEnumStateSerializer<EnumChkT1, EnumChkT2> implements SimpleVersionedSerializer<HybridSourceEnumState<EnumChkT1, EnumChkT2>> {

	private static final int CURRENT_VERSION = 0;
	private final SimpleVersionedSerializer<EnumChkT1> firstSourceEnumStateSerializer;
	private final SimpleVersionedSerializer<EnumChkT2> secondSourceEnumStateSerializer;

	public HybridSourceEnumStateSerializer(
		SimpleVersionedSerializer<EnumChkT1> firstSourceEnumStateSerializer,
		SimpleVersionedSerializer<EnumChkT2> secondSourceEnumStateSerializer) {
		this.firstSourceEnumStateSerializer = firstSourceEnumStateSerializer;
		this.secondSourceEnumStateSerializer = secondSourceEnumStateSerializer;
	}

	@Override
	public int getVersion() {
		// TODO: should consider first source version and second source version
		return CURRENT_VERSION;
	}

	@Override
	public byte[] serialize(HybridSourceEnumState<EnumChkT1, EnumChkT2> enumState) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
			 DataOutputStream out = new DataOutputStream(baos)) {
			boolean isFirstEnumState = enumState.isFirstSourceEnumState();
			out.writeBoolean(isFirstEnumState);
			if (isFirstEnumState) {
				EnumChkT1 firstEnumState = enumState.getFirstSourceEnumState();
				byte[] bytes = firstSourceEnumStateSerializer.serialize(firstEnumState);
				out.writeInt(bytes.length);
				out.write(bytes);
			} else {
				EnumChkT2 secondEnumState = enumState.getSecondSourceEnumState();
				byte[] bytes = secondSourceEnumStateSerializer.serialize(secondEnumState);
				out.writeInt(bytes.length);
				out.write(bytes);
			}
			return baos.toByteArray();
		}
	}

	@Override
	public HybridSourceEnumState<EnumChkT1, EnumChkT2> deserialize(int version, byte[] serialized) throws IOException {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
			 DataInputStream in = new DataInputStream(bais)) {
			boolean isFirstEnumState = in.readBoolean();
			int size = in.readInt();
			byte[] bytes = new byte[size];
			in.read(bytes);
			if (isFirstEnumState) {
				// TODO: use the correct version
				EnumChkT1 firstEnumState = firstSourceEnumStateSerializer.deserialize(version, bytes);
				return HybridSourceEnumState.forFirstSource(firstEnumState);
			} else {
				// TODO: use the correct version
				EnumChkT2 secondEnumState = secondSourceEnumStateSerializer.deserialize(version, bytes);
				return HybridSourceEnumState.forSecondSource(secondEnumState);
			}
		}
	}
}
