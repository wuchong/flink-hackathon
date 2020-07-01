/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.connector.file.src;

import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.AbstractCsvInputFormat;
import org.apache.flink.formats.csv.CsvRowDataDeserializationSchema;
import org.apache.flink.formats.csv.CsvRowSchemaConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MappingIterator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.apache.commons.compress.utils.BoundedInputStream;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;

public class RowDataCsvReader implements FormatSplitReader<RowData> {

	private final FSDataInputStream in;
	private final CsvRowDataDeserializationSchema.DeserializationRuntimeConverter runtimeConverter;
	private final MappingIterator<JsonNode> iterator;

	public RowDataCsvReader(RowType rowType, Path path, long offset, long length) throws IOException {
		FileSystem fs = path.getFileSystem();
		this.in = fs.open(path);
		this.in.seek(offset);
		InputStream csvInputStream = in;

		long csvStart = offset;
		if (offset != 0) {
			csvStart = findNextLineStartOffset();
		}

		if (length != -1L) {
			in.seek(offset + length);
			long nextLineStartOffset = findNextLineStartOffset();
			in.seek(csvStart);
			csvInputStream = new BoundedInputStream(in, nextLineStartOffset - csvStart);
		} else {
			in.seek(csvStart);
		}

		CsvRowDataDeserializationSchema.Builder builder = new CsvRowDataDeserializationSchema.Builder(
				rowType, new GenericTypeInfo<>(RowData.class))
				.setIgnoreParseErrors(true);
		this.runtimeConverter = builder.build().createRowConverter(rowType, true);

		CsvSchema csvSchema = CsvRowSchemaConverter.convert(rowType);
		this.iterator = new CsvMapper()
				.readerFor(JsonNode.class)
				.with(csvSchema)
				.readValues(csvInputStream);
	}

	private long findNextLineStartOffset() throws IOException {
		return AbstractCsvInputFormat.findNextLineStartOffset(in, false, new byte[0]);
	}

	@Nullable
	@Override
	public RowData read() {
		GenericRowData csvRow = null;
		while (csvRow == null) {
			try {
				JsonNode root = iterator.nextValue();
				csvRow = (GenericRowData) runtimeConverter.convert(root);
			} catch (NoSuchElementException e) {
				return null;
			} catch (Throwable ignored) {
			}
		}
		return csvRow;
	}

	@Override
	public void close() throws IOException {
		in.close();
	}
}
