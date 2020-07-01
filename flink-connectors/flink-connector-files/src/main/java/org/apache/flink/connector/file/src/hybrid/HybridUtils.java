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

package org.apache.flink.connector.file.src.hybrid;

import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;
import static org.apache.commons.lang3.time.DateUtils.MILLIS_PER_DAY;

public class HybridUtils {

	private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
			.appendValue(YEAR, 1, 10, SignStyle.NORMAL)
			.appendLiteral('-')
			.appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
			.appendLiteral('-')
			.appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
			.optionalStart()
			.appendLiteral(" ")
			.appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NORMAL)
			.appendLiteral(':')
			.appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NORMAL)
			.appendLiteral(':')
			.appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NORMAL)
			.optionalStart()
			.appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
			.optionalEnd()
			.optionalEnd()
			.toFormatter()
			.withResolverStyle(ResolverStyle.LENIENT);

	public static <T> FileSource<T> createHybridFileSource(Path path) throws IOException {
		return createHybridFileSource(path, "_SUCCESS");
	}

	public static <T> FileSource<T> createHybridFileSource(Path path, String successFile) throws IOException {
		FileSystem fileSystem = path.getFileSystem();
		FileStatus[] fileStatuses = fileSystem.listStatus(path);
		List<Path> paths = new ArrayList<>();
		for (FileStatus status : fileStatuses) {
			if (status.isDir() && fileSystem.exists(new Path(status.getPath(), successFile))) {
				paths.add(status.getPath());
			}
		}
		return new FileSource<>(paths.toArray(new Path[0]));
	}

	public static long getEndState(Path[] paths) {
		return Arrays.stream(paths)
				.map(HybridUtils::extractPartitionTime)
				.max(Long::compareTo)
				.orElse(Long.MIN_VALUE);
	}

	private static long extractPartitionTime(Path partitionPath) {
		return toMills(toLocalDateTime(partitionPath.getName()));
	}

	private static LocalDateTime toLocalDateTime(String timestampString) {
		return LocalDateTime.parse(timestampString, TIMESTAMP_FORMATTER);
	}

	private static long toMills(LocalDateTime dateTime) {
		long epochDay = dateTime.toLocalDate().toEpochDay();
		long nanoOfDay = dateTime.toLocalTime().toNanoOfDay();

		return epochDay * MILLIS_PER_DAY + nanoOfDay / 1_000_000;
	}
}
