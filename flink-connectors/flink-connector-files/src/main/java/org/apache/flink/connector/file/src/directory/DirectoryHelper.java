package org.apache.flink.connector.file.src.directory;

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
import static org.apache.flink.table.utils.PartitionPathUtils.extractPartitionValues;

public class DirectoryHelper {

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

	/**
	 * TODO go to partition...
	 */
	public static Path[] listDirectory(
			Path path,
			boolean filterBySuccessFile,
			String successFileName) throws IOException {
		if (!filterBySuccessFile) {
			return new Path[] {path};
		}

		FileSystem fileSystem = path.getFileSystem();
		FileStatus[] fileStatuses = fileSystem.listStatus(path);
		List<Path> paths = new ArrayList<>();
		for (FileStatus status : fileStatuses) {
			if (status.isDir() && fileSystem.exists(new Path(status.getPath(), successFileName))) {
				paths.add(status.getPath());
			}
		}
		return paths.toArray(new Path[0]);
	}

	public static long getEndState(Path[] paths) {
		return Arrays.stream(paths)
				.map(DirectoryHelper::extractDirectoryTime)
				.max(Long::compareTo)
				.orElse(Long.MIN_VALUE);
	}

	private static long extractDirectoryTime(Path path) {
		return toMills(toLocalDateTime(extractPartitionValues(path).get(0)));
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
