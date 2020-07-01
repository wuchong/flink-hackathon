package org.apache.flink.connector.file.src;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.SimpleSplitAssigner;
import org.apache.flink.connector.file.src.directory.DirectoryHelper;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

public class FileSourceTableFactory implements DynamicTableSourceFactory {

	public static final ConfigOption<String> PATH = key("path")
			.stringType()
			.noDefaultValue()
			.withDescription("The path of a directory");

	public static final ConfigOption<Boolean> DIRECTORY_FILTER_ENABLE = key("directory.filter.enable")
			.booleanType()
			.defaultValue(false)
			.withDescription("The path of a directory");

	public static final ConfigOption<String> DIRECTORY_SUCCESS_FILE_NAME =
			key("directory.success-file.name")
					.stringType()
					.defaultValue("_SUCCESS")
					.withDescription("The file name for success-file directory," +
							" default is '_SUCCESS'.");

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		ReadableConfig config = context.getConfiguration();
		Path path = new Path(config.getOptional(PATH).orElseThrow(() ->
				new ValidationException("Path should be not empty.")));
		boolean directoryFilter = config.get(DIRECTORY_FILTER_ENABLE);
		String successFileName = config.get(DIRECTORY_SUCCESS_FILE_NAME);

		FileSource<RowData> source;
		try {
			Path[] paths = DirectoryHelper.listDirectory(
					path, directoryFilter, successFileName);
			source = new FileSource<>(
					paths,
					FileSourceBuilder.DEFAULT_FILE_ENUMERATOR,
					new SwitchableSplitAssignerProvider(directoryFilter, paths));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		RowType rowType = (RowType) context.getCatalogTable().getSchema()
				.toPhysicalRowDataType().getLogicalType();

		String format = config.get(FactoryUtil.FORMAT);
		if (!"csv".equalsIgnoreCase(format)) {
			throw new UnsupportedOperationException();
		}
		// TODO to spi...
		source.setFormatSplitReader((config1, filePath, offset, length) ->
				new RowDataCsvReader(rowType, filePath, offset, length));

		return new FileDynamicTableSource(source);
	}

	@Override
	public String factoryIdentifier() {
		return "filesystem";
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(PATH);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(DIRECTORY_FILTER_ENABLE);
		options.add(DIRECTORY_SUCCESS_FILE_NAME);
		return options;
	}

	private static class SwitchableSplitAssignerProvider implements FileSplitAssigner.Provider {

		private final boolean directoryFilter;
		private final Path[] paths;

		private SwitchableSplitAssignerProvider(boolean directoryFilter, Path[] paths) {
			this.directoryFilter = directoryFilter;
			this.paths = paths;
		}

		@Override
		public FileSplitAssigner create(Collection<FileSourceSplit> initialSplits) {
			return new SimpleSplitAssigner(initialSplits) {

				@Override
				public Long switchEnd() {
					if (!directoryFilter) {
						throw new UnsupportedOperationException();
					}

					return DirectoryHelper.getEndState(paths);
				}
			};
		}
	}
}
