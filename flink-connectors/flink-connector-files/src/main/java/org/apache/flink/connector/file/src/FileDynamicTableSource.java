package org.apache.flink.connector.file.src;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;

/**
 * TODO Support projection push down.
 * TODO Support partition pruning.
 * TODO Support filter push down.
 */
public class FileDynamicTableSource implements DynamicTableSource, ScanTableSource {

	private final FileSource<RowData> source;

	public FileDynamicTableSource(FileSource<RowData> source) {
		this.source = source;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		return SourceProvider.of(source);
	}

	@Override
	public DynamicTableSource copy() {
		return new FileDynamicTableSource(source);
	}

	@Override
	public String asSummaryString() {
		return "FileTableSource";
	}
}
