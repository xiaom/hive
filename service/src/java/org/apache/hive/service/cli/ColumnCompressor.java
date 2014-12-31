package org.apache.hive.service.cli;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


@InterfaceAudience.Private
@InterfaceStability.Unstable

public interface ColumnCompressor {
	public byte[] compress(Column col);
}