package org.apache.hive.service.cli;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


@InterfaceAudience.Private
@InterfaceStability.Unstable
/*
 * An interface that can be implemented to implement a ResultSet Compressor
 * isCompressable(Column col) is used to check if a column, of any given type and size, can be compressed or not by an external compressor
 * compress(Column col) would accept a column as argument and return a byte array
 */
public interface ColumnCompressor {
  public boolean isCompressable(Column col);
  public byte[] compress(Column col);
}

