package org.apache.hive.service.cli.compression;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hive.service.cli.Column;


/**
 * An interface that can be implemented to implement a ResultSet Compressor
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ColumnCompressor {

  /**
   * Before calling the compress() method, should check if the column is compressible by any
   * given plugin.
   *
   * @Param col Column to be checked
   * @return boolean which says yes/no
   */
  public boolean isCompressible(Column col);

  /**
   * Compresses a given column.
   *
   * @Param col A column to be compressed
   * @return byte array with compressed data
   */
  public byte[] compress(Column col);

  /**
   * Used to register a vendor name of the plugin with Service Loader.
   * @return vendor name
   */
  public String getVendor();

  /**
   * Used to register a compressorSet name of the plugin with Service Loader.
   * @return compressorSet as a string
   */
  public String getCompressorSet();
}
