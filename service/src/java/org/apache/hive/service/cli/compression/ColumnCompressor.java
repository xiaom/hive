package org.apache.hive.service.cli.compression;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hive.service.cli.Column;


@InterfaceAudience.Private
@InterfaceStability.Unstable
/*
 * An interface that can be implemented to implement a ResultSet Compressor
 * isCompressible(Column col) is used to check if a column, of any given type and size, 
 * can be compressed or not by an external compressor
 * compress(Column col) would accept a column as argument and return a byte array
 * getVendor() and getCompressorSet() are used to inform the serviceLoader 
 * about the vendor and compressorSet names of the plugin
 */
public interface ColumnCompressor {
  /**
   * Before calling the compress() method, should check if the column is compressable by any
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
   * @return vendorname
   */
  public String getVendor();
  /**
   * Used to register a compressorSet name of the plugin with Service Loader.
   * @return compressorSet as a string
   */
  public String getCompressorSet();
}

