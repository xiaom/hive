package org.apache.hive.service.cli.compression;

import java.io.IOException;

import org.apache.hive.service.cli.Column;
import org.apache.hive.service.cli.compression.ColumnCompressor;
import org.xerial.snappy.Snappy;

public class SnappyIntCompressor implements ColumnCompressor {

  /**
   * Before calling the compress() method below, should check if the column is compressible by any
   * given plugin.
   * 
   * @Param col Column to be checked
   * @return boolean which says yes/no
   */
  @Override
  public boolean isCompressible(Column col) {
    switch (col.getType()) {
      case INT_TYPE:
        return true;
      default:
        return false;
    }
  }

  /**
   * Method provided for testing purposes. Decompresses a compressed byte array. should be
   * compressed using the compress() function below which uses the snappy JAR files. If Snappy JAR
   * file isn't used, this method will return the wrong result.
   */
  public static int[] decompress(byte[] inp) {
    try {
      return Snappy.uncompressIntArray(inp);
    } catch (IOException e) {
      return new int[0];
    }
  }

  /**
   * Compresses a given column.
   * 
   * @Param col A column to be compressed
   * @return byte array with compressed data
   */
  @Override
  public byte[] compress(Column col) {
    switch (col.getType()) {
      case INT_TYPE:
        return Snappy.compress(col.getInts());
      default:
        return new byte[0];
    }
  }

  @Override
  public String getCompressorSet() {
    return "snappy";
  }

  @Override
  public String getVendor() {
    return "snappy";
  }
}
