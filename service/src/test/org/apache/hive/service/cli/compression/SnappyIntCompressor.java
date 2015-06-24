package org.apache.hive.service.cli.compression;

import java.io.IOException;

import org.apache.hive.service.cli.Column;
import org.apache.hive.service.cli.compression.ColumnCompressor;
import org.xerial.snappy.Snappy;

public class SnappyIntCompressor implements ColumnCompressor {

  /**
   * Before calling the compress() method below, should check if the column is compressable by any
   * given plugin.
   * 
   * @Param col Column to be checked
   * @return boolean which says yes/no
   */
  public boolean isCompressible(Column col) {
    System.out.println("Yo! I can compress integers!!");
    switch (col.getType()) {
      case INT_TYPE:
        return true;
      default:
        return false;
    }
  }

  /*
   * Method provided for testing purposes. Decompresses a compressed byte array. should be
   * compressed using the compress() function below which uses the snappy JAR files. If Snappy JAR
   * file isn't used, this method will return the wrong result.
   */
  public static int[] decompress(byte[] inp) throws IOException {
    return Snappy.uncompressIntArray(inp);
  }

  /**
   * Compresses a given column.
   * 
   * @Param col A column to be compressed
   * @return byte array with compressed data
   */
  public byte[] compress(Column col) {
    System.out.println("ECBS called me! I am the compress method!!");
    switch (col.getType()) {
      case INT_TYPE:
        return Snappy.compress(col.getInts());
      default:
        return new byte[0];
    }
  }

  @Override
  public String getCompressorSet() {
    return "pluginsnappy";
  }

  @Override
  public String getVendor() {
    return "org.apache.hive.service.cli";
  }
}
