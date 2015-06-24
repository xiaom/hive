package org.apache.hive.service.cli.compression;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.ServiceLoader;

public class ColumnCompressorService {

  private static ColumnCompressorService service;
  private ServiceLoader<ColumnCompressor> loader;
  private Hashtable<String, Class<? extends ColumnCompressor>> compressorTable = new Hashtable<>();

  private ColumnCompressorService() {
    loader = ServiceLoader.load(ColumnCompressor.class);
  }

  /**
   * Get the singleton instance of ColumnCompressorService.
   * 
   * @return A singleton instance of ColumnCompressorService.
   */
  public static synchronized ColumnCompressorService getInstance() {
    if (service == null) {
      service = new ColumnCompressorService();
      Iterator<ColumnCompressor> compressors = service.loader.iterator();
      while (compressors.hasNext()) {
        ColumnCompressor compressor = compressors.next();
        service.compressorTable.put(compressor.getVendor() + "." + compressor.getCompressorSet(),
            compressor.getClass());
      }
    }
    return service;
  }

  /**
   * Get a compressor object with the specified vendor and compressorSet name, if the compressor
   * class was loaded from CLASSPATH.
   * 
   * @param vendor
   *          The vendor string of the compressor implementation, provided by the compressor class
   * @param compressorSet
   *          The compressor set string of the compressor implementation, provided by the compressor
   *          class
   * @return A ColumnCompressor implementation object
   */
  public ColumnCompressor getCompressor(String vendor, String compressorSet) {
    Class<? extends ColumnCompressor> compressorClass = compressorTable.get(vendor + "."
        + compressorSet);
    ColumnCompressor compressor = null;
    try {
      compressor = compressorClass.newInstance();
    } catch (Exception e) {
    }
    return compressor;
  }
}
