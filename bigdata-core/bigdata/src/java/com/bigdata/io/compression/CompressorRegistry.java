/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package com.bigdata.io.compression;

import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.Deflater;

/**
 * Registration pattern for {@link IRecordCompressor} implementations.
 * 
 * @author Martyn Cutcher
 */
public class CompressorRegistry {
	
    /**
     * Key for {@link Deflater} compression with BEST SPEED.
     * 
     * @see RecordCompressor
     */
    final public static String DEFLATE_BEST_SPEED = "DBS"; 

    /**
     * Key for {@link Deflater} compression with BEST COMPRESSION.
     * 
     * @see RecordCompressor
     */
    final public static String DEFLATE_BEST_COMPRESSION = "DBC";
    
    /**
     * Key for GZIP compression.
     * 
     * @see GZipCompressor
     */
    final public static String GZIP = "GZIP";
    
    /**
     * Key for no compression.
     * <p>
     * Note: <code>null</code> is more efficient than the
     * {@link NOPRecordCompressor} since it avoids all copy for all
     * {@link IRecordCompressor} methods.
     * 
     * @see NOPRecordCompressor
     */
    final public static String NOP = "NOP";

    private static CompressorRegistry DEFAULT = new CompressorRegistry();

    static public CompressorRegistry getInstance() {

        return DEFAULT;

    }

    final private ConcurrentHashMap<String, IRecordCompressor> compressors = new ConcurrentHashMap<String, IRecordCompressor>();
	
	private CompressorRegistry() {
		add(DEFLATE_BEST_SPEED, new RecordCompressor(Deflater.BEST_SPEED));
		add(DEFLATE_BEST_COMPRESSION, new RecordCompressor(Deflater.BEST_COMPRESSION));
		add(GZIP, new GZipCompressor());
		add(NOP, new NOPRecordCompressor());
	}
	
    /**
     * Global hook to allow customized compression strategies
     * 
     * @param key
     * @param compressor
     */
    public void add(final String key, final IRecordCompressor compressor) {

        if (compressors.putIfAbsent(key, compressor) != null) {

            throw new UnsupportedOperationException("Already declared: " + key);

        }
        
    }

    /**
     * Return the {@link IRecordCompressor} registered under that key (if any).
     * 
     * @param key
     *            The key (optional - may be <code>null</code>).
     * @return The {@link IRecordCompressor} -or- <code>null</code> if the key
     *         is <code>null</code> or if there is nothing registered under that
     *         key.
     */
    public IRecordCompressor get(final String key) {

        if (key == null)
            return null;

        return compressors.get(key);

    }
    
}
