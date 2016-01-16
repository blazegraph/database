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

import java.nio.ByteBuffer;
import java.util.Random;

import com.bigdata.io.TestCase3;

public class TestCompressorRegistry extends TestCase3 {
	final Random r = new Random();
	
	/**
	 * Simple test to confirm standard compress/expand utilities on
	 * HAWriteMesage
	 */
	public void testSimpleCompression() {
		byte[] bytes = grabRepeatBytes();
		final ByteBuffer src = ByteBuffer.wrap(bytes);
		
		{
            final IRecordCompressor compressor = CompressorRegistry
                    .getInstance().get(
                            CompressorRegistry.DEFLATE_BEST_COMPRESSION);

			final ByteBuffer dst = compressor.compress(src.duplicate());
			
			if(log.isInfoEnabled())
			    log.info("COMPRESSED Compressed Dst: " + dst.limit() + ", Src:" + src.limit());
	
			final ByteBuffer res = compressor.decompress(dst.duplicate());
			
            if(log.isInfoEnabled())
                log.info("Expanded Dst: " + dst.limit() + ", Src:" + res.limit());
			
			assertTrue(res.compareTo(src) == 0);
		}

		{
            final IRecordCompressor compressor = CompressorRegistry
                    .getInstance().get(
                            CompressorRegistry.DEFLATE_BEST_SPEED);

            final ByteBuffer dst = compressor.compress(src.duplicate());
			
            if(log.isInfoEnabled())
			log.info("SPEED Compressed Dst: " + dst.limit() + ", Src:" + src.limit());
	
			final ByteBuffer res = compressor.decompress(dst.duplicate());
			
            if(log.isInfoEnabled())
			log.info("Expanded Dst: " + dst.limit() + ", Src:" + res.limit());
			
			assertTrue(res.compareTo(src) == 0);
		}

		{
            final IRecordCompressor compressor = CompressorRegistry
                    .getInstance().get(
                            CompressorRegistry.NOP);
			final ByteBuffer dst = compressor.compress(src.duplicate());
			
            if(log.isInfoEnabled())
			log.info("NO COMPRESSION Compressed Dst: " + dst.limit() + ", Src:" + src.limit());
	
			final ByteBuffer res = compressor.decompress(dst.duplicate());
			
            if(log.isInfoEnabled())
			log.info("Expanded Dst: " + dst.limit() + ", Src:" + res.limit());
			
			assertTrue(res.compareTo(src) == 0);
		}

		{
            final IRecordCompressor compressor = CompressorRegistry
                    .getInstance().get(
                            CompressorRegistry.GZIP);
			final ByteBuffer dst = compressor.compress(src);
			
            if(log.isInfoEnabled())
			log.info("Compressed ZIP Dst: " + dst.limit() + ", Src:" + src.limit());
	
			final ByteBuffer res = compressor.decompress(dst);
			
            if(log.isInfoEnabled())
			log.info("Expanded ZIP Dst: " + dst.limit() + ", Src:" + res.limit());
			
			assertTrue(res.compareTo(src) == 0);
		}
		
	}
	
	private byte[] grabRepeatBytes() {

	    final byte[] bytes = new byte[512 * 1024];
		
		final byte[] src = new byte[3];
		r.nextBytes(src);
		
		// copy src pattern repeated into bytes
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = src[i % src.length];
		}

		return bytes;
	}
	
	public void testPerformanceCompression() {
		doPerformanceCompression(CompressorRegistry.NOP);
		doPerformanceCompression(CompressorRegistry.DEFLATE_BEST_SPEED);
		doPerformanceCompression(CompressorRegistry.DEFLATE_BEST_COMPRESSION);
		doPerformanceCompression(CompressorRegistry.GZIP);
	}
	
	public void doPerformanceCompression(final String strategy) {
//		HAWriteMessage.setCompression(strategy);
//		final IRecordCompressor compressor = HAWriteMessage.getCompressor();
	    
        final IRecordCompressor compressor = CompressorRegistry.getInstance()
                .get(strategy);

		final long start = System.currentTimeMillis();
		ByteBuffer res = null;
		final byte[] bytes = grabRepeatBytes();
		final ByteBuffer src = ByteBuffer.wrap(bytes);

		for (int i = 0; i < 2000; i++) {			
			final ByteBuffer dst = compressor.compress(src.duplicate());
			
			res = compressor.decompress(dst);
			
			// assertTrue(res.compareTo(src) == 0);
		}
		
        if(log.isInfoEnabled())
		log.info("Strategy " + strategy + " Compress/Expand Inflator took: "+ (System.currentTimeMillis() - start) + "ms");
	}
}
