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

package com.bigdata.io;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;

import com.bigdata.io.compression.IRecordCompressor;

/**
 * Utility class to handle Compression and Checksums on a ByteArraySlice
 * 
 * The idea is that it would be used as a "state-aware" buffer that could be
 * saved/restored to/from an IDiskStrategy.
 * 
 * @author Martyn Cutcher
 *
 */
public class AllocationData {
	IByteArraySlice slice;
	boolean compressed;
	boolean checksummed;
	int chk;
	
	public AllocationData(IByteArraySlice slice) {
		this.slice = slice;
	}
	
	public AllocationData compress(IRecordCompressor compressor) {
		if (compressed) {
			return this;
		} else {
			ByteArrayOutputStream outstr = new ByteArrayOutputStream();
			compressor.compress(slice.array(), outstr);
			
			return new AllocationData(new SimpleSlice(outstr.toByteArray()));
			
		}
	}
	
	public AllocationData decompress(IRecordCompressor compressor) {
		if (!compressed) {
			return new AllocationData(new SimpleSlice(compressor.decompress(slice.array())));
		} else {
			return null;
		}
	}
	
	public int checksum() {
		if (!checksummed) {
			chk = ChecksumUtility.getCHK().checksum(slice);
		}
		
		return chk;
	}
	
	public void write(Channel channel) {
	}
	
	static public AllocationData read(Channel channel) {
		return null;
	}
	
	private static class SimpleSlice implements IByteArraySlice {
		
	    final private byte[] buf;
		
		SimpleSlice(ByteBuffer bb) {
			buf = bb.array();
		}

		SimpleSlice(byte[] bb) {
			buf = bb;
		}

//		@Override
		public byte[] array() {
			return buf;
		}

//		@Override
		public int len() {
			return buf.length;
		}

//		@Override
		public int off() {
			return 0;
		}
		
		public byte[] toByteArray() {

		    return buf.clone();

		}
		
	}
}
