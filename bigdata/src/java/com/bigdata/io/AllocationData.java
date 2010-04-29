/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
import com.bigdata.util.ChecksumUtility;

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
	
	static class SimpleSlice implements IByteArraySlice {
		byte[] m_array;
		
		SimpleSlice(ByteBuffer bb) {
			m_array = bb.array();
		}

		SimpleSlice(byte[] bb) {
			m_array = bb;
		}

		@Override
		public byte[] array() {
			return m_array;
		}

		@Override
		public int len() {
			return m_array.length;
		}

		@Override
		public int off() {
			return 0;
		}
	}
}
