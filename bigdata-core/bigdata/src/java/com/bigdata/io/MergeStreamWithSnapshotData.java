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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.bigdata.journal.AbstractJournal.ISnapshotData;
import com.bigdata.journal.AbstractJournal.ISnapshotEntry;

/**
 * This utility class provide a method to write an InputStream to an OutputStream, merging with
 * the ISnapshotData.
 * <p>
 * The ISnapshotData provides access to an ordered iteration of [Address,Data] that is processed
 * with the InputStream to determine which data to write to the OutputStream, alternately transferring
 * bytes from the InputStream and the ISnapshotData.
 * 
 * @author Martyn Cutcher
 *
 */
public class MergeStreamWithSnapshotData {

    private static final transient Logger log = Logger
            .getLogger(MergeStreamWithSnapshotData.class);
    

    static public void process(final InputStream in, final ISnapshotData snapshotData, final OutputStream out) throws IOException {
		
		final byte[] bb = new byte[4096];
		final Iterator<ISnapshotEntry> bbs = snapshotData.entries();
		
		long curs = 0;
		
		while (bbs.hasNext()) {
			final ISnapshotEntry e = bbs.next();
			final long pos = e.getAddress();
			final byte[] buf = e.getData();
			final long blen = buf.length;
			
			// transfer Input to Output until start of buffer
			if (curs < pos) {
				transfer(in, (pos-curs), bb, out);
				
				curs = pos;
			}
			
			// transfer buffer
			out.write(buf);
			
			if (log.isInfoEnabled())
				log.info("Write ByteBuffer at " + pos +", length: " + blen);
			
			curs += blen;
			
			// skip input over size of buffer
			in.skip(blen);
		}
		
		// Now transfer remainder of stream
		transfer(in, Long.MAX_VALUE, bb, out);
	}

	private static void transfer(final InputStream in, final long len, final byte[] buf, final OutputStream out) throws IOException {
		long rem = len;
		while (rem > buf.length) {
			int rdlen = in.read(buf);
			if (rdlen == -1)
				return; // eof
			
			out.write(buf, 0, rdlen);
			rem -= rdlen;
		}
		
		// now read in what's left
		int rdlen = in.read(buf, 0, (int) rem);
		if (rdlen == -1)
			return; // eof
		
		out.write(buf, 0, rdlen);
	}
}
