package com.bigdata.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

public class MergeStreamWithSortedSet {

    private static final transient Logger log = Logger
            .getLogger(MergeStreamWithSortedSet.class);

    static public void process(final InputStream in, final Set<Entry<Long, byte[]>> snapshotData, final OutputStream out) throws IOException {
		
		final byte[] bb = new byte[4096];
		final Iterator<Entry<Long, byte[]>> bbs = snapshotData.iterator();
		
		long curs = 0;
		
		while (bbs.hasNext()) {
			final Entry<Long, byte[]> e = bbs.next();
			final long pos = e.getKey();
			final byte[] buf = e.getValue();
			final long blen = buf.length;
			
			// transfer Input to Output until start of buffer
			if (curs < pos) {
				transfer(in, (pos-curs), bb, out);
				
				curs = pos;
			}
			
			// transfer buffer
			out.write(buf);
			
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
