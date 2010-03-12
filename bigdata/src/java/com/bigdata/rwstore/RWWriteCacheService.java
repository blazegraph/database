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

package com.bigdata.rwstore;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.WriteCache;
import com.bigdata.io.WriteCache.FileChannelScatteredWriteCache;
import com.bigdata.io.WriteCacheService;

/**
 * Defines the WriteCacheService to be used by the RWStore.
 * @author mgc
 *
 */
public class RWWriteCacheService extends WriteCacheService {

    protected static final Logger log = Logger.getLogger(RWWriteCacheService.class);
    
	/**
     * Simple implementation for a {@link RandomAccessFile} to handle the direct backing store.
     */
    private static class ReopenFileChannel implements
            IReopenChannel<FileChannel> {

        final private File file;

        private final String mode;

        private volatile RandomAccessFile raf;

        public ReopenFileChannel(final File file, final String mode)
                throws IOException {

            this.file = file;

            this.mode = mode;

            reopenChannel();

        }

        public String toString() {

            return file.toString();

        }

        /**
         * Hook used by the unit tests to destroy their test files.
         */
        public void destroy() {
            try {
                raf.close();
            } catch (IOException e) {
                if (!file.delete())
                    log.warn("Could not delete file: " + file);
            }
        }

        synchronized public FileChannel reopenChannel() throws IOException {

            if (raf != null && raf.getChannel().isOpen()) {

                /*
                 * The channel is still open. If you are allowing concurrent
                 * reads on the channel, then this could indicate that two
                 * readers each found the channel closed and that one was able
                 * to re-open the channel before the other such that the channel
                 * was open again by the time the 2nd reader got here.
                 */

                return raf.getChannel();

            }

            // open the file.
            this.raf = new RandomAccessFile(file, mode);

            if (log.isInfoEnabled())
                log.info("(Re-)opened file: " + file);

            return raf.getChannel();

        }

    };

    public RWWriteCacheService(int nbuffers, final File file, final String mode) throws InterruptedException, IOException {
		super(nbuffers, new ReopenFileChannel(file, mode));
		// TODO Auto-generated constructor stub
	}

    /**
     * Provide default FileChannelScatteredWriteCache
     */
	@Override
	protected WriteCache newWriteCache(ByteBuffer buf, IReopenChannel<? extends Channel> opener)
			throws InterruptedException {
		return new FileChannelScatteredWriteCache(buf, (IReopenChannel<FileChannel>) opener);
	}

	/**
	 * Called to check if a write has already been flushed.  This is only made if a write has been made to
	 * previously committed data (in the current RW session)
	 * 
	 * If dirt writeCaches are flushed in order then it does not matter, however, if we want to be able to combine
	 * writeCaches then it makes sense that there are no duplicate writes.
	 * 
	 * On reflection this is more likely needed since for the RWStore, depending on session parameters, the same
	 * cached area could be overwritten. We could still maintain multiple writes but we need a guarantee of order
	 * when retrieving data from the write cache (newest first).
	 * 
	 * So the question is, whether it is better to keep cache consistent or to constrain with read order.

	 * @param addr the address to check
	 */
	public void clearWrite(long addr) {
		WriteCache cache = recordMap.get(addr);
		if (cache != null) {
			cache.clearAddrMap(addr);
			recordMap.remove(addr);
		}
	}

}
