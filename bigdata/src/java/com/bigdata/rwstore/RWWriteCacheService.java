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

import com.bigdata.io.IReopenChannel;
import com.bigdata.io.WriteCache;
import com.bigdata.io.WriteCacheService;
import com.bigdata.io.WriteCache.FileChannelScatteredWriteCache;
import com.bigdata.journal.ha.QuorumManager;

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

        public ReopenFileChannel(final File file, final RandomAccessFile raf, final String mode)
                throws IOException {

            this.file = file;

            this.mode = mode;
            
            this.raf = raf;

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

    public RWWriteCacheService(int nbuffers, final File file,
            final RandomAccessFile raf, final String mode,
            final QuorumManager quorumManager) throws InterruptedException,
            IOException {
        super(nbuffers, true/* useChecksum */, new ReopenFileChannel(file, raf,
                mode), quorumManager);
    }

    /**
     * Provide default FileChannelScatteredWriteCache
     */
    @Override
    protected WriteCache newWriteCache(final ByteBuffer buf,
            final boolean useChecksum,
            final IReopenChannel<? extends Channel> opener)
            throws InterruptedException {

        return new FileChannelScatteredWriteCache(buf, true/* useChecksum */,
                (IReopenChannel<FileChannel>) opener);
	    
    }

}
