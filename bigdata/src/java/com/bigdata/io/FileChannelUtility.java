/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
/*
 * Created on May 14, 2008
 */

package com.bigdata.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

/**
 * A helper class for operations on {@link FileChannel}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FileChannelUtility {

    /**
     * Log for btree opeations.
     */
    protected static final Logger log = Logger.getLogger(FileChannelUtility.class);

    /**
     * Reads {@link ByteBuffer#remaining()} bytes into the caller's
     * {@link ByteBuffer} from the channel starting at offset <i>pos</i>. The
     * position of the {@link ByteBuffer} is advanced to the limit. The offset
     * of the channel is NOT modified as a side-effect.
     * <p>
     * This handles partial reads by reading until nbytes bytes have been
     * transferred.
     * 
     * @param channel
     *            The source {@link FileChannel}.
     * @param dst
     *            A {@link ByteBuffer} into which the data will be read.
     * @param pos
     *            The offset from which the data will be read.
     * 
     * @return The #of disk read operations that were required.
     * 
     * @throws IOException
     * 
     * @throws IllegalArgumentException
     *             if any parameter is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>pos</i> is negative.
     * @throws IllegalArgumentException
     *             if <i>dst</i> does not have any bytes remaining.
     */
    static public int readAll(final FileChannel channel, final ByteBuffer dst,
            final long pos) throws IOException {

        if (channel == null)
            throw new IllegalArgumentException();
        if (dst == null)
            throw new IllegalArgumentException();
        if (pos < 0)
            throw new IllegalArgumentException();

        final int nbytes = dst.remaining();
        
        if (nbytes==0)
            throw new IllegalArgumentException();

        /*
         * Read in loop until nbytes == nread
         */

        int nreads = 0; // #of read operations.

        int count = 0; // #of bytes read.

        long p = pos; // offset into the disk file.

        while (count < nbytes) {

            // copy the data into the buffer
            final int nread = channel.read(dst, p);

            p += nread; // increment file offset.

            count += nread; // #bytes read so far.

        }

        if (count != nbytes) {

            throw new RuntimeException("Expected to read " + nbytes
                    + " bytes but read " + count + " in " + nreads + " reads");

        }

        return nreads;
        
    }

    /**
     * Write bytes in <i>data</i> from the position to the limit on the channel
     * starting at <i>pos</i>
     * <p>
     * Note: I have seen count != remaining() for a single invocation of
     * FileChannel#write(). This occured 5 hours into a run with the write cache
     * disabled (so lots of small record writes). All of a sudden, several
     * writes wound up reporting too few bytes written - this persisted until
     * the end of the run (Fedora core 6 with Sun JDK 1.6.0_03). I have since
     * modified this code to use a loop to ensure that all bytes get written.
     * 
     * @return The #of disk write operations that were required.
     */
    static public int writeAll(final FileChannel channel, final ByteBuffer data,
            final long pos) throws IOException {

        final int nbytes = data.remaining();
        
        int count = 0;
        int nwrites = 0;

        while (data.remaining() > 0) {

            count += channel.write(data, pos);

            nwrites++;

            if (nwrites == 100) {

                log.warn("writing on channel: remaining=" + data.remaining()
                        + ", nwrites=" + nwrites + ", written=" + count);

            } else if (nwrites == 1000) {

                log.error("writing on channel: remaining=" + data.remaining()
                        + ", nwrites=" + nwrites + ", written=" + count);

            }
            if (nwrites > 10000) {

                throw new RuntimeException("writing on channel: remaining="
                        + data.remaining() + ", nwrites=" + nwrites
                        + ", written=" + count);

            }

        }

        if (count != nbytes) {

            throw new RuntimeException("Expecting to write " + nbytes
                    + " bytes, but wrote " + count + " bytes in " + nwrites);

        }

        return nwrites;
        
    }

    /**
     * {@link FileChannel} to {@link FileChannel} transfer of <i>count</i>
     * bytes from the <i>sourceChannel</i> starting at the <i>fromPosition</i>
     * onto the <i>out</i> file starting at its current position. The position
     * on the <i>sourceChannel</i> is updated by this method and will be the
     * positioned after the last byte transferred. The position on the <i>out</i>
     * file is updated by this method and will be positioned after the last byte
     * written.
     * 
     * @param sourceChannel
     *            The source {@link FileChannel}.
     * @param fromPosition
     *            The first byte to transfer from the <i>sourceChannel</i>.
     * @param count
     *            The #of bytes to transfer.
     * @param out
     *            The output file.
     * 
     * @return #of IO operations required to effect the transfer.
     * 
     * @throws IOException
     */
     static public int transferAllFrom(final FileChannel sourceChannel,
            final long fromPosition, final long count,
            final RandomAccessFile out) throws IOException {

        final long begin = System.currentTimeMillis();

        // the output channel.
        final FileChannel outChannel = out.getChannel();
        
        // current position on the output channel.
        final long toPosition = outChannel.position();

        // Set the fromPosition on source channel.
        sourceChannel.position(fromPosition);

        /*
         * Extend the output file. This is required at least for some
         * circumstances.
         */
        out.setLength(toPosition + count);

        /*
         * Transfer the data. It is possible that this will take multiple writes
         * for at least some implementations.
         */

        if (log.isInfoEnabled())
            log.info("fromPosition=" + sourceChannel.position()
                    + ", toPosition=" + toPosition + ", count=" + count);

        int nwrites = 0; // #of write operations.

        long n = count;

        {

            long to = toPosition;

            while (n > 0) {

                if (log.isInfoEnabled())
                    log.info("to=" + toPosition + ", remaining=" + n
                            + ", nwrites=" + nwrites);

                long nxfer = outChannel.transferFrom(sourceChannel, to, n);

                to += nxfer;

                n -= nxfer;

                nwrites++;

            }

        }

        // Verify transfer is complete.
        if (n != 0) {

            throw new IOException("Expected to transfer " + count
                    + ", but transferred " + (count - n));

        }

        /*
         * Update the position on the output channel since transferFrom does NOT
         * do this itself.
         */
        outChannel.position(toPosition + count);

        final long elapsed = System.currentTimeMillis() - begin;

        log.warn("Transferred " + count
                + " bytes from disk channel to disk channel (offset="
                + toPosition + ") in " + nwrites + " writes and " + elapsed
                + "ms");

        return nwrites;

    }

}
