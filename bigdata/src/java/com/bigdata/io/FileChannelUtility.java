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
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

import com.bigdata.journal.TemporaryRawStore;

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

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

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
     * @param src
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
     * 
     * @todo This might need to use a static direct buffer pool to avoid leaking
     *       temporary direct buffers since Java may attempt to allocate a
     *       "temporary" direct buffer if [dst] is not already a direct buffer.
     * 
     * @deprecated by {@link #readAll(IReopenChannel, ByteBuffer, long)} which
     *             handles transparent re-opening of the store in order to
     *             complete the read operation.
     */
    static public int readAll(final FileChannel channel, final ByteBuffer src,
            final long pos) throws IOException {

        return readAll(new NOPReopener(channel), src, pos);
        
    }
    
    /**
     * Reads {@link ByteBuffer#remaining()} bytes into the caller's
     * {@link ByteBuffer} from the channel starting at offset <i>pos</i>. The
     * position of the {@link ByteBuffer} is advanced to the limit. The offset
     * of the channel is NOT modified as a side-effect.
     * <p>
     * This handles partial reads by reading until nbytes bytes have been
     * transferred.
     * 
     * @param opener
     *            An object which knows how to re-open the {@link FileChannel}
     *            and knows when the resource is no longer available.
     * @param src
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
     * 
     * @todo This might need to use a static direct buffer pool to avoid leaking
     *       temporary direct buffers since Java may attempt to allocate a
     *       "temporary" direct buffer if [dst] is not already a direct buffer.
     */
    static public int readAll(final IReopenChannel<FileChannel> opener,
            final ByteBuffer src, final long pos) throws IOException {

        if (opener == null)
            throw new IllegalArgumentException();
        if (src == null)
            throw new IllegalArgumentException();
        if (pos < 0)
            throw new IllegalArgumentException();

        final int nbytes = src.remaining();

        if (nbytes == 0)
            throw new IllegalArgumentException();

        if (DEBUG) {

            log.debug("pos=" + pos + ", #bytes=" + nbytes);
            
        }
        
        /*
         * Read in loop until nbytes == nread
         */

        int nreads = 0; // #of read operations.

        int count = 0; // #of bytes read.

        while (count < nbytes) {

            final FileChannel channel;
            try {
                
                channel = opener.reopenChannel();
                
                if(channel == null) {
                    
                    throw new AssertionError("Channel is null?");
                    
                }
                
            } catch (IllegalStateException ex) {
                
                // the channel could not be re-opened.
                throw ex;
                
            }

            final int nread;
            try {

                // copy the data into the buffer
                nread = channel.read(src, pos + count);
                
            } catch (ClosedByInterruptException ex) {
                
                /*
                 * This indicates that this thread was interrupted. We
                 * always abort in this case.
                 */
                
                throw ex;

            } catch (AsynchronousCloseException ex) {
                
                /*
                 * The channel was closed asynchronously while blocking during
                 * the read. We will continue to read if the channel can be
                 * reopened.
                 */
                continue;
                
            } catch (ClosedChannelException ex) {
                
                /*
                 * The channel is closed. This could have occurred between the
                 * moment when we got the FileChannel reference and the moment
                 * when we tried to read on the FileChannel. We will continue to
                 * read if the channel can be reopened.
                 */
                continue;
                
            } catch (IOException ex) {

                // anything else.
                throw ex;

            }

            if (nread == -1)
                throw new IOException("EOF reading on channel: remaining="
                        + (nbytes - count) + ", nread=" + nreads + ", pos="
                        + pos + ", bytesRead=" + count);
            
            count += nread; // #bytes read so far.
            
            nreads++;

            if (nreads == 100) {

                log.warn("reading on channel: remaining=" + (nbytes - count)
                        + ", nread=" + nreads + ", pos=" + pos + ", bytesRead="
                        + count);

            } else if (nreads == 1000) {

                log.error("reading on channel: remaining=" + (nbytes - count)
                        + ", nread=" + nreads + ", pos=" + pos + ", bytesRead="
                        + count);

            } else if (nreads > 10000) {

                throw new RuntimeException("reading on channel: remaining="
                        + (nbytes - count) + ", nread=" + nreads + ", pos="
                        + pos + ", bytesRead=" + count);

            }

        }

        if (count != nbytes) {

            throw new RuntimeException("Expected to read " + nbytes
                    + " bytes but read " + count + " bytes in " + nreads
                    + " reads");

        }

        if (INFO) {

            log.info("read " + nbytes + " bytes from offset=" + pos + " in "
                    + nreads + " IOs");
            
        }
        
        return nreads;
        
    }

    /**
     * Write bytes in <i>data</i> from the position to the limit on the channel
     * starting at <i>pos</i>. The position of the buffer will be advanced to
     * the limit. The position of the channel is not changed by this method.
     * <p>
     * Note: I have seen count != remaining() for a single invocation of
     * FileChannel#write(). This occurred 5 hours into a run with the write cache
     * disabled (so lots of small record writes). All of a sudden, several
     * writes wound up reporting too few bytes written - this persisted until
     * the end of the run (Fedora core 6 with Sun JDK 1.6.0_03). I have since
     * modified this code to use a loop to ensure that all bytes get written.
     * 
     * @return The #of disk write operations that were required.
     * 
     * @todo This might need to use a static direct buffer pool to avoid leaking
     *       temporary direct buffers since Java probably will attempt to
     *       allocate a "temporary" direct buffer if [data] is not already a
     *       direct buffer. There is logic in {@link TemporaryRawStore} for
     *       handling the overflow from a heap buffer onto the disk which
     *       already does this.
     * 
     * @deprecated by {@link #writeAll(IReopenChannel, ByteBuffer, long)}
     */
    static public int writeAll(final FileChannel channel, final ByteBuffer data,
            final long pos) throws IOException {

        return writeAll(new NOPReopener(channel),data,pos);
        
//        final int nbytes = data.remaining();
//        
//        int count = 0;
//        int nwrites = 0;
//
//        while (data.remaining() > 0) {
//
//            final int nwritten = channel.write(data, pos + count);
//
//            count += nwritten;
//            
//            nwrites++;
//
//            if (nwrites == 100) {
//
//                log.warn("writing on channel: remaining=" + data.remaining()
//                        + ", nwrites=" + nwrites + ", written=" + count);
//
//            } else if (nwrites == 1000) {
//
//                log.error("writing on channel: remaining=" + data.remaining()
//                        + ", nwrites=" + nwrites + ", written=" + count);
//
//            } else if (nwrites > 10000) {
//
//                throw new RuntimeException("writing on channel: remaining="
//                        + data.remaining() + ", nwrites=" + nwrites
//                        + ", written=" + count);
//
//            }
//
//        }
//
//        if (count != nbytes) {
//
//            throw new RuntimeException("Expecting to write " + nbytes
//                    + " bytes, but wrote " + count + " bytes in " + nwrites);
//
//        }
//
//        return nwrites;

    }

    /**
     * Write bytes in <i>data</i> from the position to the limit on the channel
     * starting at <i>pos</i>. The position of the buffer will be advanced to
     * the limit. The position of the channel is not changed by this method. If
     * the backing channel is asynchronously in another thread closed then it
     * will be re-opened and the write will continue.
     * <p>
     * Note: I have seen count != remaining() for a single invocation of
     * FileChannel#write(). This occurred 5 hours into a run with the write cache
     * disabled (so lots of small record writes). All of a sudden, several
     * writes wound up reporting too few bytes written - this persisted until
     * the end of the run (Fedora core 6 with Sun JDK 1.6.0_03). I have since
     * modified this code to use a loop to ensure that all bytes get written.
     * 
     * @param opener
     * @param data
     * @param pos
     * 
     * @return The #of disk write operations that were required.
     * 
     * @throws IOException
     */
    static public int writeAll(final IReopenChannel<FileChannel> opener,
            final ByteBuffer data, final long pos) throws IOException {

        final int nbytes = data.remaining();
        
        int count = 0;
        int nwrites = 0;

        while (data.remaining() > 0) {

            final FileChannel channel;
            try {
                
                channel = opener.reopenChannel();
                
                if(channel == null) {
                    
                    throw new AssertionError("Channel is null?");
                    
                }
                
            } catch (IllegalStateException ex) {
                
                // the channel could not be re-opened.
                throw ex;
                
            }

            final int nwritten;
            try {
            
                nwritten = channel.write(data, pos + count);
                
            } catch (ClosedByInterruptException ex) {

                /*
                 * This indicates that this thread was interrupted. We always
                 * abort in this case.
                 */
                
                throw ex;

            } catch (AsynchronousCloseException ex) {
                
                /*
                 * The channel was closed asynchronously while blocking during
                 * the write. We will continue to write if the channel can be
                 * reopened.
                 */
                continue;
                
            } catch (ClosedChannelException ex) {
                
                /*
                 * The channel is closed. This could have occurred between the
                 * moment when we got the FileChannel reference and the moment
                 * when we tried to write on the FileChannel. We will continue
                 * to write if the channel can be reopened.
                 */
                continue;
                
            } catch (IOException ex) {

                // anything else.
                throw ex;

            }

            count += nwritten;
            
            nwrites++;

            if (nwrites == 100) {

                log.warn("writing on channel: remaining=" + data.remaining()
                        + ", nwrites=" + nwrites + ", written=" + count);

            } else if (nwrites == 1000) {

                log.error("writing on channel: remaining=" + data.remaining()
                        + ", nwrites=" + nwrites + ", written=" + count);

            } else if (nwrites > 10000) {

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
     * {@link FileChannel} to {@link FileChannel} transfer of <i>count</i> bytes
     * from the <i>sourceChannel</i> starting at the <i>fromPosition</i> onto
     * the <i>out</i> file starting at its current position. The position on the
     * <i>sourceChannel</i> is updated by this method and will be the positioned
     * after the last byte transferred (this is done by
     * {@link FileChannel#transferFrom(java.nio.channels.ReadableByteChannel, long, long)}
     * so we have no choice about that). The position on the <i>out</i> file is
     * updated by this method and will be located after the last byte
     * transferred (this is done since we may have to extend the output file in
     * which case its position will have been changed anyway so this makes the
     * API more uniform).
     * 
     * @param sourceChannel
     *            The source {@link FileChannel}.
     * @param fromPosition
     *            The first byte to transfer from the <i>sourceChannel</i>.
     * @param count
     *            The #of bytes to transfer.
     * @param out
     *            The output file.
     * @param toPosition
     *            The data will be transferred onto the output channel starting
     *            at this position.
     * 
     * @return #of IO operations required to effect the transfer.
     * 
     * @throws IOException
     * 
     * @issue There is a known bug with large transfers under Windows. See <a
     *        href="http://bugs.sun.com/bugdatabase/view_bug.do;jsessionid=332d29312f820116d80442f33fb87?bug_id=6431344"
     *        > FileChannel.transferTo() doesn't work if address space runs out
     *        </a>
     * 
     * @todo as a workaround, detect the Windows platform and do the IO
     *       ourselves when the #of bytes to transfer exceeds 500M.
     * 
     * @todo does not handle asynchronous closes due to interrupts in other
     *       threads during a concurrent NIO operation on the same source/dest
     *       file channel (I have never seen this problem and perhaps the NIO
     *       API excludes it).
     * 
     * @todo I have seen this exception when the JVM was allocated to much of
     *       the total RAM on the machine (in this case 2600m out of 3G
     *       available with 4G installed). The workaround is to reduce the -Xmx
     *       parameter value. This could also be corrected by failing over to a
     *       copy based on normal file IO.
     * 
     *       <pre>
     * Caused by: java.io.IOException: Map failed
     *         at sun.nio.ch.FileChannelImpl.map(FileChannelImpl.java:758)
     *         at sun.nio.ch.FileChannelImpl.transferFromFileChannel(FileChannelImpl.java:537)
     *         at sun.nio.ch.FileChannelImpl.transferFrom(FileChannelImpl.java:600)
     *         at com.bigdata.io.FileChannelUtility.transferAll(FileChannelUtility.java:553)
     *         at com.bigdata.journal.AbstractBufferStrategy.transferFromDiskTo(AbstractBufferStrategy.java:420)
     *         at com.bigdata.journal.DiskOnlyStrategy.transferTo(DiskOnlyStrategy.java:2442)
     *         at com.bigdata.btree.IndexSegmentBuilder.writeIndexSegment(IndexSegmentBuilder.java:1865)
     *         at com.bigdata.btree.IndexSegmentBuilder.call(IndexSegmentBuilder.java:1148)
     *         at com.bigdata.resources.IndexManager.buildIndexSegment(IndexManager.java:1755)
     * </pre>
     */
     static public int transferAll(final FileChannel sourceChannel,
            final long fromPosition, final long count,
            final RandomAccessFile out, final long toPosition) throws IOException {

        final long begin = System.currentTimeMillis();

        // the output channel.
        final FileChannel outChannel = out.getChannel();
        
        // Set the fromPosition on source channel.
        sourceChannel.position(fromPosition);

        /*
         * Extend the output file.
         * 
         * Note: This is required at least for some circumstances.
         * 
         * Note:This can also have a side-effect on the file position.
         */
        out.setLength(toPosition + count);

        /*
         * Transfer the data. It is possible that this will take multiple writes
         * for at least some implementations.
         */

        if (INFO)
            log.info("fromPosition=" + fromPosition + ", count=" + count
                    + ", toPosition=" + toPosition);

        int nwrites = 0; // #of write operations.
        long nwritten = 0;

        long n = count;
        
        {
            
            long to = toPosition;

            while (n > 0) {

                final long nbytes = outChannel.transferFrom(sourceChannel, to,
                        n);

                to += nbytes;

                nwritten += nbytes;

                n -= nbytes;

                nwrites++;

                if (nwrites == 100) {

                    log.warn("writing on channel: remaining=" + n
                            + ", nwrites=" + nwrites + ", written=" + nwritten
                            + " of " + count + " bytes");

                } else if (nwrites == 1000) {

                    log.error("writing on channel: remaining=" + n
                            + ", nwrites=" + nwrites + ", written=" + nwritten
                            + " of " + count + " bytes");

                } else if (nwrites > 10000) {

                    throw new RuntimeException("writing on channel: remaining="
                            + n + ", nwrites=" + nwrites + ", written="
                            + nwritten + " of " + count + " bytes");

                }

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

        if (INFO)
            log.info("Transferred " + count
                    + " bytes from disk channel at offset " + fromPosition
                    + " to disk channel at offset=" + toPosition + " in "
                    + nwrites + " writes and " + elapsed + "ms");

        return nwrites;

    }

}
