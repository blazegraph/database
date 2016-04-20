/*

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
/*
 * Created on May 14, 2008
 */

package com.bigdata.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.NonReadableChannelException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.util.InnerCause;

/**
 * A helper class for operations on {@link FileChannel}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class FileChannelUtility {

    private static final Logger log = Logger.getLogger(FileChannelUtility.class);

    private static final boolean INFO = log.isInfoEnabled();

    private static final boolean DEBUG = log.isDebugEnabled();

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
     * Define interface to support callback from readAllAsync
     */
    public interface IAsyncOpener {
        AsynchronousFileChannel getAsyncChannel();
    }
    
    /**
     * The AsyncTransfer class encapsulates the state required to make
     * asynchronous transfer requests.  It was written explicitly to support
     * asynchronous transfer of blob data that requires multiple reads.
     */
    static public class AsyncTransfer {
    	private final long m_addr;
    	private final int m_bytesToRead;
    	private final ByteBuffer m_buffer;
    	private Future<Integer> m_fut = null;
    	
    	public AsyncTransfer(final long addr, final ByteBuffer buffer) {
    		m_addr = addr;
    		m_buffer = buffer;
    		m_bytesToRead = buffer.remaining();
    		m_buffer.mark(); // mark buffer to support reset on any retries
    	}

    	/**
         * Schedule a read on the channel. If the operation was previously
         * schedule and is done (normal completion), then return immediately. If
         * the operation was previously schedule and was cancelled, then throws
         * out a CancellationException. If the operation was previously schedule
         * and failed, then the future is cleared and the operation is
         * rescheduled. This is done in order to allow us to complete a high
         * level read on the channel when the backing channel may have been
         * closed by an interrupt in another thread due to Java IO channel
         * semantics (e.g., driven by query termination during reads).
         * 
         * @param channel The channel.
         * 
         * @throws IllegalArgumentException
         * @throws NonReadableChannelException
         * @throws CancellationException
         * @throws InterruptedException
         */
        private void read(final AsynchronousFileChannel channel)
                throws IllegalArgumentException, NonReadableChannelException, CancellationException, InterruptedException {
            if (isDone()) { // Check for re-scheduling of the read().
    		    try {
                    /*
                     * Note: It is either unlikely or impossible to have an
                     * InterruptedException thrown out here since we know that
                     * the Future isDone().
                     */
    		        m_fut.get(); // throws CancellationException, ExecutionException, InterruptedException. 
                } catch (ExecutionException ex) {
                    /*
                     * This read() had failed. We clear future so we can re-do
                     * the read.
                     */
    		        m_fut = null;
    		    }
    		}
    		if(!isDone()) {
    			// ensure buffer is ready
    			m_buffer.reset();
    			m_fut = channel.read(m_buffer,  m_addr); // throws IllegalArgumentException, NonReadableChannelException
    		}
    	}

        public int complete() throws InterruptedException, ExecutionException, IllegalStateException {
            if (m_fut == null) {
                // Future is not set.
                throw new IllegalStateException("Future is not set");
            }
            return m_fut.get();
		}

		public void cancel() {
            if (m_fut != null)
                m_fut.cancel(true/* mayInterruptIfRunning */);
		}

		public boolean isDone() {
			return m_fut != null && m_fut.isDone();
		}
    }

    /**
     * readAllAsync will make repeated attempts to read data into the transfer buffers defined
     * in the List of AsyncTransfer instances.
     * 
     * @param opener provides AsynchronouseFileChannel
     * @param transfers defines the list of required transfers
     * @return the total number of bytes read asynchronously
     * @throws IOException 
     */
    static public long readAllAsync(final IAsyncOpener opener, final List<AsyncTransfer> transfers) throws IOException {
        int ntries = 0;
        long totalBytesRead = 0;
        final long totalBytesExpected;
        {
            long n = 0;
            for (AsyncTransfer transfer : transfers) {
                n += transfer.m_bytesToRead;
            }
            totalBytesExpected = n;
        }
        try {
        	while (true) {
                ntries++;
                if (ntries == 100) {

                    log.warn("reading on channel: remaining=" + (totalBytesExpected - totalBytesRead) + ", ntransfers="
                            + transfers.size() + ", ntries=" + ntries + ", bytesRead=" + totalBytesRead);

                } else if (ntries == 1000) {

                    log.error("reading on channel: remaining=" + (totalBytesExpected - totalBytesRead) + ", ntransfers="
                            + transfers.size() + ", ntries=" + ntries + ", bytesRead=" + totalBytesRead);

                } else if (ntries > 10000) {

                    throw new RuntimeException(
                            "reading on channel: remaining=" + (totalBytesExpected - totalBytesRead) + ", ntransfers="
                                    + transfers.size() + ", ntries=" + ntries + ", bytesRead=" + totalBytesRead);

                }
                // Re-open the channel if the backing channel was closed.
    	        final AsynchronousFileChannel channel = opener.getAsyncChannel();
    			try {
    			    // Schedule transfer requests (does not re-schedule if already done)
    				for (AsyncTransfer transfer : transfers) {
    					// The AsyncTransfer object handles retries by checking any existing transfer state
    					transfer.read(channel); // throws IllegalArgumentException,
    					                        //        NonReadableChannelException (extends IllegalStateException),
    					                        //        CancellationException (extends IllegalStateException), 
    					                        //        InterruptedException
    				}
    				
                    // Await completion of transfers, totaling up bytes read.
                    totalBytesRead = 0;
                    for (AsyncTransfer transfer : transfers) {
                        totalBytesRead += transfer.complete(); // throws InterruptedException, ExecutionException.
                    }

                    return totalBytesRead;

                } catch (InterruptedException ex) {

                    // Wrap and throw.
                    throw new RuntimeException(ex);

                } catch (ExecutionException ex) {

                    final Throwable cause = ex.getCause();

                    if (InnerCause.isInnerCause(cause, ClosedByInterruptException.class)) {

                        /*
                         * This indicates that this thread was interrupted. We
                         * always abort in this case.
                         */

                        throw new IOException(ex);

                    } else if (InnerCause.isInnerCause(cause, AsynchronousCloseException.class)) {

                        /*
                         * The channel was closed asynchronously while blocking
                         * during the read. We will continue to read if the
                         * channel can be reopened.
                         */
                        continue;

                    } else if (InnerCause.isInnerCause(cause, ClosedChannelException.class)) {

                        /*
                         * The channel is closed. This could have occurred
                         * between the moment when we got the FileChannel
                         * reference and the moment when we tried to read on the
                         * FileChannel. We will continue to read if the channel
                         * can be reopened.
                         */
                        continue;

                    } else {

                        /*
                         * Wrap and thrown anything else.
                         */
                        throw new RuntimeException(ex);

                    }
                }
            } // while(true)
        } finally {
            /*
             * Ensure that transfer requests are cancelled.
             */
            for (AsyncTransfer transfer : transfers) {
                transfer.cancel();
            }
        }
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

        final long begin = System.nanoTime();

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

        final long elapsed = System.nanoTime() - begin;

        if (INFO) {

            log.info("wrote on disk: address: " + pos + ", bytes=" + nbytes + ", elapsed="
                    + TimeUnit.NANOSECONDS.toMillis(elapsed) + "ms");

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
     
     /**
      * An InputStream implementation that utilizes the resilient readAll method of this class
      */
     public static class ReopenerInputStream extends InputStream {
    	 
    	 final private IReopenChannel<FileChannel> m_opener;
    	 final private ByteBuffer m_buffer;
         final private IBufferAccess m_bufferAccess;
         final private long m_eof;
    	 private long m_cursor = 0;
    	 
    	 final private byte[] m_singleByte = new byte[1];
    	 
    	 public ReopenerInputStream(final IReopenChannel<FileChannel> opener) throws IOException {
    		 m_opener = opener;
    		 m_eof = opener.reopenChannel().size();
    		 try {
				m_bufferAccess = DirectBufferPool.INSTANCE.acquire();
	    		m_buffer = m_bufferAccess.buffer();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
    	 }
    	
  		@Override
  		public void close() {
  			try {
				m_bufferAccess.release();
			} catch (InterruptedException e) {
				log.warn(e);
			}
  		}
  		
 		@Override
 		public int read() throws IOException {
 			final int ret = read(m_singleByte, 0, 1);
 			if (ret == -1) {
 				return -1;
 			} else {
 				return m_singleByte[0];
 			}
 		}

		@Override
		public int read(final byte[] dst) throws IOException {
			return read(dst, 0, dst.length);
		}

		@Override
		public int read(final byte[] dst, final int off, final int len) throws IOException {
			final int rdlen = (int) ((m_cursor + len) < m_eof ? len : m_eof-m_cursor);
			
			if (rdlen == 0)
				return -1; // eof!
			
			m_buffer.position(0);
			m_buffer.limit(rdlen);
			
			final int ret = FileChannelUtility.readAll(m_opener, m_buffer, m_cursor);
			
			if (ret > 0) {
				m_cursor += rdlen;
				
				if (log.isTraceEnabled())
					log.trace("Request for " + len + " bytes");

				m_buffer.position(0);
				m_buffer.limit(rdlen);
				m_buffer.get(dst, off, rdlen);
				
				return rdlen;
			}
			
			return -1; // EOF
			
		}
     }

}
