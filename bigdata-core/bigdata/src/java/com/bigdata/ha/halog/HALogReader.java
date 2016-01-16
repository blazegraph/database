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
package com.bigdata.ha.halog;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.DigestException;
import java.security.MessageDigest;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.bigdata.ha.msg.IHAMessage;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.ChecksumUtility;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.IReopenChannel;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockUtility;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.util.ChecksumError;
import com.bigdata.util.InnerCause;

/**
 * Given an HALog file can be used to replay the file and can provide a readable
 * dump of the content. When replaying, the current position is compared to the
 * EOF to determine whether more data can be read. The called should call
 * {@link IHALogReader#hasMoreBuffers()} and if so read the next associated
 * buffer and process with the returned {@link IHAMessage}. If
 * {@link IHALogReader#hasMoreBuffers()} is false, then the committing
 * {@link IRootBlockView} should be used to commit the replayed transaction.
 * 
 * @author Martyn Cutcher
 */
public class HALogReader implements IHALogReader {

	private static final Logger log = Logger.getLogger(HALogReader.class);

	private final File m_file;
	private final RandomAccessFile m_raf;
	private final FileChannel m_channel;
	private final IRootBlockView m_openRootBlock;
	private final IRootBlockView m_closeRootBlock;
	private final StoreTypeEnum m_storeType;
	private final int magic;
	private final int version;

    /**
     * <strong>CAUTION: This constructor should not be used in circumstances in
     * which the {@link HALogWriter} is active since this constructor can not
     * differentiate atomically between the live HALog and a historical HALog
     * and will always provide a read-only view, even if the live HALog file is
     * opened.</strong>
     * 
     * @param file
     *            The HALog file.
     *            
     * @throws IOException
     */
	public HALogReader(final File file) throws IOException {

		m_file = file;

		m_raf = new RandomAccessFile(file, "r");

		m_channel = m_raf.getChannel();

		try {
			/**
			 * Must determine whether the file has consistent open and committed
			 * rootBlocks, using the commitCounter to determine which rootBlock
			 * is which.
			 * 
			 * Note: Both root block should exist (they are both written on
			 * startup). If they are identical, then the log is empty (the
			 * closing root block has not been written and the data in the log
			 * is useless).
			 * 
			 * We figure out which root block is the opening root block based on
			 * standard logic.
			 */
			/*
			 * Read the MAGIC and VERSION.
			 */
			m_raf.seek(0L);
			try {
				/*
				 * Note: this next line will throw IOException if there is a
				 * file lock contention.
				 */
				magic = m_raf.readInt();
			} catch (IOException ex) {
                throw new RuntimeException(
                        "Can not read magic. Is file locked by another process? file="
                                + file, ex);
			}
            if (magic != HALogWriter.MAGIC)
                throw new RuntimeException("Bad HALog magic: file=" + file
                        + ", expected=" + HALogWriter.MAGIC + ", actual="
                        + magic);
			version = m_raf.readInt();
			if (version != HALogWriter.VERSION1)
                throw new RuntimeException("Bad HALog version: file=" + file
                        + ", expected=" + HALogWriter.VERSION1 + ", actual="
                        + version);

			final RootBlockUtility tmp = new RootBlockUtility(reopener, file,
					true/* validateChecksum */, false/* alternateRootBlock */,
					false/* ignoreBadRootBlock */);

			m_closeRootBlock = tmp.chooseRootBlock();

			m_openRootBlock = tmp.rootBlock0 == m_closeRootBlock ? tmp.rootBlock1
					: tmp.rootBlock0;

			final long cc0 = m_openRootBlock.getCommitCounter();

			final long cc1 = m_closeRootBlock.getCommitCounter();

			if ((cc0 + 1) != cc1 && (cc0 != cc1)) {
				/*
				 * Counters are inconsistent with either an empty log file or a
				 * single transaction scope.
				 */
                throw new IllegalStateException(
                        "Incompatible rootblocks: file=" + file + ", cc0="
                                + cc0 + ", cc1=" + cc1);
			}

			m_channel.position(HALogWriter.headerSize0);

			m_storeType = m_openRootBlock.getStoreType();

		} catch (Throwable t) {

			close();

			throw new RuntimeException(t);

		}

	}

//    /**
//     * {@inheritDoc}
//     * 
//     * TODO This was added to address a file handle leak. However, I am quite
//     * dubious that this will fix the problem. While GC may be necessary to
//     * finalize {@link HALogReader} instances during a RESYNC, we have already
//     * invoked {@link #close()} on those instances in the SendHALogTask(). It
//     * may be better to remove this since finalize() methods add overhead to 
//     * GC.
//     * 
//     * @see <a
//     *      href="https://sourceforge.net/apps/trac/bigdata/ticket/678#comment:4"
//     *      > DGC Thread Leak: sendHALogForWriteSet() </a>
//     */
//	@Override
//	protected void finalize() throws Throwable {
//	    close();
//	    super.finalize();
//	}
	
	/**
	 * Hook for
	 * {@link FileChannelUtility#readAll(FileChannel, ByteBuffer, long)}
	 */
	private final IReopenChannel<FileChannel> reopener = new IReopenChannel<FileChannel>() {

		@Override
		public FileChannel reopenChannel() throws IOException {

			if (m_channel == null)
				throw new IOException("Closed");

			return m_channel;

		}
	};

    @Override
	public void close() {

		if (isOpen()) {

			try {
                
			    m_raf.close();

			} catch (IOException e) {
			    
                log.error("Problem closing file: file=" + m_file + " : " + e, e);
                
            }

		}

	}

    @Override
    public boolean isOpen() {

        return m_channel.isOpen();
        
    }
    
	@Override
	public boolean isLive() {

	    return false;
	    
	}
	
    @Override
	public boolean isEmpty() {

		return m_openRootBlock.getCommitCounter() == m_closeRootBlock
				.getCommitCounter();

	}

	private void assertOpen() throws IOException {

		if (!m_channel.isOpen())
			throw new IOException("Closed: " + m_file);

	}

	/**
	 * The {@link IRootBlockView} for the committed state BEFORE the write set
	 * contained in the HA log file.
	 */
	public IRootBlockView getOpeningRootBlock() {

		return m_openRootBlock;

	}

	public IRootBlockView getClosingRootBlock() {

		return m_closeRootBlock;

	}

    @Override
	public boolean hasMoreBuffers() throws IOException {

        if (!isOpen())
            return false;

		if (isEmpty()) {

			/*
			 * Ignore the file length if it is logically empty.
			 */

			return false;

		}

		return m_channel.position() < m_channel.size();

	}

	/**
	 * To stream from the Channel, we can use the associated RandomAccessFile
	 * since the FilePointer for one is the same as the other.
	 */
	private static class RAFInputStream extends InputStream {
		final RandomAccessFile m_raf;
		
		RAFInputStream(final RandomAccessFile raf) {
			m_raf = raf;
		}
		
		@Override
		public int read() throws IOException {
			return m_raf.read();
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			return m_raf.read(b, off, len);
		}

	}

    @Override
	public IHAWriteMessage processNextBuffer(final ByteBuffer clientBuffer)
			throws IOException {

		return processNextBuffer(m_raf, reopener, m_storeType, clientBuffer);

	}

	static public IHAWriteMessage processNextBuffer(final RandomAccessFile raf, final IReopenChannel<FileChannel> reopener,
			final StoreTypeEnum storeType, final ByteBuffer clientBuffer)
			throws IOException {

		final FileChannel channel = raf.getChannel();
		
		final ObjectInputStream objinstr = new ObjectInputStream(
				new RAFInputStream(raf));

		final IHAWriteMessage msg;
		try {

			msg = (IHAWriteMessage) objinstr.readObject();

		} catch (ClassNotFoundException e) {

			throw new IllegalStateException(e);

		}

		switch (storeType) {
		case WORM:
		case RW: {

			if (msg.getSize() > clientBuffer.capacity()) {

				throw new IllegalStateException(
						"Client buffer is not large enough for logged buffer");

			}

			// Now setup client buffer to receive from the channel
			final int nbytes = msg.getSize();
			clientBuffer.position(0);
			clientBuffer.limit(nbytes);

			// Current position on channel.
			final long pos = channel.position();

			// allow null clientBuffer for IHAWriteMessage only
			if (clientBuffer != null) {
				// Robustly read of write cache block at that position into the
				// caller's buffer. (pos=limit=nbytes)
				FileChannelUtility.readAll(reopener, clientBuffer, pos);

				// limit=pos; pos=0;
				clientBuffer.flip(); // ready for reading

				final int chksum = new ChecksumUtility().checksum(clientBuffer
						.duplicate());

				if (chksum != msg.getChk())
					throw new ChecksumError("Expected=" + msg.getChk()
							+ ", actual=" + chksum);

				if (clientBuffer.remaining() != nbytes)
					throw new AssertionError();
			}
			// Advance the file channel beyond the block we just read.
			channel.position(pos + msg.getSize());

			break;
		}

		default:
			throw new UnsupportedOperationException();
		}

		return msg;

	}

	/**
	 * Utility program will dump log files (or directories containing log files)
	 * provided as arguments.
	 * 
	 * @param args
	 *            Zero or more files or directories.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
    public static void main(final String[] args) throws InterruptedException {

        /*
         * Sort into lexical order to force visitation in lexical order.
         * 
         * Note: This should work under any OS. Files will be either directory
         * names (3 digits) or filenames (21 digits plus the file extension).
         * Thus the comparison centers numerically on the digits that encode
         * either part of a commit counter (subdirectory) or an entire commit
         * counter (HALog file).
         */
        Arrays.sort(args);
        
		final IBufferAccess buf = DirectBufferPool.INSTANCE.acquire();

		try {

			for (String arg : args) {

				final File file = new File(arg);

				if (!file.exists()) {

					System.err.println("No such file: " + file);

					continue;

				}

				if (file.isDirectory()) {

					doDirectory(file, buf);

				} else {

					doFile(file, buf);

				}

			}

		} finally {

			buf.release();

		}

	}

    private static void doDirectory(final File dir, final IBufferAccess buf) {

		final File[] files = dir.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {

				if (new File(dir, name).isDirectory()) {

					// Allow recursion through directories.
					return true;

				}

				return name.endsWith(IHALogReader.HA_LOG_EXT);

			}
		});

        /*
         * Sort into lexical order to force visitation in lexical order.
         * 
         * Note: This should work under any OS. Files will be either directory
         * names (3 digits) or filenames (21 digits plus the file extension).
         * Thus the comparison centers numerically on the digits that encode
         * either part of a commit counter (subdirectory) or an entire commit
         * counter (HALog file).
         */
		Arrays.sort(files);
		
		for (File file : files) {

			if (file.isDirectory()) {

				doDirectory(file, buf);

			} else {

				doFile(file, buf);

			}

		}

	}

	private static void doFile(final File file, final IBufferAccess buf) {

	    try {
            
	        doFile2(file,buf);
            
        } catch (Throwable e) {
            
            if(InnerCause.isInnerCause(e, InterruptedException.class)) {
            
                // Propagate interrupt.
                Thread.currentThread().interrupt();
                
            }

            final String msg = "ERROR: Could not read file: file=" + file
                    + ", cause=" + e;
            
            System.err.println(msg);
            
            log.error(msg, e);
            
        }
	    
    }

    private static void doFile2(final File file, final IBufferAccess buf)
            throws IOException {
        
        final HALogReader r = new HALogReader(file);

        try {

            final IRootBlockView openingRootBlock = r.getOpeningRootBlock();

            final IRootBlockView closingRootBlock = r.getClosingRootBlock();

            final boolean isWORM = openingRootBlock.getStoreType() == StoreTypeEnum.WORM;

            System.out.println("----------begin----------");
            System.out.println("file=" + file);
            System.out.println("openingRootBlock=" + openingRootBlock);
            System.out.println("closingRootBlock=" + closingRootBlock);

            if (openingRootBlock.getCommitCounter() == closingRootBlock
                    .getCommitCounter()) {

                System.err
                        .println("WARN : LOGICALLY EMPTY LOG (closing root block == opening root block): file="
                                + file);

            }
            
            while (r.hasMoreBuffers()) {

                // don't pass buffer in if WORM, just validate the messages
                final IHAWriteMessage msg = r.processNextBuffer(isWORM ? null
                        : buf.buffer());

                System.out.println(msg.toString());

            }
            System.out.println("-----------end-----------");

        } finally {

            r.close();

        }

	}

    @Override
    public void computeDigest(final MessageDigest digest)
            throws DigestException, IOException {

        computeDigest(reopener, digest);
        
    }
    
    static void computeDigest(final IReopenChannel<FileChannel> reopener,
            final MessageDigest digest) throws DigestException, IOException {

        IBufferAccess buf = null;
        try {

            try {
                // Acquire a buffer.
                buf = DirectBufferPool.INSTANCE.acquire();
            } catch (InterruptedException ex) {
                // Wrap and re-throw.
                throw new IOException(ex);
            }

            // The backing ByteBuffer.
            final ByteBuffer b = buf.buffer();

//            // A byte[] with the same capacity as that ByteBuffer.
//            final byte[] a = new byte[b.capacity()];

            // The capacity of that buffer (typically 1MB).
            final int bufferCapacity = b.capacity();

            // The size of the file at the moment we begin.
            final long fileExtent = reopener.reopenChannel().size();

            // The #of bytes whose digest will be computed.
            final long totalBytes = fileExtent;

            // The #of bytes remaining.
            long remaining = totalBytes;

            // The offset.
            long offset = 0L;

            // The block sequence.
            long sequence = 0L;

            if (log.isInfoEnabled())
                log.info("Computing digest: nbytes=" + totalBytes);

            while (remaining > 0) {

                final int nbytes = (int) Math.min((long) bufferCapacity,
                        remaining);

                if (log.isDebugEnabled())
                    log.debug("Computing digest: sequence=" + sequence
                            + ", offset=" + offset + ", nbytes=" + nbytes);

                // Setup for read.
                b.position(0);
                b.limit(nbytes);

                // read block
                FileChannelUtility.readAll(reopener, b, offset);

//                // Copy data into our byte[].
//                final byte[] c = BytesUtil.toArray(b, false/* forceCopy */, a);

                // update digest
//                digest.update(c, 0/* off */, nbytes/* len */);
                b.flip();
                digest.update(b);
                
                offset += nbytes;
                
                remaining -= nbytes;
                
                sequence++;
                
            }

            if (log.isInfoEnabled())
                log.info("Computed digest: #blocks=" + sequence + ", #bytes="
                        + totalBytes);

            // Done.
            return;

        } finally {

            if (buf != null) {
                try {
                    // Release the direct buffer.
                    buf.release();
                } catch (InterruptedException e) {
                    log.warn(e);
                }
            }

        }

    }

}
