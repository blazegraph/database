package com.bigdata.ha;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;

/**
 * Given an HALog file can be used to replay the file and can provide a readable dump of the content.
 * 
 * When replaying, the current position is compared to the EOF to determine whether more data can be read.
 * 
 * The called should call hasMoreBuffers() and if so read the next associated buffer and process with the
 * returned IHAMessage.
 * 
 * If hasMoreBuffers() is false, then the committing rootBlock should be used to commit the replayed 
 * transaction.
 * 
 * @author Martyn Cutcher
 *
 */
public class HALogReader {
	
	final RandomAccessFile m_raf;
	final FileChannel m_channel;
	final IRootBlockView m_openRootBlock;
	final IRootBlockView m_commitRootBlock;
	
	public HALogReader(final File halog) throws IOException {
		m_raf = new RandomAccessFile(halog, "r");
		m_channel = m_raf.getChannel();
		
		/**
		 * Must determine whether the file has consistent open and committed
		 * rootBlocks, using the commitCounter to determine which rootBlock
		 * is which.
		 */

		final IRootBlockView rb0 = readRootBlock(ProcessLogWriter.ROOTBLOCK_0, true);
		final long cc0 = rb0.getCommitCounter();
		final IRootBlockView rb1 = readRootBlock(ProcessLogWriter.ROOTBLOCK_1, false);
		final long cc1 = rb0.getCommitCounter();
		
		if (cc0 == cc1) { // same rootblock - no committed block written
			throw new IllegalArgumentException("Uncommitted log file");
		} else if (cc0 == cc1+1) { // open - 1, commit - 0
			m_openRootBlock = rb1;
			m_commitRootBlock = rb0;
		} else if (cc1 == cc1+1) { //open - 0, commit - 1
			m_openRootBlock = rb0;
			m_commitRootBlock = rb1;
		} else { // counters inconsistent with single transaction scope
			throw new IllegalStateException("Incompatible rootblocks for single transaction");
		}
		m_channel.position(ProcessLogWriter.START_DATA);
	}
	
	private IRootBlockView readRootBlock(final int rootblockPos, final boolean rootBlock0) throws IOException {
		final ByteBuffer dst = ByteBuffer.allocate(RootBlockView.SIZEOF_ROOT_BLOCK);
		
		m_channel.read(dst);
		
		return new RootBlockView(rootBlock0, dst, null);
	}
	
	public IRootBlockView getOpeningRootblock() {
		return m_openRootBlock;
	}
	
	public IRootBlockView getCommittingRootblock() {
		return m_commitRootBlock;
	}

	/**
	 * Checks whether we have reached the end of the file
	 */
	public boolean hasMoreBuffers() throws IOException {
		return m_channel.position() < m_channel.size();
	}
	
	/**
	 * To stream from the Channel, we can use the associated RandomAccessFile
	 * since the FilePointer for one is the same as the other.
	 */
	class RAFInputStream extends InputStream {

		@Override
		public int read() throws IOException {
			return m_raf.read();
		}
		
		@Override
		public int read(byte[] b, int off, int len)  throws IOException {
			return m_raf.read(b, off, len);
		}
		
	}
	
	/**
	 * Attempts to read the next IHAWriteMessage and then the
	 * expected buffer, that is read into the client buffer.  
	 * The IHAWriteMessage is returned to the caller.
	 */
	public IHAWriteMessage processNextBuffer(final ByteBuffer clientBuffer) throws IOException {
		final ObjectInputStream objinstr = new ObjectInputStream(new RAFInputStream());
		final IHAWriteMessage msg;
		try {
			msg = (IHAWriteMessage) objinstr.readObject();
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(e);
		}
		
		if (msg.getSize() > clientBuffer.capacity()) {
			throw new IllegalStateException("Client buffer is not large enough for logged buffer");
		}
		
		// Now setup client buffer to receive from the channel
		clientBuffer.position(0);
		clientBuffer.limit(msg.getSize());
		
		m_channel.read(clientBuffer);
		clientBuffer.flip(); // ready for reading
		
		return msg;
	}
	
}
