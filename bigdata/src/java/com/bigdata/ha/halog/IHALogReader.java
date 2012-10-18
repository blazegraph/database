package com.bigdata.ha.halog;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.WORMStrategy;

public interface IHALogReader {
	
	/**
	 * Closes the Reader.
	 * 
	 * @throws IOException
	 */
	void close() throws IOException;
	
	/**
	 * Return <code>true</code> if the root blocks in the log file have the same
	 * commit counter. Such log files are logically empty regardless of their
	 * length.
	 */
	boolean isEmpty();
	
	/**
	 * The {@link IRootBlockView} for the committed state AFTER the write set
	 * contained in the HA log file has been applied.
	 */
	IRootBlockView getClosingRootBlock() throws IOException;
	
	/**
	 * Checks whether we have reached the end of the file.
	 */
	boolean hasMoreBuffers() throws IOException;
	
	/**
	 * Attempts to read the next {@link IHAWriteMessage} and then the expected
	 * buffer, that is read into the client buffer. The {@link IHAWriteMessage}
	 * is returned to the caller.
	 * <p>
	 * Note: The caller's buffer will be filled in IFF the data is on the HALog.
	 * For some {@link IHABufferStrategy} implementations, that data is not
	 * present in the HALog. The caller's buffer will not be modified and the
	 * caller is responsible for getting the data from the
	 * {@link IHABufferStrategy} (e.g., for the {@link WORMStrategy}).
	 * <p>
	 * Note: IF the buffer is filled, then the limit will be the #of bytes ready
	 * to be transmitted and the position will be zero.
	 * 
	 * @param clientBuffer
	 *            A buffer from the {@link DirectBufferPool#INSTANCE}.
	 */
	IHAWriteMessage processNextBuffer(final ByteBuffer clientBuffer) throws IOException;

}
