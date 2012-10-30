/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.ha.althalog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Formatter;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.eclipse.jetty.util.log.Log;

import com.bigdata.ha.althalog.HALogFile.IHALogFileCallback;
import com.bigdata.ha.althalog.HALogFile.IHALogFileWriterCallback;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockUtility;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.rawstore.Bytes;

/**
 * Wrapper class to handle process log creation and output for HA.
 * 
 * The process log stores the HAWriteMessages and buffers to support reading and
 * reprocessing as part of the HA synchronization protocol.
 * 
 * The writer encapsulates not only the writing of individual messages but also
 * the closing and creation of new files.
 * 
 * @author Martyn Cutcher
 */
public class HALogWriter {

	/**
	 * Logger for HA events.
	 */
	private static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

	private final HALogFile m_haLogFile;

	private final IHALogFileWriterCallback m_callback;

	/**
	 * The root block of the leader at the start of the current write set.
	 */
	private IRootBlockView m_rootBlock;

	/** Current write cache block sequence counter. */
	private long m_nextSequence = 0;

	/** state lock **/
	final private ReentrantReadWriteLock m_stateLock = new ReentrantReadWriteLock();

	public static final String HA_LOG_EXT = ".ha-log";

	/**
	 * Return the commit counter that is expected for the writes that will be
	 * logged (the same commit counter that is on the opening root block).
	 */
	public long getCommitCounter() {

		return m_rootBlock.getCommitCounter();

	}

	private void assertOpen() {

		if (!m_callback.isOpen())
			throw new IllegalStateException();

	}

	/**
	 * Return the sequence number that is expected for the next write.
	 */
	public long getSequence() {

		assertOpen();

		return m_nextSequence;

	}

	public String toString() {

		final IRootBlockView tmp = m_rootBlock;

		final long seq = m_nextSequence;

		return getClass().getName() + "{" + ((!m_callback.isOpen()) ? "closed"
				: "commitCounter=" + tmp.getCommitCounter() + ",nextSequence="
						+ seq) + "}";

	}

	public HALogWriter(final IHALogFileWriterCallback callback) {

		m_callback = callback;
		m_haLogFile = m_callback.getHALogFile();
		
	}

	public void write(final IHAWriteMessage msg, final ByteBuffer data) throws IOException {
		m_callback.write(msg, data);
	}
	
	public void close(final IRootBlockView rbv) throws IOException {		
		m_callback.close(rbv);
	}
	
	/**
	 * Utility to return a ByteBuffer containing the external version of the
	 * object.
	 * 
	 * @return The {@link ByteBuffer}. The position will be zero. The limit will
	 *         be the #of bytes in the serialized object.
	 */
	private ByteBuffer bufferObject(final Object obj) throws IOException {

		// Note: pos=0; limit=capacity=length.
		return ByteBuffer.wrap(SerializerUtil.serialize(obj));

	}

	/**
	 * Close the file (does not flush).
	 * @throws IOException 
	 */
	public void close() throws IOException {
		m_callback.close();
	}

}
