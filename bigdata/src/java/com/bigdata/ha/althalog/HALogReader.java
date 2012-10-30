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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

import com.bigdata.ha.althalog.HALogFile.IHALogFileCallback;
import com.bigdata.ha.althalog.HALogFile.IHALogFileReaderCallback;
import com.bigdata.ha.althalog.HALogFile.IReadPosition;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.IReopenChannel;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockUtility;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.journal.WORMStrategy;
import com.bigdata.util.ChecksumError;
import com.bigdata.util.ChecksumUtility;

/**
 * Given an HALog file can be used to replay the file and can provide a readable
 * dump of the content.
 * 
 * When replaying, the current position is compared to the EOF to determine
 * whether more data can be read.
 * 
 * The called should call hasMoreBuffers() and if so read the next associated
 * buffer and process with the returned IHAMessage.
 * 
 * If hasMoreBuffers() is false, then the committing rootBlock should be used to
 * commit the replayed transaction.
 * 
 * @author Martyn Cutcher
 */
public class HALogReader implements IHALogReader {

	private static final Logger log = Logger.getLogger(HALogReader.class);
	
	private final HALogFile m_file;

	private final IHALogFileReaderCallback m_callback;
	
	private int m_record = 0;

	public HALogReader(final IHALogFileReaderCallback callback) throws IOException {

		m_callback = callback;
		
		m_file = m_callback.getHALogFile();

	}

	public void close() throws IOException {

		m_callback.close();

	}

	public boolean isEmpty() {
		
		return m_file.isEmpty();

	}

	private void assertOpen() throws IOException {

		if (!m_callback.isOpen())
			throw new IOException("Closed: " + m_file);

	}

	/**
	 * The {@link IRootBlockView} for the committed state BEFORE the write set
	 * contained in the HA log file.
	 */
	public HALogFile getHALogFile() {

		return m_file;

	}

	public boolean hasMoreBuffers() throws IOException {

		assertOpen();

		return m_callback.waitOnData(m_readPosition.getPosition());

	}

	private IReadPosition m_readPosition = new IReadPosition() {
		long m_position = HALogFile.START_DATA;
		
		@Override
		public long getPosition() {
			return m_position;
		}

		@Override
		public void setPosition(long position) {
			m_position = position;
		}
		
	};
	
	public IHAWriteMessage processNextBuffer(final ByteBuffer clientBuffer)
			throws IOException {

		return m_callback.processNextBuffer(m_readPosition, clientBuffer);

	}


	@Override
	public IRootBlockView getClosingRootBlock() throws IOException {
		return m_callback.getClosingRootBlock();
	}

	public IRootBlockView getOpeningRootBlock() {
		return m_callback.getOpeningRootBlock();
	}
}
