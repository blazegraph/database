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
package com.bigdata.ha.althalog;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;

import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.WORMStrategy;

/**
 * Interface for reading on an HA Log.
 * 
 * Readers can be requested of any HALogFile.
 */
public interface IHALogReader {
	
    /**
     * The filename extension used for the HALog files.
     */
    public static final String HA_LOG_EXT = ".ha-log";

    /**
     * A {@link FileFilter} that visits all files ending with the
     * {@link #HA_LOG_EXT} and the names of all direct child directories. This
     * {@link FileFilter} may be used to establish recursive scans of the HALog
     * directory.
     */
    static public final FileFilter HALOG_FILTER = new FileFilter() {

        @Override
        public boolean accept(final File f) {

            if (f.isDirectory()) {

                return true;

            }

            return f.getName().endsWith(HA_LOG_EXT);

        }

    };

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
	 * The {@link IRootBlockView} for the committed state BEFORE the write set
	 * contained in the HA log file has been applied.
	 */
	IRootBlockView getOpeningRootBlock();
	
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

	void computeDigest(MessageDigest digest) throws DigestException,
			IOException;

}
