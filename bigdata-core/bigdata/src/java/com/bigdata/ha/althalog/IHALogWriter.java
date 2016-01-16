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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.journal.IRootBlockView;

/**
 * A constrained interface to a new HALogFile to allow writing.
 * <p>
 * In order to write to an HALogFile, a writer must be requested. The
 * implementation is private to the HALogFile.
 * 
 * @author Martyn Cutcher
 */
public interface IHALogWriter {

	/**
	 * @return the next expected write sequence.
	 */
	public long getSequence();

	/**
	 * @return a String showing the current state of the writer
	 */
	public String toString();

	/**
	 * Writes the message to the file and the associated data if the
	 * backing store is not a WORM.
	 * 
	 * @param msg
	 * @param data
	 * @throws IOException
	 */
	public void write(IHAWriteMessage msg, ByteBuffer data) throws IOException;
	
	/**
	 * Writes the closing rootblock to the log file and closes the
	 * file to further writes.
	 * 
	 * @param rbv is the rootblock associated with the commit point
	 * @throws IOException
	 */
	public void close(IRootBlockView rbv) throws IOException;
	
	/**
	 * Close the file (does not flush).
	 * @throws IOException 
	 */
	public void close() throws IOException;

	/**
	 * The commit counter for the committed state BEFORE the write
	 * set contained in the file is applied.
	 */
	public long getCommitCounter();

}
