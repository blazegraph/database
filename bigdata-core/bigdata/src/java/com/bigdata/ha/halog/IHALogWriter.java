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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.journal.IRootBlockView;

/**
 * A constrained interface providing access to limited operations on the
 * live HALog.
 * 
 * @author Martyn Cutcher
 * @author Bryan Thompson
 */
public interface IHALogWriter {

    /**
     * Return <code>true</code> iff there is an HALog file that is
     * currently open for writing.
     */
    public boolean isHALogOpen();

    /**
     * Return the commit counter that is expected for the writes that will be
     * logged (the same commit counter that is on the opening root block).
     * <p>
     * Note: Once the HALog is sealed, the closing root block will have a
     * commitCounter that is equal to <code>getCommitCounter() + 1</code>.
     * <p>
     * Note: The HALog filename contains the closing commit counter - that is,
     * the HALog file is named for the commit counter associated with the
     * closing root block for a given write set.
     */
    public long getCommitCounter();

    /**
     * Return the sequence number that is expected for the next write.
     */
    public long getSequence();

    /**
     * Write the message and the data on the live HALog.
     * 
     * @param msg
     *            The message.
     * @param data
     *            The data.
     * @throws IllegalStateException
     *             if the message is not appropriate for the state of the log.
     * @throws IOException
     *             if we can not write on the log.
     */
    public void writeOnHALog(IHAWriteMessage msg, ByteBuffer data) throws IOException;
    
    /**
     * Write the final root block on the HA log and close the file. This "seals"
     * the file, which now represents the entire write set associated with the
     * commit point in the given root block.
     * 
     * @param rootBlock
     *            The final root block for the write set.
     * @throws IOException
     */
    public void closeHALog(IRootBlockView rootBlock) throws IOException;
    
    /**
     * Disable (and remove) the current log file if one is open (an HALog file
     * which has been committed by applying its closing root block is NOT
     * removed).
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/695">
     *      HAJournalServer reports "follower" but is in SeekConsensus and is
     *      not participating in commits</a>
     */
    public void disableHALog() throws IOException;

}
