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
package com.bigdata.ha.msg;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.bigdata.io.compression.IRecordCompressor;
import com.bigdata.journal.StoreTypeEnum;

/**
 * A message carrying RMI metadata about a payload which will be replicated
 * using a socket-level transfer facility.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IHAWriteMessage extends IHAWriteMessageBase {

    /** The {@link UUID} of the store to which this message belongs. */
    UUID getUUID();
    
    /** The commit counter for the opening root block associated with the write set for this message.*/
    long getCommitCounter();

    /** The commit time associated with this message. */
    long getLastCommitTime();

    /**
     * The write cache buffer sequence number (reset to ZERO (0) for the first
     * message after each commit and incremented for each buffer sent by the
     * leader).
     */
    long getSequence();

    /**
     * Applies associated {@link IRecordCompressor} (if any) to decompress the
     * data
     */
    ByteBuffer expand(ByteBuffer bin);
    
    /**
     * Return the associated {@link IRecordCompressor} key (if any).
     */
    String getCompressorKey();

    /** The type of backing store (RW or WORM). */
    StoreTypeEnum getStoreType();

    /** The quorum token for which this message is valid. */
    long getQuorumToken();

    /** The replication factor for the quorum leader. */
    int getReplicationFactor();

    /** The length of the backing file on the disk. */
    long getFileExtent();

    /** The file offset at which the data will be written (WORM only or for Rebuild request). */
    long getFirstOffset();

}
