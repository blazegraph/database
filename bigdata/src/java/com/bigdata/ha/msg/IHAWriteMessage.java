/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
package com.bigdata.ha.msg;

import com.bigdata.journal.StoreTypeEnum;

/**
 * A message carrying RMI metadata about a payload which will be replicated
 * using a socket-level transfer facility.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IHAWriteMessage extends IHAWriteMessageBase {

    /** The commit counter associated with this message */
    long getCommitCounter();

    /** The commit time associated with this message. */
    long getLastCommitTime();

    /**
     * The write cache buffer sequence number (reset to ZERO (0) for the first
     * message after each commit and incremented for each buffer sent by the
     * leader).
     */
    long getSequence();

    /** The type of backing store (RW or WORM). */
    StoreTypeEnum getStoreType();

    /** The quorum token for which this message is valid. */
    long getQuorumToken();

    /** The length of the backing file on the disk. */
    long getFileExtent();

    /** The file offset at which the data will be written (WORM only). */
    long getFirstOffset();

}