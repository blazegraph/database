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
/*
 * Created on May 18, 2010
 */

package com.bigdata.journal;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.bigdata.journal.ha.HAWriteMessage;
import com.bigdata.journal.ha.Quorum;

/**
 * A highly available {@link IBufferStrategy}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IHABufferStrategy extends IBufferStrategy {

    /**
     * Write a buffer containing data replicated from the master onto the local
     * persistence store.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    void writeRawBuffer(HAWriteMessage msg, ByteBuffer b) throws IOException,
            InterruptedException;

    /**
     * Read from the local store in support of failover reads on nodes in a
     * highly available {@link Quorum}.
     * 
     * @throws InterruptedException
     */
    ByteBuffer readFromLocalStore(final long addr) throws InterruptedException;

    /**
     * Extend local store for a highly available {@link Quorum}.
     * 
     * @throws InterruptedException
     */
    void setExtentForLocalStore(final long extent) throws IOException,
    		InterruptedException;
    
}
