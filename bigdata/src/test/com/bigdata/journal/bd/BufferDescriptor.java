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
 * Created on Apr 21, 2010
 */

package com.bigdata.journal.bd;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * An interface encapsulating a unique identifier for a buffer together with the
 * checksum of the data in the buffer and a means to read the data in the
 * buffer. A smart proxy for this interface is used to allow nodes to read on
 * remote buffers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface BufferDescriptor {

    /**
     * An identifier for a specific buffer view when is assigned when the buffer
     * is registered to make its contents available to the failover group
     */
    public UUID getId();

    /**
     * The #of bytes to be read from the buffer.
     */
    public int size();
    
    /**
     * The checksum of the data in the buffer.
     */
    public int getChecksum();

    /**
     * Transfer the data into the caller's {@link ByteBuffer}.
     */
    public void get(ByteBuffer b);
    
}