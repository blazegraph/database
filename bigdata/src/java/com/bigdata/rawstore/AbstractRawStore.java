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
/*
 * Created on Sep 5, 2007
 */

package com.bigdata.rawstore;

import java.nio.ByteBuffer;

import com.bigdata.LRUNexus;
import com.bigdata.journal.IAllocationContext;

/**
 * Abstract base class for {@link IRawStore} implementations. This class uses a
 * delegation pattern for the {@link IStoreSerializer} interface and does not
 * implement either the methods defined directly by the {@link IRawStore}
 * interface nor the methods of the {@link IAddressManager} interface. As such
 * it may be used as an abstract base class by any {@link IRawStore}
 * implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRawStore implements IRawStore {

    /**
     * Return the delegate object that provides the {@link IAddressManager}
     * implementation for this {@link IRawStore}.
     * <p>
     * Note: {@link LRUNexus} depends on the delegation model to retain
     * references to the {@link IAddressManager} without causing the
     * {@link IRawStore} to be retained. It uses the {@link IAddressManager} to
     * decode the address in order to track the bytesOnDisk for the buffered
     * records.
     */
    abstract public IAddressManager getAddressManager();
   
    /**
     * The designated constructor.
     */
    public AbstractRawStore() {

    }

    /**
     * The default implementation delegates to {@link #write(ByteBuffer)}.
     */
    public long write(ByteBuffer data, long oldAddr) {
    	return write(data);
    }

    /**
     * The default implementation is a NOP.
     */
	public void delete(long addr) {
	    // NOP.
	}

	public void delete(long addr, IAllocationContext context) {
		delete(addr);
	}

	public long write(ByteBuffer data, IAllocationContext context) {
		return write(data);
	}

	public long write(ByteBuffer data, long oldAddr, IAllocationContext context) {
		return write(data, oldAddr);
	}

    /**
     * The default implementation is a NOP.
     */
	public void detachContext(IAllocationContext context) {
		// NOP
	}
}
