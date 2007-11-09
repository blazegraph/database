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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * An abstract base class for {@link IRawStore} implementations that use an
 * append only (Write Once, Read Many) strategy. The {@link IAddressManager}
 * interface is delegated to an {@link WormAddressManager} allowing flexible
 * configuration of the use of bits to represent the byte offset of a record in
 * the store and the bits used to represent the size of a record in the store.
 * 
 * @see WormAddressManager
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRawWormStore extends AbstractRawStore implements IWORM {

    /**
     * The object that knows how to encode, decode, and (de-)serialize
     * addresses.
     */
    protected final WormAddressManager am;
    
    /**
     * The object that knows how to encode, decode, and (de-)serialize
     * addresses.
     */
    public final WormAddressManager getAddressManger() {
        
        return am;
        
    }
    
    /**
     * The #of bits out of a 64-bit long integer that are used to encode the
     * byte offset as an unsigned integer.  The remaining bits are used to
     * encode the byte count (aka record length) as an unsigned integer.
     */
    public final int getOffsetBits() {
        
        return am.offsetBits;
        
    }
    
    /**
     * @param offsetBits
     *            The #of bits that will be used to represent the byte offset in
     *            the 64-bit long integer addresses for the store. See
     *            {@link WormAddressManager}.
     */
    public AbstractRawWormStore(int offsetBits) {
        
        super();
        
        this.am = new WormAddressManager(offsetBits);
        
    }

    /*
     * IAddressManager
     */
    
    final public long toAddr(int nbytes, long offset) {
        
        return am.toAddr(nbytes, offset);
        
    }

    final public long getOffset(long addr) {
        
        return am.getOffset(addr);
        
    }

    final public int getByteCount(long addr) {

        return am.getByteCount(addr);
        
    }

    final public void packAddr(DataOutput out, long addr) throws IOException {

        am.packAddr(out, addr);
        
    }

    final public long unpackAddr(DataInput in) throws IOException {

        return am.unpackAddr(in);
        
    }

    final public String toString(long addr) {

        return am.toString(addr);
        
    }

}
