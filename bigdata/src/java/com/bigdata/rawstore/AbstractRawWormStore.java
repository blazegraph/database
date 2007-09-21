/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

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
