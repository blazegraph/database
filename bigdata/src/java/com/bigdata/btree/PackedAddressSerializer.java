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
 * Created on Dec 26, 2006
 */

package com.bigdata.btree;

import java.io.DataInput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;

/**
 * Packs the addresses using the {@link IAddressManager} for the backing
 * {@link IRawStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PackedAddressSerializer implements IAddressSerializer, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 7533128830948670801L;
    
    public static final IAddressSerializer INSTANCE = new PackedAddressSerializer();

    public PackedAddressSerializer() {
        
    }
    
    public void putChildAddresses(IAddressManager addressManager, DataOutputBuffer os,
            long[] childAddr, int nchildren) throws IOException {

        for (int i = 0; i < nchildren; i++) {

            final long addr = childAddr[i];

            /*
             * Children MUST have assigned persistent identity.
             */
            if (addr == 0L) {

                throw new RuntimeException("Child is not persistent: index="
                        + i);

            }

            addressManager.packAddr(os, addr);

        }

    }

    public void getChildAddresses(IAddressManager addressManager, DataInput is,
            long[] childAddr, int nchildren) throws IOException {

        for (int i = 0; i < nchildren; i++) {

            final long addr = addressManager.unpackAddr(is);

            if (addr == 0L) {

                throw new RuntimeException(
                        "Child does not have persistent address: index=" + i);

            }

            childAddr[i] = addr;

        }

    }


    public void readExternal(ObjectInput arg0) throws IOException, ClassNotFoundException {

        // NOP (no state)
        
    }

    public void writeExternal(ObjectOutput arg0) throws IOException {

        // NOP (no state)

    }

}
