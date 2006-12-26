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
 * Created on Dec 26, 2006
 */

package com.bigdata.objndx;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.bigdata.journal.Bytes;

/**
 * Serializes each address as an unpacked long integer.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AddressSerializer implements IAddressSerializer {

    public static final IAddressSerializer INSTANCE = new AddressSerializer();
    
    private AddressSerializer() {}

    /**
     * This returns the exact space requirements.
     */
    public int getSize(int n) {
        
        return Bytes.SIZEOF_LONG * n;
        
    }

    public void putChildAddresses(DataOutputStream os, long[] childAddr,
            int nchildren) throws IOException {

        for (int i = 0; i < nchildren; i++) {

            final long addr = childAddr[i];

            /*
             * Children MUST have assigned persistent identity.
             */
            if (addr == 0L) {

                throw new RuntimeException("Child is not persistent: index="
                        + i);

            }

            os.writeLong(addr);

        }

    }

    public void getChildAddresses(DataInputStream is, long[] childAddr,
            int nchildren) throws IOException {

        for (int i = 0; i < nchildren; i++) {

            final long addr = is.readLong();

            if (addr == 0L) {

                throw new RuntimeException(
                        "Child does not have persistent address: index=" + i);

            }

            childAddr[i] = addr;

        }

    }

}
