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
 * Created on Nov 5, 2006
 */

package com.bigdata.objndx;

import java.nio.ByteBuffer;
import java.util.zip.Adler32;

/**
 * Utility class for computing the {@link Adler32} checksum of a buffer.  This
 * class is NOT thread-safe.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ChecksumUtility {

    /**
     * Private helper object.
     */
    private final Adler32 chk = new Adler32();

    /**
     * Compute the {@link Adler32} checksum of the buffer.  The position,
     * mark, and limit are unchanged by this operation.  The operation is
     * optimized when the buffer is backed by an array.
     * 
     * @param buf
     *            The buffer.
     * @param pos
     *            The starting position.
     * @param limit
     *            The limit.
     * 
     * @return The checksum.
     */
    public int checksum(ByteBuffer buf, int pos, int limit) {
        
        assert buf != null;
        assert pos >= 0;
        assert limit > pos;
        
        // reset before computing the checksum.
        chk.reset();
        
        if (buf.hasArray()) {

            /*
             * Optimized when the buffer is backed by an array.
             */
            
            final byte[] bytes = buf.array();
            
            final int len = limit - pos;
            
            chk.update(bytes, pos, len);
            
        } else {
            
            for (int i = pos; i < limit; i++) {
                
                chk.update(buf.get(i));
                
            }
            
        }
        
        /*
         * The Adler checksum is a 32-bit value.
         */
        
        return (int) chk.getValue();
        
    }

}
