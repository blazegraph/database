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
 * Created on Oct 22, 2006
 */

package com.bigdata.journal;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;


/**
 * Utility class for encoding and decoding an {@link ISlotAllocation}.
 * 
 * @todo Perhaps use int (offset) + byte (runLength) and do not try to use the
 *       packing routines? This means less memory allocation during encoding or
 *       decoding since we do not have to setup streams. The decision here needs
 *       to examine the context in which we are encoding and decoding. If we are
 *       using extser to serialize the nodes of the object index then it might
 *       be easy to do this as part of serialization since the required streams
 *       will already exist and we will not have to worry about marking the byte
 *       start and length of the encoding. This scenario is likely if we believe
 *       that the object index is a general purpose btree. In that event, just
 *       implement the appropriate Serializer helper class to be used by any of
 *       the {@link ISlotAllocation} classes.
 * 
 * @todo Write tests for encoding and decoding. There are a lot of fence posts
 *       dealing with correct detection of run length and multiple runs.
 *       
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SlotAllocationHelper {

    /**
     * Encode an instance. This is used to compact or "freeze" an read-only
     * {@link ISlotAllocation} when it is placed into the {@link IObjectIndex}.
     * There is an expectation that space efficiency domainates runtime access
     * for this use case.
     * 
     * @param src
     *            The {@link ISlotAllocation} to be encoded.
     * 
     * @todo This does memory allocation that might be reduced through the reuse
     *       of buffers, especially if we make a single threading assumption and
     *       perhaps for the case where nslots == 1 in any case.
     */
    static public byte[] encode(ISlotAllocation src) {

        assert src != null;
        
//        if( src.size() == 1 ) { /* @todo optimize when nslots == 1 */}
            
        final int firstSlot = src.firstSlot();

        if (firstSlot == -1)
            throw new IllegalArgumentException();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {

            final DataOutputStream dos = new DataOutputStream(baos);

            LongPacker.packLong(dos, firstSlot);

            int lastSlot = firstSlot;

            do {

                int nextSlot;

                short runLength = 1;

                while ((nextSlot = src.nextSlot()) != -1) {

                    if (nextSlot == lastSlot + 1) {

                        runLength++;

                        continue;

                    }

                    break;

                }

                ShortPacker.packShort(dos, runLength);

                lastSlot = nextSlot;

            } while (lastSlot != -1);

            dos.flush();

        }

        catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        return baos.toByteArray();

    }

    /**
     * Decode an existing serialized instance.
     * 
     * @param src
     */
    static public ISlotAllocation decode(byte[] src) {

        throw new UnsupportedOperationException();

    }

}
