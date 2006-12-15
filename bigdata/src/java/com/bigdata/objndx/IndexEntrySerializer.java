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
 * Created on Nov 20, 2006
 */

package com.bigdata.objndx;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.journal.Bytes;
import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.SlotMath;

/**
 * Serializer for values in the leaves of the object index. The values are
 * packed to minimize the space requirements of a leaf.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Since we are packing the versionCounter we could get away with using an
 *       int32 or even int64 field.
 * 
 * @todo the version counter will probably become the transaction timestamp and
 *       the object will probably be moved inline so that it is stored in the
 *       leaf of the tree rather than scattered randomly in the journal and the
 *       distributed store.
 */
public class IndexEntrySerializer implements IValueSerializer {

    private final SlotMath slotMath;

    /**
     * The maximum size of an {@link ISlotAllocation} encoded as a long integer.
     * This is used for references to data versions in the journal. Since we do
     * not know the size of the referenced objects in advanced we have to
     * serialize the full int64 value. However, we can generally make them more
     * compact than int64 by breaking the {@link ISlotAllocation} down into the
     * #of bytes and the firstSlot identifier and serializing each of those as a
     * packed positive int32 value.
     */
    static final int SIZEOF_SLOTS = Bytes.SIZEOF_LONG;
    
    /**
     * The maximum size of a serialized version counter.
     */
    static final int SIZEOF_VERSION_COUNTER = Bytes.SIZEOF_SHORT;
    
    /**
     * The maximum size of a serialized value.
     */
    static final int SIZEOF_LEAF_VALUE
            = SIZEOF_VERSION_COUNTER // versionCounter
            + SIZEOF_SLOTS // currentVersion (slots as long)
            + SIZEOF_SLOTS // preExistingVersion (slots as long)
            ;

    /**
     * 
     * @param slotMath
     *            Used to decode a long integer encoding an
     *            {@link ISlotAllocation}.
     */
    public IndexEntrySerializer(SlotMath slotMath) {
    
        assert slotMath != null;
        
        this.slotMath = slotMath;
        
    }
    
    /**
     * The maximum size of a serialized value in bytes.  The values are packed
     * and a leaf in practice will require much less space.
     */
    public int getSize(int n) {
        
        return n * SIZEOF_LEAF_VALUE;
        
    }

    /**
     * Values are packed as follows:
     * <dl>
     * <dt>versionCounter</dt>
     * <dd>Packed non-negative short integer.</dd>
     * <dt>currentVersionSlots</dt>
     * <dd>When null, a packed long integer whose value is zero(0). Otherwise
     * the {@link ISlotAllocation} is broken down into the #of bytes and the
     * first slot. Since of those is a positive int32 value, we write them as
     * packed integers. This results in even few bytes of storage when compared
     * with writing them as a single positive long integer since we get to elide
     * the leading zero bytes for both values.</dd>
     * <dt>preExistingVersionSlots</dt>
     * <dd>As per currentVersionSlots.</dd>
     * </dl>
     */
    public void putValues(DataOutputStream os, Object[] values, int nvals)
            throws IOException {

        assert os != null;
        assert values != null;
        assert nvals >= 0;

        if( nvals == 0 ) return;
        
        for (int i = 0; i < nvals; i++) {

            IndexEntry entry = (IndexEntry) values[i];

            assert entry != null;
            
            final short versionCounter = entry.versionCounter;
            assert versionCounter >= 0;
            
            // May be zero (indicates the current version is deleted).
            final long currentVersionSlots = entry.currentVersion;
            assert currentVersionSlots>=0;

            // May be null (indicates first version in this isolation/tx).
            final long preExistingVersionSlots = entry.preExistingVersion;
            assert preExistingVersionSlots>=0;

                /*
                 * versionCounter.
                 */
                ShortPacker.packShort(os, versionCounter);

                /*
                 * currentVersionSlots.
                 */
                if( currentVersionSlots == 0L) {

                    LongPacker.packLong(os, 0L);
                
                } else {
                    
                    int nbytes = SlotMath.getByteCount(currentVersionSlots);
                    
                    // this assert is critical or the de-serialization will break.
                    assert nbytes>0;
                    
                    int firstSlot = SlotMath.getFirstSlot(currentVersionSlots);
                    
                    assert firstSlot>0;
                    
                    LongPacker.packLong(os, nbytes);

                    LongPacker.packLong(os, firstSlot);

                }

                /*
                 * preExistingVersionSlots.
                 */
                if (preExistingVersionSlots == 0L ) {

                    LongPacker.packLong(os, 0L);

                } else {

                    int nbytes = SlotMath.getByteCount(preExistingVersionSlots);

                    // this assert is critical or the de-serialization will break.
                    assert nbytes>0;
                    
                    int firstSlot = SlotMath.getFirstSlot(preExistingVersionSlots);
                    
                    assert firstSlot>0;

                    LongPacker.packLong(os, nbytes);

                    LongPacker.packLong(os, firstSlot);

                }

                os.flush();

        }
        
    }

    public void getValues(DataInputStream is, Object[] values, int n)
        throws IOException
    {

        assert is != null;
        assert values != null;
        assert n >= 0;

        if( n == 0 ) return;
        
        for (int i = 0; i < n; i++) {

                final short versionCounter = ShortPacker.unpackShort(is);

                final long currentVersion;
                {

                    long v = LongPacker.unpackLong(is);
                    
                    if( v == 0L ) {
                    
                        currentVersion = 0L;
                        
                    } else {
                    
                        assert v>0 && v<Integer.MAX_VALUE;
                        
                        long v1 = LongPacker.unpackLong(is);

                        assert v1>0 && v1<Integer.MAX_VALUE;
                        
                        int byteCount = (int)v;
                        
                        int firstSlot = (int)v1;
                    
                        currentVersion = SlotMath.toLong(byteCount, firstSlot);
                        
                    }
                    
                }

                final long preExistingVersion;
                {

                    long v = LongPacker.unpackLong(is);
                    
                    if( v == 0L ) {
                    
                        preExistingVersion = 0L;
                        
                    } else {
                        
                        assert v>0 && v<Integer.MAX_VALUE;
                        
                        long v1 = LongPacker.unpackLong(is);

                        assert v1>0 && v1<Integer.MAX_VALUE;
                        
                        int byteCount = (int)v;
                        
                        int firstSlot = (int)v1;
                
                        preExistingVersion = SlotMath.toLong(byteCount, firstSlot);
                        
                    }
                    
                }
                
                values[i] = new IndexEntry(slotMath, versionCounter,
                        currentVersion, preExistingVersion);
            
        }

    }
    
}
