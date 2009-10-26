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
package com.bigdata.mdi;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.btree.BytesUtil;
import com.bigdata.service.ndx.ClientIndexView;

/**
 * An immutable object that may be used to locate an index partition. Instances
 * of this class are stored as the values in the {@link MetadataIndex}.
 * <p>
 * Note: The {@link ISeparatorKeys#getLeftSeparatorKey()} is always equal to the
 * key under which the {@link PartitionLocator} is stored in the metadata index.
 * Likewise, the {@link ISeparatorKeys#getRightSeparatorKey()} is always equal
 * to the key under which the successor of the index entry is stored (or
 * <code>null</code> iff there is no successor). However, the left and right
 * separator keys are stored explicitly in the values of the metadata index
 * because it <em>greatly</em> simplifies client operations. While the left
 * separator key is directly obtainable from the key under which the locator was
 * stored, the right separator key is much more difficult to obtain in the
 * various contexts within which the {@link ClientIndexView} requires that
 * information.
 * 
 * @todo write a custom serializer for the {@link MetadataIndex} that factors
 *       out the left separator key and which uses prefix compression to only
 *       write the delta for the right separator key over the left separator
 *       key? this would require de-serialization of the partition locator and
 *       then re-serialization in a record format which does not include the
 *       left separator key, but the aggregate space savings could be quite
 *       large. Further space savings could be realized if we simply factored
 *       out both keys in the index but delivered the keys to the client when a
 *       single partition locator was requested. When a key-range of locators is
 *       requested, the space savings again make it worth while to factor out
 *       the left/right keys.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PartitionLocator implements IPartitionMetadata, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 5234405541356126104L;

    /**
     * The unique partition identifier.
o     */
    private int partitionId;

    /**
     * The UUID of the (logical) data service on which the index partition
     * resides.
     */
    private UUID dataServiceUUID;
    
    private byte[] leftSeparatorKey;
    private byte[] rightSeparatorKey;
    
    /**
     * De-serialization constructor.
     */
    public PartitionLocator() {
        
    }

    /**
     * 
     * @param partitionId
     *            The unique partition identifier assigned by the
     *            {@link MetadataIndex}.
     * @param logicalDataServiceUUID
     *            The ordered array of data service identifiers on which data
     *            for this partition will be written and from which data for
     *            this partition may be read.
     * @param leftSeparatorKey
     *            The separator key that defines the left edge of that index
     *            partition (always defined) - this is the first key that can
     *            enter the index partition. The left-most separator key for a
     *            scale-out index is always an empty <code>byte[]</code> since
     *            that is the smallest key that may be defined.
     * @param rightSeparatorKey
     *            The separator key that defines the right edge of that index
     *            partition or <code>null</code> iff the index partition does
     *            not have a right sibling (a <code>null</code> has the
     *            semantics of having no upper bound).
     */
    public PartitionLocator(final int partitionId, final UUID logicalDataServiceUUID,
            final byte[] leftSeparatorKey, final byte[] rightSeparatorKey) {

        if (logicalDataServiceUUID == null)
            throw new IllegalArgumentException();

        if (leftSeparatorKey == null)
            throw new IllegalArgumentException();
        
        // Note: rightSeparatorKey MAY be null.
        
        if (rightSeparatorKey != null) {

            if (BytesUtil.compareBytes(leftSeparatorKey, rightSeparatorKey) >= 0) {

                throw new IllegalArgumentException(
                        "Separator keys are out of order: left="
                                + BytesUtil.toString(leftSeparatorKey)
                                + ", right="
                                + BytesUtil.toString(rightSeparatorKey));
                
            }
            
        }
     
        this.partitionId = partitionId;
        
        this.dataServiceUUID = logicalDataServiceUUID;

        this.leftSeparatorKey = leftSeparatorKey;
        
        this.rightSeparatorKey = rightSeparatorKey;

    }

    final public int getPartitionId() {
        
        return partitionId;
        
    }

    /**
     * The UUID of the (logical) data service on which the index partition
     * resides.
     */
    public UUID getDataServiceUUID() {
        
        return dataServiceUUID;
        
    }
    
    final public byte[] getLeftSeparatorKey() {

        return leftSeparatorKey;
        
    }
    
    final public byte[] getRightSeparatorKey() {
        
        return rightSeparatorKey;
        
    }

    final public int hashCode() {

        // per the interface contract.
        return partitionId;
        
    }

    // Note: used by assertEquals in the test cases.
    public boolean equals(Object o) {

        if (this == o)
            return true;

        final PartitionLocator o2 = (PartitionLocator) o;

        if (partitionId != o2.partitionId)
            return false;

        if (!dataServiceUUID.equals(o2.dataServiceUUID))
            return false;

        if (!BytesUtil.bytesEqual(leftSeparatorKey, o2.leftSeparatorKey))
            return false;

        if (rightSeparatorKey == null && o2.rightSeparatorKey != null)
            return false;

        if (!BytesUtil.bytesEqual(rightSeparatorKey, o2.rightSeparatorKey))
            return false;
        
        return true;

    }

    public String toString() {

        return 
            "{ partitionId="+partitionId+
            ", dataServiceUUID="+dataServiceUUID+
            ", leftSeparator="+BytesUtil.toString(leftSeparatorKey)+
            ", rightSeparator="+(rightSeparatorKey==null?null:BytesUtil.toString(rightSeparatorKey))+
            "}"
            ;

    }

    private static final transient short VERSION0 = 0x0;
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        final short version = ShortPacker.unpackShort(in);
        
        if (version != VERSION0) {
            
            throw new IOException("Unknown version: "+version);
            
        }

        partitionId = (int)LongPacker.unpackLong(in);
        
        dataServiceUUID = new UUID(in.readLong()/*MSB*/,in.readLong()/*LSB*/);
        
        final int leftLen = (int) LongPacker.unpackLong(in);

        final int rightLen = (int) LongPacker.unpackLong(in);

        leftSeparatorKey = new byte[leftLen];

        in.readFully(leftSeparatorKey);

        if (rightLen != 0) {

            rightSeparatorKey = new byte[rightLen];

            in.readFully(rightSeparatorKey);

        } else {

            rightSeparatorKey = null;

        }
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        ShortPacker.packShort(out, VERSION0);
        
        LongPacker.packLong(out, partitionId);

        out.writeLong(dataServiceUUID.getMostSignificantBits());
        
        out.writeLong(dataServiceUUID.getLeastSignificantBits());
            
        LongPacker.packLong(out, leftSeparatorKey.length);

        LongPacker.packLong(out, rightSeparatorKey == null ? 0
                : rightSeparatorKey.length);

        out.write(leftSeparatorKey);

        if (rightSeparatorKey != null) {

            out.write(rightSeparatorKey);

        }
        
    }

}
