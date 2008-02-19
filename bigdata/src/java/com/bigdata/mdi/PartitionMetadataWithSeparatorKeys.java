/*

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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.UUID;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.Journal;


/**
 * An immutable object whose state describes an index partition together with
 * the left and right separator keys surrounding the index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PartitionMetadataWithSeparatorKeys extends PartitionMetadata {

    /**
     * 
     */
    private static final long serialVersionUID = -1511361004851335936L;
    
    private byte[] leftSeparatorKey;
    private byte[] rightSeparatorKey;

    /**
     * De-serialization constructor.
     */
    public PartitionMetadataWithSeparatorKeys() {
        
    }
    
    /**
     * 
     * @param partId
     *            The unique partition identifier assigned by the
     *            {@link MetadataIndex}.
     * @param dataServices
     *            The ordered array of data service identifiers on which data
     *            for this partition will be written and from which data for
     *            this partition may be read.
     * @param resources
     *            A description of each {@link Journal} or {@link IndexSegment}
     *            resource associated with that partition. The entries in the
     *            array reflect the creation time of the resources. The earliest
     *            resource is listed first. The most recently created resource
     *            is listed last.
     * @param leftSeparatorKey
     *            The first key that can enter this index partition. The left
     *            separator key for the first index partition is always
     *            <code>new byte[]{}</code>. The left separator key MAY NOT
     *            be <code>null</code>.
     * @param rightSeparatorKey
     *            The first key that is excluded from this index partition or
     *            <code>null</code> iff there is no upper bound.
     */
    public PartitionMetadataWithSeparatorKeys(int partitionId,
            UUID[] dataServices, IResourceMetadata[] resources,
            byte[] leftSeparatorKey, byte[] rightSeparatorKey) {

        super(partitionId, dataServices, resources);

        if (leftSeparatorKey == null)
            throw new IllegalArgumentException("leftSeparatorKey");
        
        // Note: rightSeparatorKey MAY be null.
        
        if(rightSeparatorKey!=null) {
            
            if(BytesUtil.compareBytes(leftSeparatorKey, rightSeparatorKey)>=0) {
                
                throw new IllegalArgumentException(
                        "Separator keys are out of order: left="
                                + Arrays.toString(leftSeparatorKey)
                                + ", right="
                                + Arrays.toString(rightSeparatorKey));
                
            }
            
        }
        
        this.leftSeparatorKey = leftSeparatorKey;
        
        this.rightSeparatorKey = rightSeparatorKey;
        
    }

    public PartitionMetadataWithSeparatorKeys(byte[] leftSeparatorKey,
            IPartitionMetadata src, byte[] rightSeparatorKey) {

        this(src.getPartitionId(), src.getDataServices(), src
                .getResources(), leftSeparatorKey, rightSeparatorKey);
        
    }
    
    /**
     * The separator key that defines the left edge of that index partition
     * (always defined) - this is the first key that can enter the index
     * partition. The left-most separator key for a scale-out index is
     * always an empty byte[] since that is the smallest key that may be
     * defined.
     */
    public byte[] getLeftSeparatorKey() {

        return leftSeparatorKey;
        
    }
    
    /**
     * The separator key that defines the right edge of that index partition
     * or [null] iff the index partition does not have a right sibling (a
     * null has the semantics of no upper bound).
     */
    public byte[] getRightSeparatorKey() {
        
        return rightSeparatorKey;
        
    }
    
    public String toString() {

        return 
        "{ partitionId="+getPartitionId()+
        ", dataServices="+Arrays.toString(getDataServices())+
        ", resourceMetadata="+Arrays.toString(getResources())+
        ", leftSeparator="+Arrays.toString(getLeftSeparatorKey())+
        ", rightSeparator="+(getRightSeparatorKey()==null?"null":Arrays.toString(getRightSeparatorKey()))+
        "}"
        ;

    }
    
    private static final transient short VERSION0 = 0x0;
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        super.readExternal(in);
        
        final short version = ShortPacker.unpackShort(in);
        
        if(version!=VERSION0) {
            
            throw new IOException("Unknown version: "+version);
            
        }

        final int leftLen = (int) LongPacker.unpackLong(in);

        final int rightLen = (int) LongPacker.unpackLong(in);

        leftSeparatorKey = new byte[leftLen];

        in.read(leftSeparatorKey);

        if (rightLen != 0) {

            rightSeparatorKey = new byte[rightLen];

            in.read(rightSeparatorKey);

        } else {

            rightSeparatorKey = null;

        }

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        super.writeExternal(out);

        ShortPacker.packShort(out, VERSION0);

        LongPacker.packLong(out, leftSeparatorKey.length);

        LongPacker.packLong(out, rightSeparatorKey == null ? 0
                : rightSeparatorKey.length);

        out.write(leftSeparatorKey);

        if (rightSeparatorKey != null) {

            out.write(rightSeparatorKey);

        }
        
    }
    
}