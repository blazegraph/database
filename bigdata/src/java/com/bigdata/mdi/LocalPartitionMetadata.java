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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.UUID;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentFileStore;
import com.bigdata.journal.Journal;


/**
 * An immutable object providing metadata about a local index partition,
 * including the partition identifier, the left and right separator keys
 * defining the half-open key range of the index partition, and optionally
 * defining the {@link IResourceMetadata}[] required to materialize a view of
 * that index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LocalPartitionMetadata implements IPartitionMetadata,
        Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = -1511361004851335936L;
    
    /**
     * The unique partition identifier.
     */
    private int partitionId;

    /**
     * 
     */
    private byte[] leftSeparatorKey;
    private byte[] rightSeparatorKey;
    
    /**
     * Description of the resources required to materialize a view of the index
     * partition (optional - not stored when the partition metadata is stored on
     * an {@link IndexSegmentFileStore}).
     * <p>
     * The entries in the array reflect the creation time of the resources. The
     * earliest resource is listed first. The most recently created resource is
     * listed last.
     */
    private IResourceMetadata[] resources;
    
    /**
     * De-serialization constructor.
     */
    public LocalPartitionMetadata() {
        
    }
    
    /**
     * 
     * @param partId
     *            The unique partition identifier assigned by the
     *            {@link MetadataIndex}.
     * @param leftSeparatorKey
     *            The first key that can enter this index partition. The left
     *            separator key for the first index partition is always
     *            <code>new byte[]{}</code>. The left separator key MAY NOT
     *            be <code>null</code>.
     * @param rightSeparatorKey
     *            The first key that is excluded from this index partition or
     *            <code>null</code> iff there is no upper bound.
     * @param resources
     *            A description of each {@link Journal} or {@link IndexSegment}
     *            resource(s) required to compose a view of the index partition
     *            (optional).
     *            <p>
     *            The entries in the array reflect the creation time of the
     *            resources. The earliest resource is listed first. The most
     *            recently created resource is listed last.
     *            <p>
     *            Note: This is required if the {@link LocalPartitionMetadata}
     *            record will be saved on the {@link IndexMetadata} of a
     *            {@link BTree}. It is NOT recommended when it will be saved on
     *            the {@link IndexMetadata} of an {@link IndexSegment}.
     */
    public LocalPartitionMetadata(//
            int partitionId,//
            byte[] leftSeparatorKey,//
            byte[] rightSeparatorKey,// 
            IResourceMetadata[] resources//
            ) {

        this.partitionId = partitionId;

        if (leftSeparatorKey == null)
            throw new IllegalArgumentException("leftSeparatorKey");

        // Note: rightSeparatorKey MAY be null.

        if (rightSeparatorKey != null) {

            if (BytesUtil.compareBytes(leftSeparatorKey, rightSeparatorKey) >= 0) {

                throw new IllegalArgumentException(
                        "Separator keys are out of order: left="
                                + Arrays.toString(leftSeparatorKey)
                                + ", right="
                                + Arrays.toString(rightSeparatorKey));

            }

        }

        this.leftSeparatorKey = leftSeparatorKey;

        this.rightSeparatorKey = rightSeparatorKey;

        if (resources != null) {

            if (resources.length == 0)
                throw new IllegalArgumentException();

            /*
             * This is the "live" journal.
             * 
             * Note: The "live" journal is still available for writes. Index
             * segments created off of this journal will therefore have a
             * createTime that is greater than the firstCommitTime of this
             * journal while being LTE to the lastCommitTime of the journal and
             * strictly LT the commitTime of any other resource for this index
             * partition.
             */

            IResourceMetadata resource = resources[0];

            if (!(resource instanceof JournalMetadata)) {

                throw new RuntimeException(
                        "Expecting a journal as the first resource");

            }

            // scan from 1 to n-1 - these are historical resources
            // (non-writable).
            {

                long lastTimestamp = Long.MIN_VALUE;

                for (int i = 1; i < resources.length; i++) {

                    // createTime of the resource.
                    final long thisTimestamp = resources[i].getCreateTime();

                    if (thisTimestamp <= lastTimestamp) {

                        throw new RuntimeException(
                                "Resources are out of timestamp order @ index="
                                        + i + ", lastTimestamp="
                                        + lastTimestamp + ", this timestamp="
                                        + thisTimestamp);

                    }

                    lastTimestamp = resources[i].getCreateTime();

                }

            }
        }

        this.resources = resources;

    }

    final public int getPartitionId() {
        
        return partitionId;
        
    }
    
    final public byte[] getLeftSeparatorKey() {

        return leftSeparatorKey;
        
    }
    
    final public byte[] getRightSeparatorKey() {
        
        return rightSeparatorKey;
        
    }
    
    /**
     * Description of the resources required to materialize a view of the index
     * partition (optional, but required for a {@link BTree}).
     * <p>
     * The entries in the array reflect the creation time of the resources. The
     * earliest resource is listed first. The most recently created resource is
     * listed last. The order of the resources corresponds to the order in which
     * a fused view of the index partition will be read. Reads begin with the
     * most "recent" data for the index partition and stop as soon as there is a
     * "hit" on one of the resources (including a hit on a deleted index entry).
     * <p>
     * Note: the {@link IResourceMetadata}[] is only available when the
     * {@link LocalPartitionMetadata} is attached to the {@link IndexMetadata}
     * of a {@link BTree} and is NOT defined when the
     * {@link LocalPartitionMetadata} is attached to an {@link IndexSegment}.
     * The reason is that the index partition view is always described by the
     * {@link BTree} and that view evolves as journals overflow. On the other
     * hand, {@link IndexSegment}s are used as resources in index partition
     * views but exist in a one to many relationship to those views.
     */
    final public IResourceMetadata[] getResources() {
        
        return resources;
        
    }

    final public int hashCode() {

        // per the interface contract.
        return partitionId;
        
    }
    
    // Note: used by assertEquals in the test cases.
    public boolean equals(Object o) {

        if (this == o)
            return true;

        LocalPartitionMetadata o2 = (LocalPartitionMetadata) o;

        if (partitionId != o2.partitionId)
            return false;

        if( ! BytesUtil.bytesEqual(leftSeparatorKey,o2.leftSeparatorKey)) {
            
            return false;
            
        }
        
        if(rightSeparatorKey==null) {
            
            if(o2.rightSeparatorKey!=null) return false;
            
        } else {
            
            if(!BytesUtil.bytesEqual(rightSeparatorKey, o2.rightSeparatorKey)) {
                
                return false;
                
            }
            
        }
        
        if (resources.length != o2.resources.length)
            return false;

        for (int i = 0; i < resources.length; i++) {

            if (!resources[i].equals(o2.resources[i]))
                return false;

        }

        return true;

    }


    public String toString() {

        return 
        "{ partitionId="+getPartitionId()+
        ", leftSeparator="+Arrays.toString(getLeftSeparatorKey())+
        ", rightSeparator="+(getRightSeparatorKey()==null?"null":Arrays.toString(getRightSeparatorKey()))+
        ", resourceMetadata="+Arrays.toString(getResources())+
        "}"
        ;

    }
    
    private static final transient short VERSION0 = 0x0;
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        final short version = ShortPacker.unpackShort(in);
        
        if (version != VERSION0) {
            
            throw new IOException("Unknown version: "+version);
            
        }

        partitionId = (int) LongPacker.unpackLong(in);
        
        final int nresources = ShortPacker.unpackShort(in);
        
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

        resources = nresources>0 ? new IResourceMetadata[nresources] : null;

        for (int j = 0; j < nresources; j++) {

            boolean isIndexSegment = in.readBoolean();
            
            String filename = in.readUTF();

            long nbytes = LongPacker.unpackLong(in);

            UUID uuid = new UUID(in.readLong()/*MSB*/,in.readLong()/*LSB*/);

            long commitTime = in.readLong();
            
            resources[j] = (isIndexSegment //
                    ? new SegmentMetadata(filename, nbytes, uuid, commitTime) //
                    : new JournalMetadata(filename, nbytes, uuid, commitTime) //
                    );

        }
                
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        ShortPacker.packShort(out, VERSION0);

        LongPacker.packLong(out, partitionId);
        
        final int nresources = (resources == null ? 0 : resources.length);
        
        assert nresources < Short.MAX_VALUE;

        ShortPacker.packShort(out, (short) nresources);

        LongPacker.packLong(out, leftSeparatorKey.length);

        LongPacker.packLong(out, rightSeparatorKey == null ? 0
                : rightSeparatorKey.length);

        out.write(leftSeparatorKey);

        if (rightSeparatorKey != null) {

            out.write(rightSeparatorKey);

        }
        
        /*
         * Note: we serialize using the IResourceMetadata interface so that we
         * can handle different subclasses and then special case the
         * deserialization based on the boolean flag. This is significantly more
         * compact than using an Externalizable for each ResourceMetadata object
         * since we do not have to write the class names for those objects.
         */

        for (int j = 0; j < nresources; j++) {

            IResourceMetadata rmd = resources[j];

            out.writeBoolean(rmd.isIndexSegment());
            
            out.writeUTF(rmd.getFile());

            LongPacker.packLong(out,rmd.size());

            final UUID resourceUUID = rmd.getUUID();
            
            out.writeLong(resourceUUID.getMostSignificantBits());
            
            out.writeLong(resourceUUID.getLeastSignificantBits());
            
            out.writeLong(rmd.getCreateTime());

        }

    }
    
}
