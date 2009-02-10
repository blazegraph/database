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
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;
import com.bigdata.resources.MoveIndexPartitionTask;
import com.bigdata.service.DataService;
import com.bigdata.service.Event;

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
     * The maximum length of the history string (4kb).
     * <p>
     * Note: The history is written each time the {@link IndexMetadata} is
     * written and is read each time it is read so this can be the main driver
     * of the size of the {@link IndexMetadata} record.
     */
    protected final static transient int MAX_HISTORY_LENGTH = 4 * Bytes.kilobyte32;
    
    /**
     * The unique partition identifier.
     */
    private int partitionId;

    /**
     * 
     * @see #getSourcePartitionId()
     */
    private int sourcePartitionId;
    
    /**
     * 
     */
    private byte[] leftSeparatorKey;
    private byte[] rightSeparatorKey;
    
    /**
     * Description of the resources required to materialize a view of the index
     * partition (optional - not stored when the partition metadata is stored on
     * an {@link IndexSegmentStore}).
     * <p>
     * The entries in the array reflect the creation time of the resources. The
     * earliest resource is listed first. The most recently created resource is
     * listed last.
     * <p>
     * When present, the #of sources in the index partition view includes: the
     * mutable {@link BTree}, any {@link BTree}s on historical journal(s)
     * still incorporated into the view, and any {@link IndexSegment}s
     * incorporated into the view.
     */
    private IResourceMetadata[] resources;

    /**
     * A history of operations giving rise to the current partition metadata.
     * E.g., register(timestamp), copyOnOverflow(timestamp), split(timestamp),
     * join(partitionId,partitionId,timestamp), etc. This is truncated when
     * serialized to keep it from growing without bound.
     */
    private String history;
    
    /**
     * If the history string exceeds {@link #MAX_HISTORY_LENGTH} characters then
     * truncates it to the last {@link #MAX_HISTORY_LENGTH}-3 characters,
     * prepends "...", and returns the result. Otherwise returns the entire
     * history string.
     */
    protected String getTruncatedHistory() {
        
        String history = this.history;
        
        if(history.length() > MAX_HISTORY_LENGTH) {

            /*
             * Truncate the history.
             */

            final int len = history.length();
            
            final int fromIndex = len - (MAX_HISTORY_LENGTH - 3);

            assert fromIndex > 0 : "len=" + len + ", fromIndex=" + fromIndex
                    + ", maxHistoryLength=" + MAX_HISTORY_LENGTH;
            
            history = "..." + history.substring(fromIndex, len);
            
        }

        return history;
        
    }
    
    /**
     * De-serialization constructor.
     */
    public LocalPartitionMetadata() {
        
    }
    
//    public LocalPartitionMetadata(//
//            final int partitionId,//
//            final byte[] leftSeparatorKey,//
//            final byte[] rightSeparatorKey,// 
//            final IResourceMetadata[] resources,//
//            final String history
//            ) {
//        
//        this(partitionId, -1/* sourcePartitionId */, leftSeparatorKey,
//                rightSeparatorKey, resources, history);
//        
//    }
    
    /**
     * 
     * @param partitionId
     *            The unique partition identifier assigned by the
     *            {@link MetadataIndex}.
     * @param sourcePartitionId
     *            <code>-1</code> unless this index partition is the target
     *            for a move, in which case this is the partition identifier of
     *            the source index partition.
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
     *            the {@link IndexMetadata} of an {@link IndexSegment}. When
     *            the {@link IndexMetadata} is sent to a remote
     *            {@link DataService} this field MUST be <code>null</code> and
     *            the remote {@link DataService} will fill it in on arrival.
     * @param history
     *            A human interpretable history of the index partition. The
     *            history is a series of whitespace delimited records each of
     *            more or less the form <code>foo(x,y,z)</code>. The history
     *            gets truncated when the {@link LocalPartitionMetadata} is
     *            serialized in order to prevent it from growing without bound.
     */
    public LocalPartitionMetadata(//
            final int partitionId,//
            final int sourcePartitionId,//
            final byte[] leftSeparatorKey,//
            final byte[] rightSeparatorKey,// 
            final IResourceMetadata[] resources,//
            final String history
            ) {

        /*
         * Set fields first so that toString() can be used in thrown exceptions.
         */
        
        this.partitionId = partitionId;

        this.sourcePartitionId = sourcePartitionId;
        
        this.leftSeparatorKey = leftSeparatorKey;

        this.rightSeparatorKey = rightSeparatorKey;

        this.resources = resources;

        this.history = history;
        
        /*
         * Test arguments.
         */
        
        if (leftSeparatorKey == null)
            throw new IllegalArgumentException("leftSeparatorKey");

        // Note: rightSeparatorKey MAY be null.

        if (rightSeparatorKey != null) {

            if (BytesUtil.compareBytes(leftSeparatorKey, rightSeparatorKey) >= 0) {

                throw new IllegalArgumentException(
                        "Separator keys are out of order: " + this);

            }

        }

        if (resources != null) {

            if (resources.length == 0) {
                
                throw new IllegalArgumentException("Empty resources array.");
                
            }

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

            if (!(resources[0] instanceof JournalMetadata)) {

                throw new RuntimeException(
                        "Expecting a journal as the first resource: " + this);

            }

            /*
             * Scan from 1 to n-1 - these are historical resources
             * (non-writable). resources[0] is always the live journal. The
             * other resources may be either historical journals or index
             * segments.
             * 
             * The order of the array is the order of the view. Reads on a
             * FusedView will process the resources in the order in which they
             * are specified by this array.
             * 
             * The live journal gets listed first since it can continue to
             * receive writes and therefore logically comes before any other
             * resource in the ordering since any writes on the live index on
             * the journal will be more recent than the data on the index
             * segment.
             * 
             * Normally, each successive entry in the resources[] will have an
             * earlier createTime (smaller number) than the one that follows it.
             * However, there is one exception. The createTime of the live
             * journal MAY be less than the createTime of index segments created
             * from that journal - this will be true if those indices are
             * created from a historical view found on that journal and put into
             * play while the journal is still the live journal. To work around
             * this we start at the 2nd entry in the array.
             */
            if (resources.length > 2) {

                long lastTimestamp = resources[1/*2ndEntry*/].getCreateTime();

                for (int i = 2/* 3rd entry */; i < resources.length; i++) {

                    // createTime of the resource.
                    final long thisTimestamp = resources[i].getCreateTime();

                    if (lastTimestamp <= thisTimestamp) {

                        throw new RuntimeException(
                                "Resources out of timestamp order @ index="
                                        + i + " : "+ this);

                    }

                    lastTimestamp = resources[i].getCreateTime();

                }

            }
        }

    }

    final public int getPartitionId() {
        
        return partitionId;
        
    }

    /**
     * <code>-1</code> unless this index partition is the target for a move,
     * in which case this is the partition identifier of the source index
     * partition and the move operation has not been completed. This property is
     * used to prevent the target data service from de-defining the index
     * partition using a split, join or move operation while the MOVE operation
     * is proceeding. The property is cleared to <code>-1</code> (which is an
     * invalid index partition identifier) once the move has been completed
     * successfully.
     * 
     * @see MoveIndexPartitionTask
     */
    final public int getSourcePartitionId() {
        
        return sourcePartitionId;
        
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
     * When present, the #of sources in the index partition view includes: the
     * mutable {@link BTree}, any {@link BTree}s on historical journal(s)
     * still incorporated into the view, and any {@link IndexSegment}s
     * incorporated into the view.
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

    /**
     * 
     * @return
     * 
     * @todo I am not convince that the history is worth keeping. It definately
     *       swell the size of the {@link IndexMetadata} record and is MUST less
     *       useful than an analysis of the {@link Event} log.
     */
    final public String getHistory() {
        
        return history;
        
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

        if (!BytesUtil.bytesEqual(leftSeparatorKey, o2.leftSeparatorKey)) {

            return false;

        }

        if (rightSeparatorKey == null) {

            if (o2.rightSeparatorKey != null)
                return false;

        } else {

            if (!BytesUtil.bytesEqual(rightSeparatorKey, o2.rightSeparatorKey)) {

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
        "{ partitionId="+partitionId+
        (sourcePartitionId!=-1?", sourcePartitionId="+sourcePartitionId:"")+
        ", leftSeparator="+BytesUtil.toString(leftSeparatorKey)+
        ", rightSeparator="+BytesUtil.toString(rightSeparatorKey)+
        ", resourceMetadata="+Arrays.toString(resources)+
        ", history="+history+
        "}"
        ;

    }
    
    /*
     * Externalizable
     */
    
    private static final transient short VERSION0 = 0x0;
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        final short version = ShortPacker.unpackShort(in);
        
        if (version != VERSION0) {
            
            throw new IOException("Unknown version: "+version);
            
        }

        partitionId = (int) LongPacker.unpackLong(in);

        sourcePartitionId = in.readInt(); // MAY be -1.
        
        final int nresources = ShortPacker.unpackShort(in);
        
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

        history = in.readUTF();
        
        resources = nresources>0 ? new IResourceMetadata[nresources] : null;

        for (int j = 0; j < nresources; j++) {

            final boolean isIndexSegment = in.readBoolean();
            
            final String filename = in.readUTF();

//            long nbytes = LongPacker.unpackLong(in);

            final UUID uuid = new UUID(in.readLong()/*MSB*/,in.readLong()/*LSB*/);

            final long commitTime = in.readLong();
            
            resources[j] = (isIndexSegment //
                    ? new SegmentMetadata(filename, /*nbytes,*/ uuid, commitTime) //
                    : new JournalMetadata(filename, /*nbytes,*/ uuid, commitTime) //
                    );

        }
                
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        ShortPacker.packShort(out, VERSION0);

        LongPacker.packLong(out, partitionId);
        
        out.writeInt(sourcePartitionId); // MAY be -1.
        
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

        out.writeUTF(getTruncatedHistory());
                
        /*
         * Note: we serialize using the IResourceMetadata interface so that we
         * can handle different subclasses and then special case the
         * deserialization based on the boolean flag. This is significantly more
         * compact than using an Externalizable for each ResourceMetadata object
         * since we do not have to write the class names for those objects.
         */

        for (int j = 0; j < nresources; j++) {

            final IResourceMetadata rmd = resources[j];

            out.writeBoolean(rmd.isIndexSegment());
            
            out.writeUTF(rmd.getFile());

//            LongPacker.packLong(out,rmd.size());

            final UUID resourceUUID = rmd.getUUID();
            
            out.writeLong(resourceUUID.getMostSignificantBits());
            
            out.writeLong(resourceUUID.getLeastSignificantBits());
            
            out.writeLong(rmd.getCreateTime());

        }

    }
    
}
