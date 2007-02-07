package com.bigdata.scaleup;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.bigdata.objndx.BTree;
import com.bigdata.objndx.BTreeMetadata;
import com.bigdata.objndx.IValueSerializer;
import com.bigdata.objndx.IndexSegment;
import com.bigdata.rawstore.IRawStore;

/**
 * A metadata index for the partitions of a distributed index. There is one
 * metadata index for each distributed index. The keys of the metadata index
 * are the first key that would be directed into the corresponding index
 * segment, e.g., a <em>separator key</em> (this is just the standard
 * btree semantics). Keys in the metadata index are updated as index
 * partitions are split (or joined). Splits are performed when the fully
 * compacted partition exceeds ~200M. Joins of index segments are performed
 * when sibling partitions could be merged to produce a partition of ~100M.
 * Splitting and joining partitions is atomic and respects transaction
 * isolation.
 * 
 * @todo locator logic on a single host (a journal and zero or more index
 *       segments).
 * 
 * @todo split/join logic.
 * 
 * @todo split/join logic respecting transactional isolation.
 * 
 * @todo locator logic on a cluster (a socket address in addition to the
 *       other information).
 * 
 * @todo consider multiplexing metadata indices for different distributed
 *       indices. a good idea or not?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MetadataIndex extends BTree {

    public MetadataIndex(IRawStore store, int branchingFactor) {

        super(store, branchingFactor, PartitionMetadata.Serializer.INSTANCE);
        
    }

    public MetadataIndex(IRawStore store, long metadataId) {
        
        super(store, BTreeMetadata.read(store, metadataId));
    }

//    /**
//     * Append a new {@link IndexSegment} for a partition. If there is no
//     * partition for that key then a new partition will be created.
//     * 
//     * @param key
//     *            The lowest key that will enter this partition.
//     * 
//     * @param segmentMetadata
//     *            Select metadata for the {@link IndexSegment}.
//     * 
//     * @return The ordered array of metadata objects for the
//     *         {@link IndexSegment}s in the identified partition.
//     */
//    public SegmentMetadata[] addSegment(byte[] key,SegmentMetadata segmentMetadata) {
//        
//        PartitionMetadata part = (PartitionMetadata) super.lookup(key);
//        
//        if( part == null ) {
//            
//            part = new PartitionMetadata();
//            
//        }
//        
//        // clone to avoid modifying the value in the index.
//        part = (PartitionMetadata) part.clone();
//
//        // append the new segment to the list of segments.
//        part.segs.add(segmentMetadata);
//        
//        super.insert(key,part);
//        
//        return part.segs.toArray(new SegmentMetadata[]{});
//        
//    }
    
//    public SegmentMetadata[] removeSegment(byte[] key,String filename) {
//
//        PartitionMetadata part = (PartitionMetadata) super.lookup(key);
//        
//        if( part == null ) {
//            
//            throw new IllegalArgumentException("No such partition.");
//            
//        }
//        
//        if( part == null ) {
//
//            throw new IllegalArgumentException("Not part of this partition: filename="+filename);
//            
//        }
//        
//    }

    /**
     * Find and return the partition spanning the given key.
     * 
     * @return The partition spanning the given key or <code>null</code> if
     *         there are no partitions defined.
     */
    public PartitionMetadata find(byte[] key) {
        
        int pos = super.indexOf(key);
        
        if (pos < 0) {

            /*
             * the key lies between the partition separators and represents the
             * insert position.  we convert it to an index and subtract one to
             * get the index of the partition that spans this key.
             */
            
            pos = -(pos+1);

            if(pos == 0) {
                
                assert nentries == 0;
                
                return null;
                
            }
                
            pos--;

            return (PartitionMetadata) super.valueAt(pos);
            
        } else {
            
            /*
             * exact hit on a partition separator, so we choose the entry with
             * that key.
             */
            
            return (PartitionMetadata) super.valueAt(pos);
            
        }
        
    }
    
    /**
     * The partition with that separator key or <code>null</code> (exact
     * match on the separator key).
     * 
     * @param key
     *            The separator key (the first key that would go into that
     *            partition).
     * 
     * @return The partition with that separator key or <code>null</code>.
     */
    public PartitionMetadata get(byte[] key) {
        
        return (PartitionMetadata) super.lookup(key);
        
    }
    
    /**
     * Create or update a partition (exact match on the separator key).
     * 
     * @param key
     *            The separator key (the first key that would go into that
     *            partition).
     * @param val
     *            The parition metadata.
     * 
     * @return The previous partition metadata for that separator key or
     *         <code>null</code> if there was no partition for that separator
     *         key.
     * 
     * @exception IllegalArgumentException
     *                if the key identifies an existing partition but the
     *                partition identifers do not agree.
     */
    public PartitionMetadata put(byte[] key,PartitionMetadata val) {
        
        if (val == null) {

            throw new IllegalArgumentException();

        }
        
        PartitionMetadata oldval = (PartitionMetadata) super.insert(key,
                val);

        if (oldval != null && oldval.partId != val.partId) {

            throw new IllegalArgumentException("Expecting: partId="
                    + oldval.partId + ", but have partId=" + val.partId);

        }

        return oldval;
    
    }

    /**
     * Remove a partition (exact match on the separator key).
     * 
     * @param key
     *            The separator key (the first key that would go into that
     *            partition).
     * 
     * @return The existing partition for that separator key or
     *         <code>null</code> if there was no entry for that separator key.
     */
    public PartitionMetadata remove(byte[] key) {
        
        return (PartitionMetadata) super.remove(key);
        
    }
    
    /**
     * Update the metadata index to reflect the split of one index segment
     * into two index segments.
     * 
     * @param separatorKey
     *            Requests greater than or equal to the separatorKey (and
     *            less than the next largest separatorKey in the metadata
     *            index) are directed into seg2. Requests less than the
     *            separatorKey (and greated than any proceeding separatorKey
     *            in the metadata index) are directed into seg1.
     * @param seg1
     *            The metadata for the index segment that was split.
     * @param seg2
     *            The metadata for the right sibling of the split index
     *            segment in terms of the key range of the distributed
     *            index.
     */
    public void split(Object separatorKey, PartitionMetadata md1, PartitionMetadata md2) {
        
    }

    /**
     * @todo join of index segment with left or right sibling. unlike the
     *       nodes of a btree we merge nodes whenever a segment goes under
     *       capacity rather than trying to redistribute part of the key
     *       range from one index segment to another.
     */
    public void join() {
        
    }

    public static enum StateEnum {
     
        NEW("New",0),
        LIVE("Live",1),
        DEAD("Dead",2);
        
        final private String name;
        final private int id;
        
        StateEnum(String name,int id) {this.name = name;this.id = id;}
        
        public String toString() {return name;}
        
        public int valueOf() {return id;}
        
        static public StateEnum valueOf(int id) {
            switch(id) {
            case 0: return NEW;
            case 1: return LIVE;
            case 2: return DEAD;
            default: throw new IllegalArgumentException("Unknown: code="+id);
            }
        }
        
    }
    
    /**
     * Metadata for a single {@link IndexSegment}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class SegmentMetadata implements Cloneable {
        
        final public String filename;
        final public long nbytes;
        final public StateEnum state;
        
        public SegmentMetadata(String filename,long nbytes,StateEnum state) {

            this.filename = filename;
            
            this.nbytes = nbytes;
            
            this.state = state;
            
        }
     
        // Note: used by assertEquals in the test cases.
        public boolean equals(Object o) {
            
            if(this == o)return true;
            
            SegmentMetadata o2 = (SegmentMetadata)o;
            
            if(filename.equals(o2.filename) && nbytes==o2.nbytes && state == o2.state) return true;
            
            return false;
            
        }
        
    }
    
    /**
     * The location of the process that will handle requests for some key range
     * together with some metadata about that {process + key range}. Note that
     * many key ranges, potentially for different indices, are multiplexed onto
     * the same process. Each process is backed by a journal providing
     * transactional isolation. Periodically the journal is frozen, a new
     * journal is created, and {@link IndexSegment} files are created from the
     * journal. Those files are incorporated into the view of the index iff they
     * contain committed state. (Uncommitted state from a transaction that
     * bridges a journal boundary if permitted must be handled once the
     * transaction commits.)
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class PartitionMetadata {

        /**
         * The unique partition identifier.
         */
        final int partId;
        
        /**
         * Zero or more files containing {@link IndexSegment}s holding live
         * data for this partition. The entries in the array reflect the
         * creation time of the index segments. The earliest segment is listed
         * first. The most recently created segment is listed last.
         */
        final SegmentMetadata[] segs;
        
        public PartitionMetadata(int partId) {
         
            this.partId = partId;
            
            segs = new SegmentMetadata[]{};
            
        }

        public PartitionMetadata(int partId, SegmentMetadata[] segs) {
            
            this.partId = partId;
            
            this.segs = segs;
            
        }
        
        // Note: used by assertEquals in the test cases.
        public boolean equals(Object o) {
            
            if(this == o)return true;
            
            PartitionMetadata o2 = (PartitionMetadata)o;
            
            if( partId != o2.partId ) return false;
            
            if (segs.length != o2.segs.length)
                return false;
            
            for( int i=0; i<segs.length; i++) {
                
                if( ! segs[i].equals(o2.segs[i])) return false;
                
            }
            
            return true;
            
        }
        
        public String toString() {
            
            return ""+partId;
            
        }

//        public PartitionMetadata clone()  {
//            
//            try {
//
//                return (PartitionMetadata)super.clone();
//                
//            } catch(CloneNotSupportedException ex) {
//                
//                throw new RuntimeException(ex);
//                
//            }
//            
//            
//        }
        
//        /**
//         * The metadata about an index segment life cycle as served by a
//         * specific service instance on some host.
//         * 
//         * @todo we need to track load information for the service and the host.
//         *       however that information probably does not need to be restart
//         *       safe so it is easily maintained within a rather small hashmap
//         *       indexed by the service address.
//         * 
//         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//         *         Thompson</a>
//         * @version $Id$
//         */
//        public static class IndexSegmentServiceMetadata {
//
//            /**
//             * The service that is handling this index segment. This service
//             * typically handles many index segments and multiplexes them on a
//             * single journal.
//             * 
//             * @todo When a client looks up an index segment in the metadata index,
//             *       what we send them is the set of key-addr entries from the leaf
//             *       in which the index segment was found. If a request by the
//             *       client to that service discovers that the service no longer
//             *       handles a key range, that the service is dead, etc., then the
//             *       client will have to invalidate its cache entry and lookup the
//             *       current location of the index segment in the metadata index.
//             */
//            public InetSocketAddress addr;
//            
//        }
//
//        /**
//         * An array of the services that are registered as handling this index
//         * segment. One of these services is the master and accepts writes from
//         * the client. The other services mirror the segment and provide
//         * redundency for failover and load balancing. The order in which the
//         * segments are listed in this array could reflect the master (at
//         * position zero) and the write pipeline from the master to the
//         * secondaries could be simply the order of the entries in the array.
//         */
//        public IndexSegmentServiceMetadata services[];
//        
//        /**
//         * The time that the index segment was started on that service.
//         */
//        public long startTime;
//
//        /**
//         * A log of events for the index segment. This could just be a linked
//         * list of strings that get serialized as a single string. Each event is
//         * then a semi-structured string, typically generated by a purpose
//         * specific logging appender.
//         */
//        public Vector<Event> eventLog;
//        
//        public PartitionMetadata(InetSocketAddress addr) {
//        }
//
//        /**
//         * An event for an index segment.
//         * 
//         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//         * @version $Id$
//         */
//        public static class Event {
//        
////            public long timestamp;
//            
//            public String msg;
//            
//            /**
//             * Serialization for an event.
//             * 
//             * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//             * @version $Id$
//             */
//            public static class Serializer /*implements ...*/{
//                
//            }
//            
//        }
        
        /**
         * Serialization for an index segment metadata entry.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        public static class Serializer implements IValueSerializer {

            /**
             * 
             */
            private static final long serialVersionUID = 4307076612127034103L;
            
            public transient static final Serializer INSTANCE = new Serializer();
            
            public Serializer() {}
           
            public void putValues(DataOutputStream os, Object[] values, int nvals) throws IOException {
           
                for(int i=0; i<nvals; i++) {
                    
                    PartitionMetadata val = (PartitionMetadata) values[i];
                
                    final int nsegs = val.segs.length;
                    
                    os.writeInt(val.partId);
                    
                    os.writeInt(nsegs);
                    
                    for( int j=0; j<nsegs; j++) {
                        
                        SegmentMetadata segmentMetadata = val.segs[j];
                        
                        os.writeUTF(segmentMetadata.filename);
                        
                        os.writeLong(segmentMetadata.nbytes);
                        
                        os.writeInt(segmentMetadata.state.valueOf());
                        
                    }
                    
                }
                
                
            }

            public void getValues(DataInputStream is, Object[] values, int nvals) throws IOException {

                for(int i=0; i<nvals; i++) {

                    final int partId = is.readInt();

                    final int nsegs = is.readInt();

                    PartitionMetadata val = new PartitionMetadata(partId,
                            new SegmentMetadata[nsegs]);
                    
                    for(int j=0; j<nsegs; j++) {
                        
                        String filename = is.readUTF();
                        
                        long nbytes = is.readLong();
                        
                        StateEnum state = StateEnum.valueOf(is.readInt());
                        
                        val.segs[j] = new SegmentMetadata(filename,nbytes,state);
                        
                    }
                    
                    values[i] = val;
                    
                }
                
            }
            
        }
        
    }

}
