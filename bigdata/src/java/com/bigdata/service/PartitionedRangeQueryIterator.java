package com.bigdata.service;

import java.util.NoSuchElementException;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.ITuple;
import com.bigdata.io.ByteArrayBufferWithPosition;
import com.bigdata.io.IByteArrayBuffer;
import com.bigdata.scaleup.MetadataIndex;

/**
 * Class supports range query across one or more index partitions.
 * 
 * FIXME support an optional key and/or value filter that is executed on the
 * data service.
 * 
 * @todo refactor to use {@link RangeQueryIterator} in order to simplify the
 *       logic in this class to crossing partitions when the iterator for a
 *       given partition has been exhausted.
 * 
 * @todo if unisolated or read-committed, then we may need to re-assess the
 *       toIndex during the query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PartitionedRangeQueryIterator implements IEntryIterator {

    /**
     * Error message used by {@link #getKey()} when the iterator was not
     * provisioned to request keys from the data service.
     */
    static public transient final String ERR_NO_KEYS = "Keys not requested";
    
    /**
     * Error message used by {@link #getValue()} when the iterator was not
     * provisioned to request values from the data service.
     */
    static public transient final String ERR_NO_VALS = "Values not requested";

    /**
     * The index on which the range query is being performed.
     */
    private final ClientIndexView ndx;
    
    /**
     * The transaction identifier -or- zero iff the request is unisolated.
     */
    private final long tx;

    /**
     * The first key to visit -or- null iff no lower bound.
     */
    private final byte[] fromKey;
    
    /**
     * The first key to NOT visit -or- null iff no upper bound.
     */
    private final byte[] toKey;

    /**
     * This controls the #of results per data service query.
     */
    private final int capacity;

    /**
     * These flags control whether keys and/or values are requested. If
     * neither keys nor values are requested, then this is just a range
     * count operation and you might as well use rangeCount instead.
     */
    private final int flags;
    
    /**
     * Index of the first partition to be queried.
     */
    private final int fromIndex;
    
    /**
     * Index of the last partition to be queried.
     */
    private final int toIndex;

    /**
     * Index of the partition that is currently being queried.
     */
    private int index;

    /**
     * The #of range query operations executed.
     */
    private int nqueries;
    
    /**
     * The #of partitions visited.
     */
    private int nparts;
    
    /**
     * The metadata for the current index partition.
     */
    PartitionMetadataWithSeparatorKeys pmd = null;

    /**
     * The data service for the current index partition (this should
     * failover).
     */
    IDataService dataService = null;

    /**
     * The current result set. For each index partition spanned by the
     * overall key range supplied by the client, we will issue at least one
     * range query against the data service for that index partition. Once
     * all entries in a result set have been consumed by the client, we test
     * the result set to see whether or not it exhausted the entries that
     * could be matched for that index partition. If not, then we will issue
     * another "continuation" range query against the same index position
     * starting from the successor of the last key scanned. If it is
     * exhausted, then we will issue a new range query against the next
     * index partition. If no more index partitions remain that are spanned
     * by the key range specified by the client then we are done.
     * <p>
     * Note: A result set will be empty if there are no entries (including
     * after filtering) that lie within the key range in a given index
     * partition. It is possible for any of the result sets to be empty.
     * Consider a case of static partitioning of an index into N partitions.
     * When the index is empty, a range query of the entire index will still
     * query each of the N partitions. However, since the index is empty
     * none of the partitions will have any matching entries and all result
     * sets will be empty.
     */
    private ResultSet rset = null;

    /**
     * The #of enties visited so far.
     */
    private long nvisited = 0;
    
    /**
     * The index of the last entry visited in the current {@link ResultSet}.
     * This is reset to <code>-1</code> each time we obtain a new
     * {@link ResultSet}.
     */
    private int lastVisited = -1;
    
    /**
     * The last key visited by the iterator.
     */
    private byte[] lastKey = null;

    /**
     * The last value visited by the iterator.
     */
    private byte[] lastVal = null;
    
    /**
     * When true, the entire key range specified by the client has been
     * visited and the iterator is exhausted (i.e., all done).
     */
    private boolean exhausted = false;

    /**
     * The #of index partitions queried so far.
     */
    public int getPartitionCount() {
        
        return nparts;
        
    }
    
    /**
     * The #of queries issued so far.
     */
    public int getQueryCount() {

        return nqueries;
        
    }
    
    /**
     * The #of entries visited so far (not the #of entries scanned, which
     * can be much greater if a filter is in use).
     */
    public long getVisitedCount() {
        
        return nvisited;
        
    }
    
    public PartitionedRangeQueryIterator(ClientIndexView ndx, long tx,
            byte[] fromKey, byte[] toKey, int capacity, int flags) {

        if (ndx == null) {

            throw new IllegalArgumentException();
            
        }
        
        if (capacity <= 0) {

            throw new IllegalArgumentException("capacity must be positive.");
            
        }

        this.ndx = ndx;
        this.tx = tx;
        this.fromKey = fromKey;
        this.toKey = toKey;
        this.capacity = capacity;
        this.flags = flags;

//          IMetadataService metadataService = getMetadataService();
        MetadataIndex mdi = ndx.getMetadataIndex();

        final int fromIndex;
        final int toIndex;
//            try {

            // index of the first partition to check.
            fromIndex = (fromKey == null ? 0 : mdi.findIndexOf(fromKey));

            // index of the last partition to check.
            toIndex = (toKey == null ? mdi.getEntryCount()-1 : mdi.findIndexOf(toKey));
            
//            } catch (IOException ex) {
//                
//                throw new RuntimeException(ex);
//                
//            }

        // keys are out of order.
        if (fromIndex > toIndex ) {
        
            throw new IllegalArgumentException("fromKey > toKey");
            
        }

        this.fromIndex = fromIndex;
        this.toIndex = toIndex;

        // starting index is the lower bound.
        index = fromIndex;

        // Obtain the first result set.
        rangeQuery();
        
        nparts = 1;
        
    }

    /**
     * Issues a new range query against the current partition (the one
     * identified by {@link #index}. 
     */
    private void rangeQuery() {

        assert ! exhausted;

        pmd = ndx.getPartitionAtIndex(ndx.getName(), index);

        dataService = ndx.getDataService(pmd);
        
        try {

            /*
             * Note: The range query request is formed such that it
             * addresses only those keys that actually lie within the
             * partition. This has two benefits:
             * 
             * (1) The data service can check the range and notify clients
             * that appear to be requesting data for index partitions that
             * have been relocated.
             * 
             * (2) In order to avoid double-counting when multiple
             * partitions for the same index are mapped onto the same data
             * service we MUST query at most the key range for a specific
             * partition (or we must provide the data service with the index
             * partition identifier and it must restrict the range on our
             * behalf).
             */

            /*
             * For the first (last) partition use the caller's fromKey
             * (toKey) so that we do not count everything from the start of
             * (up to the close) of the partition unless the caller
             * specified fromKey := null (toKey := null).
             */

            final byte[] _fromKey = (index == fromIndex ? fromKey : pmd
                    .getLeftSeparatorKey());

            final byte[] _toKey = (index == toIndex ? toKey : pmd
                    .getRightSeparatorKey());

            final int partitionId = pmd.getPartitionId();
            
            ClientIndexView.log.info("name=" + ndx.getName() + ", partition=" + partitionId
                    + ", fromKey=" + BytesUtil.toString(_fromKey)
                    + ", toKey=" + BytesUtil.toString(_toKey));
            
            // the name of the index partition.
            final String name = DataService.getIndexPartitionName(ndx.getName(), partitionId);
            
            rset = dataService.rangeQuery(tx, name, _fromKey, _toKey, capacity,
                    flags);
            
            // reset index into the ResultSet.
            lastVisited = -1;
            
            nqueries++;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }
    
    /**
     * Issues a "continuation" query within the same index partition. This
     * is invoked iff the there are no entries left to visit in the current
     * {@link ResultSet} but {@link ResultSet#isExhausted()} is [false],
     * indicating that there is more data in that index partition.
     */
    private void continuationQuery() {
        
        assert ! exhausted;
        assert rset != null;
        assert ! rset.isExhausted();
        assert pmd != null;
        assert dataService != null;
        
        try {

            /*
             * Note: The range query request is formed such that it
             * addresses only those keys that actually lie within the
             * partition. This has two benefits:
             * 
             * (1) The data service can check the range and notify clients
             * that appear to be requesting data for index partitions that
             * have been relocated.
             * 
             * (2) In order to avoid double-counting when multiple
             * partitions for the same index are mapped onto the same data
             * service we MUST query at most the key range for a specific
             * partition (or we must provide the data service with the index
             * partition identifier and it must restrict the range on our
             * behalf).
             */

            /*
             * Start from the successor of the last key scanned by the
             * previous result set.
             */

            final byte[] _fromKey = rset.successor();

            /*
             * The toKey is choosen in the same manner for each query issued
             * against a given index partition.
             */
            final byte[] _toKey = (index == toIndex ? toKey : pmd
                    .getRightSeparatorKey());

            final int partitionId = pmd.getPartitionId();
            
            ClientIndexView.log.info("name=" + ndx.getName() + ", partition=" + partitionId
                    + ", fromKey=" + BytesUtil.toString(_fromKey)
                    + ", toKey=" + BytesUtil.toString(_toKey));
            
            // the name of the index partition.
            final String name = DataService.getIndexPartitionName(
                    ndx.getName(), partitionId);
            
            rset = dataService.rangeQuery(tx, name, _fromKey, _toKey, capacity,
                    flags);
            
            // reset index into the ResultSet.
            lastVisited = -1;

            nqueries++;
            
        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }
    
    /**
     * Issues a new range query against the next index partititon.
     */
    private void nextPartition() {

        assert ! exhausted;
        assert rset != null;
        
        index++;

        rangeQuery();
        
        nparts++;

    }
    
    /**
     * There are three levels at which we need to test in order to determine
     * if the total iterator is exhausted. First, we need to test to see if
     * there are more entries remaining in the current {@link ResultSet}.
     * If not and the {@link ResultSet} is NOT
     * {@link ResultSet#isExhausted() exhausted}, then we issue a
     * {@link #continuationQuery()} against the same index partition. If the
     * {@link ResultSet} is {@link ResultSet#isExhausted() exhausted}, then
     * we test to see whether or not we have visited all index partitions.
     * If so, then the iterator is exhausted. Otherwise we issue a range
     * query against the {@link #nextPartition()}.
     * 
     * @return True iff the iterator is not exhausted.
     */
    public boolean hasNext() {
        
        assert rset != null;
        
        if (exhausted) {

            return false;
            
        }
        
        final int ntuples = rset.getNumTuples();
        
        if (ntuples > 0 && lastVisited + 1 < ntuples) {
            /*
             * There is more data in the current ResultSet.
             */
            return true;
        }
        
        if(!rset.isExhausted()) {
            /*
             * This result set is empty but there is more data in the
             * current partition.
             */
            continuationQuery();
            /*
             * Recursive query since the result set might be empty.
             * 
             * Note: The result set could be empty if we are: (a)
             * unisolated; or (b) in a read committed transaction; or (c) if
             * a filter is being applied.
             */
            return hasNext();
        }
        if(index < toIndex) {
            /*
             * The current index partition is empty, but there are other
             * index partitions left to query.
             */
            nextPartition();
            /*
             * Recursive query since the index partition might be empty.
             */
            return hasNext();
        }
        /*
         * Exausted.
         */
        exhausted = true;
        return false;
    }

    /**
     * The byte[] containing the serialized value for the next matching
     * entry in the index -or- <code>null</code> iff the iterator was
     * provisioned to NOT visit values.
     */
    public Object next() {

        if (!hasNext()) {

            throw new NoSuchElementException();
            
        }
        
        nvisited++;

        lastVisited++;
        
        lastKey = ((flags & IDataService.KEYS) == 0) ? null : rset
                .getKeys()[lastVisited];

        lastVal = ((flags & IDataService.VALS) == 0) ? null : rset
                .getValues()[lastVisited];
        
        return lastVal;
        
    }

    /**
     * The byte[] containing the key for the last matching entry in the
     * index.
     * 
     * @exception IllegalStateException
     *                if nothing has been visited.
     * 
     * @exception UnsupportedOperationException
     *                if the iterator was not provisioned to visit keys.
     */
    public byte[] getKey() {
        
        if(nvisited==0L) {
            
            // Nothing visited yet.
            throw new IllegalStateException();
            
        }
        
        if((flags & IDataService.KEYS)==0) {

            // Keys not requested.
            throw new UnsupportedOperationException(ERR_NO_KEYS);
            
        }
        
        return lastKey;
        
    }

    public Object getValue() {

        if(nvisited==0L) {
           
            // Nothing visited yet.
            throw new IllegalStateException();
            
        }
        
        if((flags & IDataService.VALS)==0) {
            
            // Values not requested.
            throw new UnsupportedOperationException(ERR_NO_VALS);
            
        }

        return lastVal;
        
    }

    /**
     * This operation is not supported.
     * 
     * @exception UnsupportedOperationException
     *                Always.
     */
    public void remove() {
        
        throw new UnsupportedOperationException();
        
    }

    public ITuple getTuple() {
        
        return tuple;
        
    }

    private final Tuple tuple = new Tuple();

    /**
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class Tuple implements ITuple {

        // begin with an empty buffer.
        private ByteArrayBufferWithPosition buf = new ByteArrayBufferWithPosition(0);
        
        public byte[] getKey() {

            return PartitionedRangeQueryIterator.this.getKey();
            
        }

        public IByteArrayBuffer getKeyBuffer() {

            /*
             * @todo if the keys are compressed in the ResultSet then the
             * ResultSet should copy them into [buf] to avoid an allocation.
             */
            final byte[] key = PartitionedRangeQueryIterator.this.getKey();
            
            // copy the key into the buffer.
            buf.reset().put(key);
            
            return buf;
            
        }

        public boolean getKeysRequested() {

            return (flags & IDataService.KEYS) == 1;
            
        }

        public boolean getValuesRequested() {
            
            return (flags & IDataService.VALS) == 1;
            
        }

        public long getVisitCount() {

            return nvisited;
            
        }
        
    }
    
}
