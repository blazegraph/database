package com.bigdata.service;

import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.btree.proc.RangeCountProcedure;
import com.bigdata.journal.ITx;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;

/**
 * An implementation that performs NO caching. All methods read through to the
 * remote metadata index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NoCacheMetadataIndexView implements IMetadataIndex {

    final private AbstractScaleOutFederation fed;

    final private String name;

    final private long timestamp;

    final private MetadataIndexMetadata mdmd;

    public MetadataIndexMetadata getIndexMetadata() {

        return mdmd;
        
    }

    protected IMetadataService getMetadataService() {

        return fed.getMetadataService();

    }

    /**
     * 
     * @param name
     *            The name of the scale-out index.
     * @param timestamp
     */
    public NoCacheMetadataIndexView(AbstractScaleOutFederation fed,
            String name, long timestamp, MetadataIndexMetadata mdmd) {

        if (fed == null)
            throw new IllegalArgumentException();
        if (name == null)
            throw new IllegalArgumentException();
        if (mdmd == null)
            throw new IllegalArgumentException();

        this.fed = fed;
        
        this.name = name;

        this.timestamp = timestamp;

        this.mdmd = mdmd;
        
    }
    
    // @todo re-fetch if READ_COMMITTED or UNISOLATED? it's very unlikely to change.
    public IndexMetadata getScaleOutIndexMetadata() {

        return mdmd;
        
    }

    // this could be cached easily, but its only used by the unit tests.
    public PartitionLocator get(byte[] key) {

        try {

            return getMetadataService().get(name, timestamp, key);
            
        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }
        
    }

    // harder to cache - must look for "gaps"
    public PartitionLocator find(byte[] key) {

        try {

            return getMetadataService().find(name, timestamp, key);
            
        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }
        
    }

    public long rangeCount() {

        return rangeCount(null, null);
        
    }

    // only used by unit tests.
    public long rangeCount(byte[] fromKey, byte[] toKey) {

        final IIndexProcedure proc = new RangeCountProcedure(
                false/* exact */, fromKey, toKey);

        final Long rangeCount;
        try {

            rangeCount = (Long) getMetadataService().submit(timestamp,
                    MetadataService.getMetadataIndexName(name), proc);

        } catch (Exception e) {

            throw new RuntimeException(e);

        }

        return rangeCount.longValue();

    }

    public long rangeCountExact(byte[] fromKey, byte[] toKey) {

        final IIndexProcedure proc = new RangeCountProcedure(
                true/* exact */, fromKey, toKey);

        final Long rangeCount;
        try {

            rangeCount = (Long) getMetadataService().submit(timestamp,
                    MetadataService.getMetadataIndexName(name), proc);

        } catch (Exception e) {

            throw new RuntimeException(e);

        }

        return rangeCount.longValue();
        
    }
    
    public ITupleIterator rangeIterator() {
        
        return rangeIterator(null,null);
        
    }
    
    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey) {

        return rangeIterator(fromKey, toKey, 0/* capacity */,
                IRangeQuery.DEFAULT, null/* filter */);

    }

    /**
     * Note: Since this view is read-only this method forces the use of
     * {@link ITx#READ_COMMITTED} IFF the timestamp for the view is
     * {@link ITx#UNISOLATED}. This produces the same results on read and
     * reduces contention for the {@link WriteExecutorService}. This is
     * already done automatically for anything that gets run as an index
     * procedure, so we only have to do this explicitly for the range
     * iterator method.
     */
    // not so interesting to cache, but could cache the iterator results on the scale-out index.
    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags, IFilterConstructor filter) {

        return new RawDataServiceTupleIterator(getMetadataService(),//
                MetadataService.getMetadataIndexName(name), //
                (timestamp==ITx.UNISOLATED?ITx.READ_COMMITTED:timestamp),//
                true, // read-consistent semantics.
                fromKey,//
                toKey,//
                capacity,//
                flags, //
                filter
        );

    }

}
