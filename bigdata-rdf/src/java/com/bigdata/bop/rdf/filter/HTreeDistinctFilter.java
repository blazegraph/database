package com.bigdata.bop.rdf.filter;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import com.bigdata.bop.BOp;
import com.bigdata.bop.HTreeAnnotations;
import com.bigdata.bop.ap.filter.BOpFilterBase;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.htree.HTree;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rwstore.sector.MemStore;
import com.bigdata.rwstore.sector.MemoryManager;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Filterator;

/**
 * A scalable DISTINCT operator based for {@link IV} based on an {@link HTree}.
 * 
 * FIXME While highly scalable, this class will absorb a miminum of one direct
 * buffer per use. This is because we do not have access to the memory manager
 * of the {@link IRunningQuery} on which the distinct filter is being run. For
 * this reason, it is allocating a private {@link MemStore} and using a finalizer
 * pattern to ensure the eventual release of that {@link MemStore} and the backing
 * direct buffers. 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DistinctElementFilter.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 */
public class HTreeDistinctFilter extends BOpFilterBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends
            com.bigdata.bop.ap.filter.BOpFilter.Annotations, HTreeAnnotations {

        /**
         * The default maximum length of an inlined {@link IV} before it is
         * coverted into a raw record reference.  Encoded {@link IV}s tend to
         * be relatively small so we are generally better off inlining them
         * into the bucket page of the {@link HTree}.
         */
        int DEFAULT_MAX_RECLEN = 32;
        
    }

    /**
     * A instance using the default configuration for the in memory hash map.
     */
    public static HTreeDistinctFilter newInstance() {
        return new HTreeDistinctFilter(BOp.NOARGS, BOp.NOANNS);
    }
    
    /**
     * Required deep copy constructor.
     */
    public HTreeDistinctFilter(final HTreeDistinctFilter op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public HTreeDistinctFilter(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

    }

    @Override
    final protected Iterator filterOnce(Iterator src, final Object context) {
        
        return new Filterator(src, context, new DistinctFilterImpl());
        
    }

    /**
     * @see Annotations#ADDRESS_BITS
     */
    public int getAddressBits() {

        return getProperty(Annotations.ADDRESS_BITS,
                Annotations.DEFAULT_ADDRESS_BITS);

    }

    /**
     * @see Annotations#RAW_RECORDS
     */
    public boolean getRawRecords() {

        return getProperty(Annotations.RAW_RECORDS,
                Annotations.DEFAULT_RAW_RECORDS);

    }
    
    /**
     * @see Annotations#MAX_RECLEN
     */
    public int getMaxRecLen() {

        return getProperty(Annotations.MAX_RECLEN,
                Annotations.DEFAULT_MAX_RECLEN);

    }

    private class DistinctFilterImpl extends Filter {

        private static final long serialVersionUID = 1L;

        private final MemStore store;
        private final IKeyBuilder keyBuilder;
        private final HTree members;

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            members.close();
            store.close();
        }
        
        public DistinctFilterImpl() {
            
            /*
             * Create the map(s).
             */
            
            final IndexMetadata metadata = new IndexMetadata(
                    UUID.randomUUID());

            metadata.setAddressBits(getAddressBits());

            metadata.setRawRecords(getRawRecords());

            metadata.setMaxRecLen(getMaxRecLen());

            metadata.setKeyLen(Bytes.SIZEOF_INT); // int32 hash code keys.

            /*
             * TODO This sets up a tuple serializer for a presumed case of 4
             * byte keys (the buffer will be resized if necessary) and
             * explicitly chooses the SimpleRabaCoder as a workaround since the
             * keys IRaba for the HTree does not report true for isKeys(). Once
             * we work through an optimized bucket page design we can revisit
             * this as the FrontCodedRabaCoder should be a good choice, but it
             * currently requires isKeys() to return true.
             */
            final ITupleSerializer<?, ?> tupleSer = new DefaultTupleSerializer(
                    new ASCIIKeyBuilderFactory(Bytes.SIZEOF_INT),
                    new SimpleRabaCoder(),// keys : TODO Optimize for int32!
                    new SimpleRabaCoder() // vals
            );

            metadata.setTupleSerializer(tupleSer);

            /*
             * This wraps an efficient raw store interface around a child memory
             * manager created from the IMemoryManager which is backing the
             * query.
             */
            store = new MemStore(new MemoryManager(DirectBufferPool.INSTANCE));

            // Will support incremental eviction and persistence.
            members = HTree.create(store, metadata);
            
            // Small initial capacity since it is just used for IVs.
            keyBuilder = new KeyBuilder(16);

        }

        @SuppressWarnings("rawtypes")
        @Override
        public boolean isValid(final Object obj) {

            final IV iv = (IV) obj;

            final byte[] key = keyBuilder.reset().append(iv.hashCode())
                    .getKey();

            final byte[] val = iv.encode(keyBuilder.reset()).getKey();

            final ITupleIterator<?> itr = members.lookupAll(key);

            while (itr.hasNext()) {
            
                final ITuple t = itr.next();
                
                final IV actual = IVUtility.decodeFromOffset(t.getValueBuffer()
                        .array(), 0);
                
                if (iv.equals(actual)) {
                    
                    // Already in the map.
                    return false;
                    
                }
                
            }

            // Add to the map.
            
            members.insert(key, val);
            
            return true;
            
        }

    }

}
