package com.bigdata.bop.rdf.filter;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BTreeAnnotations;
import com.bigdata.bop.HashMapAnnotations;
import com.bigdata.bop.ap.filter.BOpFilterBase;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BloomFilterFactory;
import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.raba.codec.EmptyRabaValueCoder;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoder;
import com.bigdata.htree.HTree;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rwstore.sector.MemStore;
import com.bigdata.rwstore.sector.MemoryManager;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Filterator;

/**
 * A scalable DISTINCT operator based for {@link SPO}s.
 * <p>
 * Note: While highly scalable, this class will absorb a miminum of one direct
 * buffer per use. This is because we do not have access to the memory manager
 * of the {@link IRunningQuery} on which the distinct filter is being run. For
 * this reason, it is allocating a private {@link MemStore} and using a
 * finalizer pattern to ensure the eventual release of that {@link MemStore} and
 * the backing direct buffers.
 * <p>
 * Note: This can not be used with pipelined joins because it would allocate one
 * instance per as-bound evaluation of the pipeline join.
 * <p>
 * Note: You can change the code over the HTree/BTree by modifying only a few
 * lines.
 * 
 * TODO It would be nicer if we left the MRU 10k in the map and evicted the LRU
 * 10k each time the map reached 20k. This can not be done with the
 * {@link LinkedHashMap} as its API is not sufficient for this purpose. However,
 * similar batch LRU update classes have been defined in the
 * <code>com.bigdata.cache</code> package and could be adapted here for that
 * purpose.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DistinctElementFilter.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 */
public class NativeDistinctFilter extends BOpFilterBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends
            com.bigdata.bop.ap.filter.BOpFilter.Annotations, BTreeAnnotations,
            HashMapAnnotations {
        
        /**
         * Overrides the default initial capacity to be relatively large. Up to
         * this many entries will be permitted. Once the map reaches that
         * threshold, the entries will be flushed through to the backing index
         * in order to vector updates against that index.
         */
        int DEFAULT_INITIAL_CAPACITY = 10000;

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
    public static NativeDistinctFilter newInstance() {
        return new NativeDistinctFilter(BOp.NOARGS, BOp.NOANNS);
    }
    
    /**
     * Required deep copy constructor.
     */
    public NativeDistinctFilter(final NativeDistinctFilter op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public NativeDistinctFilter(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

    }

    @SuppressWarnings("rawtypes")
    @Override
    final protected Iterator filterOnce(Iterator src, final Object context) {
        
        return new Filterator(src, context, new DistinctFilterImpl());
        
    }

    private class DistinctFilterImpl extends Filter {

        private static final long serialVersionUID = 1L;

        /**
         * The size at which we will batch evict things from the LRU.
         */
        private final int nominalCapacity;
        
        /**
         * The fast JVM based cache. This is always allocated.
         */
        private final LinkedHashMap<SPO, byte[]> lru;

        /**
         * The metadata used to create the index.
         */
        private final IndexMetadata metadata;

        /**
         * The object used to format the keys for the index.
         */
        private final IKeyBuilder keyBuilder;

        /**
         * A persistence capable index for very large data sets. This is
         * allocated IFF the {@link #lru} overflows at least once.
         * <p>
         * Note: Maybe either a {@link BTree} or an {@link HTree}. The code has
         * paths for both.
         */
        private volatile BTree index;
        private volatile MemStore store;
        
        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            if (index != null) {
                index.close();
                index = null;
            }
            if (store != null) {
                store.close();
                store = null;
            }
        }
        
        public DistinctFilterImpl() {
            
            final int initialCapacity = NativeDistinctFilter.this.getProperty(
                    Annotations.INITIAL_CAPACITY,
                    Annotations.DEFAULT_INITIAL_CAPACITY);

            final float loadFactor = NativeDistinctFilter.this.getProperty(
                    Annotations.LOAD_FACTOR,
                    Annotations.DEFAULT_LOAD_FACTOR);
                    
            lru = new LinkedHashMap<SPO, byte[]>(initialCapacity, loadFactor);

            this.nominalCapacity = initialCapacity;

            /*
             * Setup the index. It will be created on demand.
             * 
             * Note: This sets up the keyBuilder. We need that even before the
             * index is created.
             */
            {
                
                metadata = new IndexMetadata(UUID.randomUUID());

                metadata.setAddressBits(NativeDistinctFilter.this.getProperty(
                        Annotations.BRANCHING_FACTOR,
                        Annotations.DEFAULT_BRANCHING_FACTOR));

//                metadata.setAddressBits(NativeDistinctFilter.this.getProperty(
//                        Annotations.ADDRESS_BITS,
//                        Annotations.DEFAULT_ADDRESS_BITS));

                metadata.setRawRecords(NativeDistinctFilter.this.getProperty(
                        Annotations.RAW_RECORDS,
                        Annotations.DEFAULT_RAW_RECORDS));

                metadata.setMaxRecLen(NativeDistinctFilter.this.getProperty(
                        Annotations.MAX_RECLEN, Annotations.DEFAULT_MAX_RECLEN));

                metadata.setBloomFilterFactory(BloomFilterFactory.DEFAULT);

                metadata.setAddressBits(NativeDistinctFilter.this.getProperty(
                        Annotations.WRITE_RETENTION_QUEUE_CAPACITY,
                        Annotations.DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY));

                final int ratio = 32; // TODO Config/tune.

                @SuppressWarnings("rawtypes")
                final ITupleSerializer<?, ?> tupleSer = new DefaultTupleSerializer(
                        new ASCIIKeyBuilderFactory(64/* initialCapacity */),//
                        new FrontCodedRabaCoder(ratio),// keys
                        EmptyRabaValueCoder.INSTANCE // vals
                );

                metadata.setTupleSerializer(tupleSer);

                // Used for building the index keys.
                keyBuilder = tupleSer.getKeyBuilder();
            
            }
            
        }

        /**
         * Evict everything in the LRU cache onto the persistence capable index.
         */
        private void evictAll() {
            if (index == null) {
                // Lazily allocate the index.
                allocateIndex();
            }
            final int n = lru.size();
            final byte[][] a = new byte[n][];
            {
                // Evict everthing into an array.
                final Iterator<Map.Entry<SPO, byte[]>> itr = lru.entrySet()
                        .iterator();
                int i = 0;
                while (itr.hasNext()) {
                    a[i++] = itr.next().getValue();
                    itr.remove();
                }
            }
            // Vector
            Arrays.sort(a, 0, a.length, UnsignedByteArrayComparator.INSTANCE);
            // Insert
            for (int i = 0; i < a.length; i++) {
                add(index, a[i]);
            }
        }
        
        /**
         * Create the persistence capable index.
         */
        private void allocateIndex() {

            if (index != null)
                throw new IllegalStateException();
            
            /*
             * This wraps an efficient raw store interface around a child memory
             * manager created from the IMemoryManager which is backing the
             * query.
             */
            store = new MemStore(new MemoryManager(DirectBufferPool.INSTANCE));

            /*
             * Create the index. It will support incremental eviction and
             * persistence.
             */
            index = BTree.create(store, metadata);
            
        }
        
        /**
         * Vectored DISTINCT filter.
         */
        @Override
        public boolean isValid(final Object obj) {

            final SPO spo = (SPO) obj;

            return add(spo);
            
        }
        
        /**
         * Add the {@link SPO} to the collection. This has the same semantics as
         * {@link Set#add(Object)}. The "collection" has two layers. A JVM
         * {@link LinkedHashMap}, which provides fast tests for recently
         * observed objects, and a persistence capable index, which scales to
         * very large data sets. The {@link #lru} is checked first. If the
         * object is not found, then the persistence capable index is checked.
         * If the object is not found there either, then object will be added
         * the {@link #lru}. Objects inserted into the {@link #lru} are
         * eventually batched through to the backing index.
         * 
         * @param spo
         *            The {@link SPO}.
         * 
         * @return <code>true</code> if the collection was modified.
         */
        private boolean add(final SPO spo) {

            if(lru.containsKey(spo)) {
                // already in the LRU
                return false;
            }

            // Build the key.
            keyBuilder.reset();
            IVUtility.encode(keyBuilder, spo.s());
            IVUtility.encode(keyBuilder, spo.p());
            IVUtility.encode(keyBuilder, spo.o());
            final byte[] key = keyBuilder.getKey();
            
            if (index != null && index.contains(key)) {
                // Already in the index.
                return false;
            }
            
            // Add to LRU.
            lru.put(spo, key);

            if (lru.size() >= nominalCapacity) {
                // Batch evict the entries into the index.
                evictAll();
            }

            // Return true since this is an SPO we had not seen before.
            return true;

        }

        /**
         * Add to {@link BTree}.
         * 
         * @param members
         * @param key
         * @return <code>true</code> iff not already present.
         */
        private boolean add(final BTree members, final byte[] key) {

            if (members.contains(key)) {

                return false;
                
            }
            
            members.insert(key, null/* val */);

            return true;
            
        }
        
        /**
         * Add to {@link HTree}.
         * 
         * @param members
         * @param key
         * @return <code>true</code> iff not already present.
         */
        private boolean add(final HTree members,final byte[] key) {
            
            final ITupleIterator<?> itr = members.lookupAll(key);

            if (itr.hasNext()) {

                // Already in the map.
                return false;

            }

            // Add to the map.
            members.insert(key, null/* val */);

            return true;

        }
        
    }

}
