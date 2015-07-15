package com.bigdata.bop.rdf.filter;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BTreeAnnotations;
import com.bigdata.bop.HTreeAnnotations;
import com.bigdata.bop.HashMapAnnotations;
import com.bigdata.bop.ap.filter.BOpFilterBase;
import com.bigdata.bop.ap.filter.DistinctFilter;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BloomFilterFactory;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.HTreeIndexMetadata;
import com.bigdata.btree.ICheckpointProtocol;
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
import com.bigdata.rdf.sparql.ast.eval.ASTConstructIterator;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rwstore.sector.MemStore;
import com.bigdata.rwstore.sector.MemoryManager;
import com.bigdata.util.BytesUtil.UnsignedByteArrayComparator;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Filterator;
import cutthecrap.utils.striterators.ICloseable;
import cutthecrap.utils.striterators.IPropertySet;

/**
 * A scalable DISTINCT operator based for {@link SPO}s.
 * <p>
 * Note: While highly scalable, this class will absorb a minimum of one direct
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
 * lines. See the comments in the file.
 * 
 * TODO Reads against the index will eventually degrade since we can not use
 * ordered reads because the iterator filter pattern itself is not vectored. We
 * might be able to fix this with a chunked filter pattern. Otherwise fixing
 * this will require a more significant refactor.
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
        
        /**
         */
        String KEY_ORDER = "keyOrder";
        
    }

    /**
     * A instance using the default configuration for the in memory hash map.
     * 
     * @param indexKeyOrder
     *            The natural order in which the {@link ISPO}s will arrive at
     *            this filter. This is used to decide on the filter key order
     *            which will have the best locality given the order of arrival.
     */
    public static NativeDistinctFilter newInstance(
            final SPOKeyOrder indexKeyOrder) {

        return new NativeDistinctFilter(BOp.NOARGS, Collections.singletonMap(
                Annotations.KEY_ORDER, (Object) indexKeyOrder));

    }
    
    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
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

        // MUST be specified.
        getRequiredProperty(Annotations.KEY_ORDER);

    }

    @SuppressWarnings("rawtypes")
    @Override
    final protected Iterator filterOnce(Iterator src, final Object context) {
        
        return new Filterator(src, context, new DistinctFilterImpl(this));
        
    }

    /**
     * Return the 3-component key order which has the best locality given that
     * the SPOs will be arriving in the natural order of the
     * <i>indexKeyOrder</i>. This is the keyOrder that we will use for the
     * filter. This gives the filter index structure the best possible locality
     * in terms of the order in which the SPOs are arriving.
     * <p>
     * The return valuer is an <code>int[3]</code>. The index is the ordinal
     * position of the triples mode key component for the filter keys. The value
     * at that index is the position in the {@link SPOKeyOrder} of the quads
     * mode index whose natural order determines the order of arrival of the
     * {@link ISPO} objects at this filter.
     * <p>
     * Thus, given indexKeyOrder = {@link SPOKeyOrder#CSPO}, the array:
     * 
     * <pre>
     * int[] = {1,2,3}
     * </pre>
     * 
     * would correspond to the filter key order SPO, which is the best possible
     * filter key order for the natural order order of the
     * {@link SPOKeyOrder#CSPO} index.
     * <p>
     * Note, however, that key orders can be expressed in this manner which are
     * not defined by {@link SPOKeyOrder}. For example, given
     * {@link SPOKeyOrder#PCSO} the best filter key order is <code>PSO</code>.
     * While there is no <code>PSO</code> key order declared by the
     * {@link SPOKeyOrder} class, we can use
     * 
     * <pre>
     * int[] = {0,2,3}
     * </pre>
     * 
     * which models the <code>PSO</code> key order for the purposes of this
     * class.
     * <p>
     * Note: This method now accepts triples in support of the
     * {@link ASTConstructIterator}
     * 
     * @see Annotations#INDEX_KEY_ORDER
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/579">
     *      CONSTRUCT should apply DISTINCT (s,p,o) filter </a>
     */
    public static int[] getFilterKeyOrder(final SPOKeyOrder indexKeyOrder) {

        if (indexKeyOrder == null)
            throw new IllegalArgumentException();

//        if (indexKeyOrder.getKeyArity() != 4)
//            throw new IllegalArgumentException();

        final int[] filterKeyOrder;
        switch (indexKeyOrder.index()) {
        // TRIPLES
        case SPOKeyOrder._SPO:
            filterKeyOrder = new int[] { 0, 1, 2 };
            break;
        case SPOKeyOrder._POS:
            filterKeyOrder = new int[] { 1, 2, 0 };
            break;
        case SPOKeyOrder._OSP:
            filterKeyOrder = new int[] { 2, 0, 1 };
            break;
        // QUADS
        case SPOKeyOrder._SPOC:
            filterKeyOrder = new int[] { 0, 1, 2 };
            break;
        case SPOKeyOrder._CSPO:
            filterKeyOrder = new int[] { 1, 2, 3 };
            break;
        case SPOKeyOrder._POCS:
            filterKeyOrder = new int[] { 0, 1, 3 };
            break;
        case SPOKeyOrder._OCSP:
            filterKeyOrder = new int[] { 0, 2, 3 };
            break;
        case SPOKeyOrder._PCSO:
            filterKeyOrder = new int[] { 0, 2, 3 };
            break;
        case SPOKeyOrder._SOPC:
            filterKeyOrder = new int[] { 0, 1, 2 };
            break;
        default:
            throw new AssertionError();
        }
        return filterKeyOrder;
    }
    
    /**
     * A {@link Filter} which passes only the DISTINCT {@link ISPO}s and is
     * backed by a scalable data structure (BTree or HTree).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    static public class DistinctFilterImpl extends Filter implements
            ICloseable {

        private static final long serialVersionUID = 1L;

        /**
         * The size at which we will batch evict things from the LRU.
         */
        private final int nominalCapacity;
        
        /**
         * The fast JVM based cache. This is always allocated.
         */
        private final LinkedHashMap<ISPO, byte[]> lru;

        /**
         * The metadata used to create the index.
         */
        private final IndexMetadata metadata;

        /**
         * The object used to format the keys for the index.
         */
        private final IKeyBuilder keyBuilder;

        /**
         * The key order used to build the triples mode keys for the DISTINCT
         * SPO filter.
         * 
         * @see NativeDistinctFilter#getFilterKeyOrder(SPOKeyOrder)
         */
        private final int[] filterKeyOrder;
        
        /**
         * A persistence capable index for very large data sets. This is
         * allocated IFF the {@link #lru} overflows at least once.
         * <p>
         * Note: Maybe either a {@link BTree} or an {@link HTree}. The code has
         * paths for both.
         */
        private volatile ICheckpointProtocol index;
        
        /**
         * <code>true</code> until {@link #close() closed}.
         */
        private final AtomicBoolean open = new AtomicBoolean(true);
        
        /**
         * When <code>true</code>, the {@link BTree} will be used. When
         * <code>false</code> the {@link HTree}.
         * <p>
         * Note: Historical testing indicated that the {@link BTree} was faster
         * for this application.
         * 
         * TODO Edit HTree/BTree here.
         */
        private final boolean isBTree = true;
        
        /**
         * The backing store (native memory).
         */
        private volatile MemStore store;
        
        @Override
        protected void finalize() throws Throwable {
            close();
            super.finalize();
        }

        /**
         * Release resources associated with the filter.
         * <p>
         * Note: This is done automatically by {@link #finalize()}, but it
         * should be done pro-actively whenever possible.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/582">
         *      IStriterator does not support close() protocol for Ifilter </a>
         */
        @Override
        public void close() {
            if (open.compareAndSet(true/* expect */, false/* update */)) {
                /*
                 * Close when first invoked.
                 */
                if (index != null) {
                    index.close();
                    index = null;
                }
                if (store != null) {
                    store.close();
                    store = null;
                }
            }
        }

        @SuppressWarnings("unchecked")
        static private <T> T getRequiredProperty(final IPropertySet pset,
                final String name) {

            final Object val = pset.getProperty(name);

            if (val == null)

                if (val == null)
                    throw new IllegalStateException("Required property: "
                            + name + " : " + NativeDistinctFilter.class);

            return (T) val;

        }

        @SuppressWarnings("unchecked")
        static private <T> T getProperty(final IPropertySet pset,
                final String name, final T defaultValue) {

            final Object val = pset.getProperty(name);

            if (val != null)
                return (T) val;

            return defaultValue;

        }
        
        /**
         * DISTINCT {@link ISPO} filter based on persistence capable data
         * structures.
         * 
         * @param properties
         *            Used to configure the DISTINCT filter.
         * 
         * @see DistinctFilter.Annotations
         */
        public DistinctFilterImpl(final IPropertySet properties) {
            
            final int initialCapacity = getProperty(properties,
                    Annotations.INITIAL_CAPACITY,
                    Annotations.DEFAULT_INITIAL_CAPACITY);

            final float loadFactor = getProperty(properties,
                    Annotations.LOAD_FACTOR,
                    Annotations.DEFAULT_LOAD_FACTOR);
                    
            lru = new LinkedHashMap<ISPO, byte[]>(initialCapacity, loadFactor);

            this.nominalCapacity = initialCapacity;

            /*
             * Setup the index. It will be created on demand.
             * 
             * Note: This sets up the keyBuilder. We need that even before the
             * index is created.
             */
            {

                final SPOKeyOrder indexKeyOrder = (SPOKeyOrder) getRequiredProperty(
                        properties, Annotations.KEY_ORDER);

                filterKeyOrder = getFilterKeyOrder(indexKeyOrder);

                // Setup BTree/HTree.
                metadata = isBTree ? new IndexMetadata(UUID.randomUUID())
                        : new HTreeIndexMetadata(UUID.randomUUID());

                // IFF BTree
                metadata.setBranchingFactor(getProperty(properties,
                        BTreeAnnotations.BRANCHING_FACTOR,
                        256));// TODO Overridden here. BTreeAnnotations.DEFAULT_BRANCHING_FACTOR));

                // IFF HTree
                if (metadata instanceof HTreeIndexMetadata) {
                    ((HTreeIndexMetadata) metadata)
                            .setAddressBits(getProperty(properties,
                                            HTreeAnnotations.ADDRESS_BITS,
                                            HTreeAnnotations.DEFAULT_ADDRESS_BITS));
                }

                metadata.setRawRecords(getProperty(properties,
                        Annotations.RAW_RECORDS,
                        Annotations.DEFAULT_RAW_RECORDS));

                // No values.
                metadata.setMaxRecLen(0);

                metadata.setBloomFilterFactory(BloomFilterFactory.DEFAULT);

                metadata.setWriteRetentionQueueCapacity(getProperty(properties,
                        Annotations.WRITE_RETENTION_QUEUE_CAPACITY,
                        Annotations.DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY));

                final int ratio = 32; // TODO Config/tune front-coding ratio.

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
                // Evict everything into an array.
                final Iterator<Map.Entry<ISPO, byte[]>> itr = lru.entrySet()
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
            if (index instanceof BTree) {
                for (int i = 0; i < a.length; i++) {
                    add((BTree) index, a[i]);
                }
            } else if (index instanceof HTree) {
                for (int i = 0; i < a.length; i++) {
                    add((HTree) index, a[i]);
                }
            } else
                throw new AssertionError();
        }
        
        /**
         * Create the persistence capable index.
         */
        private void allocateIndex() {

            if (index != null)
                throw new IllegalStateException();

            if (!open.get()) {
                // Explicitly closed.
                throw new IllegalStateException();
            }
            
            /*
             * This wraps an efficient raw store interface around a child memory
             * manager created from the IMemoryManager which is backing the
             * query.
             */
            store = new MemStore(new MemoryManager(DirectBufferPool.INSTANCE));

            /*
             * Create the index. It will support incremental eviction and
             * persistence (against the memory manager).
             */
            if (isBTree) {
                index = BTree.create(store, metadata);
            } else {
                index = HTree.create(store, (HTreeIndexMetadata) metadata);
            }
            
        }
        
        /**
         * Vectored DISTINCT filter.
         */
        @Override
        public boolean isValid(final Object obj) {

            final ISPO spo = (ISPO) obj;

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
        private boolean add(final ISPO spo) {

            if(lru.containsKey(spo)) {
                // already in the LRU
                return false;
            }

            /*
             * Build the key in the order in which the hash join is visiting the
             * B+Tree AP. This gives the DISTINCT index the best possible
             * locality in terms of the natural order in which the SPOs will be
             * arriving.
             */
            keyBuilder.reset();

            for (int i = 0; i < 3; i++) {

                IVUtility.encode(keyBuilder, spo.get(filterKeyOrder[i]));

            }

            final byte[] key = keyBuilder.getKey();
            
            if (index != null) {
                // Test index for this key.
                if (index instanceof BTree) {
                    if (((BTree) index).contains(key)) {
                        // Already in the index.
                        return false;
                    }
                } else if (index instanceof HTree) {
                    if (((HTree) index).contains(key)) {
                        // Already in the index.
                        return false;
                    }
                } else
                    throw new AssertionError();
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
        private boolean add(final HTree members, final byte[] key) {

            if(members.contains(key)) {
                
                return false;
                
            }
            
            // Add to the map.
            members.insert(key, null/* val */);

            return true;

        }
        
    }

}
