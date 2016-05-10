/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
/*
 * Created on Feb 15, 2012
 */

package com.bigdata.rdf.internal.encoder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IndexAnnotations;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BloomFilterFactory;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.Tuple;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.lexicon.BlobsIndexHelper;
import com.bigdata.rdf.lexicon.BlobsTupleSerializer;
import com.bigdata.rdf.lexicon.Id2TermTupleSerializer;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.util.Bytes;

/**
 * A concrete implementation using scalable {@link BTree}s to store the mapping
 * from an {@link IV} to the cached RDF {@link Value}. This approach is useful
 * when you will be encoding a LOT of data and you need to get the cached RDF
 * {@link Value} objects off of the JVM heap.
 * <p>
 * Note: Two different {@link BTree} instances are used. One for {@link TermId}s
 * and another for {@link BlobIV}s. These indices use exactly the same schema as
 * the ID2TERM and BLOBS indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * This class is out of use. As part of https://jira.blazegraph.com/browse/BLZG-1899, 
 * we carried out some experiments illustrating that caching the IVs (i.e., preserving
 * them in HTree hash joins) is far slower than just re-materializing them. We decided
 * to retire the class and, instead, do remove variables from the doneSet within the
 * {@link AST2BOpUtility} update process whenever we set up an analytic hash join.
 */
@Deprecated
public class IVBindingSetEncoderWithIVCache extends IVBindingSetEncoder {

    /**
     * The namespace of the {@link LexiconRelation} IFF we need to maintain
     * the {@link #ivCache}.
     */
    private final String namespace;

    /**
     * The {@link BigdataValueFactory} for the {@link LexiconRelation} IFF we
     * need to maintain the {@link #ivCache}.
     */
    private final BigdataValueFactory valueFactory;

    /**
     * The set of variables for which materialized {@link IV}s have been
     * observed.
     */
    final protected LinkedHashSet<IVariable<?>> ivCacheSchema;

    /**
     * A cache mapping from non-inline {@link IV}s ({@link TermId}s and
     * {@link BlobIV}s) whose {@link IVCache} association was set to the
     * corresponding {@link BigdataValue}. Used to batch updates into
     * the ID2TERM and BLOBS indices.
     */
    final Map<IV<?, ?>, BigdataValue> cache;    
    
    /**
     * The {@link IV}:{@link BigdataValue} mapping for non-{@link BlobIV}s. This
     * captures any cached BigdataValue references encountered on {@link IV}s.
     * This map does not store duplicate entries for the same {@link IV}.
     * <p>
     * Note: This is precisely the same mapping we use for the ID2TERM index.
     */
    private final AtomicReference<BTree> ivCache = new AtomicReference<BTree>();

    /**
     * The {@link IV}:{@link BigdataValue} mapping for {@link BlobIV}s with
     * cached {@link BigdataValue}s. This captures any cached BigdataValue
     * references encountered on {@link BlobIV}s. This map does not store
     * duplicate entries for the same {@link IV}.
     * <p>
     * Note: This is precisely the same mapping we use for the BLOBS index.
     */
    private final AtomicReference<BTree> blobsCache = new AtomicReference<BTree>();
    
    /**
     * The {@link IV}:{@link BigdataValue} mapping for {@link LiteralExtensionIV}s
     * with cached {@link BigdataValue}s. This captures any cached BigdataValue
     * references encountered on {@link LiteralExtensionIV}s. This map does not
     * store duplicate entries for the same {@link IV}.
     */
    private final AtomicReference<BTree> literalExtensionIVCache = new AtomicReference<BTree>();

    public String toString() {
        /*
         * Note: schema and ivCacheSchema are not thread-safe, so it is not safe
         * to show their state here. A concurrent modification exception could
         * easily be thrown. Not synchronizing on these things is currently
         * valued more than observing their contents outside of a debugger.
         */
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("{namespace=" + namespace);
        if (ivCache.get() != null)
            sb.append(",ivCacheSize=" + getIVCacheSize());
        if (blobsCache.get() != null)
            sb.append(",blobCacheSize=" + getBlobsCacheSize());
        sb.append("}");
        return sb.toString();
    }
    
    private long getIVCacheSize() {

        final BTree ndx = ivCache.get();

        if (ndx != null) {

            return ndx.getEntryCount();

        }

        return 0L;
        
    }
    
    private long getLiteralExtensionIVCacheSize() {

        final BTree ndx = literalExtensionIVCache.get();

        if (ndx != null) {

            return ndx.getEntryCount();

        }

        return 0L;
        
    }

    private long getBlobsCacheSize() {

        final BTree ndx = blobsCache.get();

        if (ndx != null) {

            return ndx.getEntryCount();

        }

        return 0L;
        
    }

    /**
     * Setup the {@link IndexMetadata} for {@link #ivCache}.
     * <p>
     * Note: This is basically the same setup as the ID2TERM index.
     */
    private IndexMetadata getIVCacheIndexMetadata(final BOp op) {

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        final int branchingFactor = 256;// TODO Config/tune.
        
        final int ratio = 32; // TODO Config/tune.
        
        metadata.setBranchingFactor(branchingFactor);

        metadata.setWriteRetentionQueueCapacity(op.getProperty(
                IndexAnnotations.WRITE_RETENTION_QUEUE_CAPACITY,
                IndexAnnotations.DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY));

        metadata.setTupleSerializer(new Id2TermTupleSerializer(namespace,
                valueFactory, new ASCIIKeyBuilderFactory(Bytes.SIZEOF_LONG),//
                new FrontCodedRabaCoder(ratio), SimpleRabaCoder.INSTANCE));

        // a bloom filter should help avoid lookups when IVs do not have cached
        // values.
        metadata.setBloomFilterFactory(BloomFilterFactory.DEFAULT);

        if (true) {
          
            // enable raw record support.
            metadata.setRawRecords(true);

            /*
             * Very small RDF values can be inlined into the index, but after
             * that threshold we want to have the values out of line on the
             * backing store.
             * 
             * TODO Tune this and the threshold at which we use the BLOBS index
             * instead.
             */
            metadata.setMaxRecLen(16);
            
        }
        
        return metadata;

    }
    
    /**
     * Setup the {@link IndexMetadata} for {@link #literalExtensionIVCache}.
     * <p>
     * Note: This is basically the same setup as the ID2TERM index.
     */
    private IndexMetadata getLiteralExtensionIVCacheIndexMetadata(final BOp op) {
        
        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        final int branchingFactor = 256;// TODO Config/tune.
        
        final int ratio = 32; // TODO Config/tune.
        
        metadata.setBranchingFactor(branchingFactor);

        metadata.setWriteRetentionQueueCapacity(op.getProperty(
                IndexAnnotations.WRITE_RETENTION_QUEUE_CAPACITY,
                IndexAnnotations.DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY));

        metadata.setTupleSerializer(new Id2TermTupleSerializer(namespace,
                valueFactory, new ASCIIKeyBuilderFactory(Bytes.SIZEOF_LONG),//
                new FrontCodedRabaCoder(ratio), SimpleRabaCoder.INSTANCE));

        // a bloom filter should help avoid lookups when IVs do not have cached
        // values.
        metadata.setBloomFilterFactory(BloomFilterFactory.DEFAULT);

        if (true) {
          
            // enable raw record support.
            metadata.setRawRecords(true);

            /*
             * Very small RDF values can be inlined into the index, but after
             * that threshold we want to have the values out of line on the
             * backing store.
             * 
             * TODO Tune this and the threshold at which we use the BLOBS index
             * instead.
             */
            metadata.setMaxRecLen(16);
            
        }
        
        return metadata;
    }
    
    /**
     * Setup the {@link IndexMetadata} for {@link #blobsCache}.
     * <p>
     * Note: This is basically the same setup as the BLOBS index.
     */
    private IndexMetadata getBlobsCacheIndexMetadata(final BOp op) {

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        metadata.setTupleSerializer(new BlobsTupleSerializer(namespace,
                valueFactory));

        // enable raw record support.
        metadata.setRawRecords(true);

        /*
         * The presumption is that we are storing large literals (blobs) in this
         * index so we always want to write them on raw records rather than have
         * them be inline in the leaves of the index.
         */
        metadata.setMaxRecLen(0);

        /*
         * TODO The default branching factor for this index should probably be
         * pretty big. All of the values are on raw records, so it is just the
         * keys in the index and the have a fixed width (8 bytes).
         */
        final int branchingFactor = 256;
        
        metadata.setBranchingFactor(branchingFactor);

        metadata.setWriteRetentionQueueCapacity(op.getProperty(
                IndexAnnotations.WRITE_RETENTION_QUEUE_CAPACITY,
                IndexAnnotations.DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY));

        // a bloom filter should help avoid lookups when IVs do not have cached
        // values.
        metadata.setBloomFilterFactory(BloomFilterFactory.DEFAULT);

        return metadata;

    }

    /**
     * @param store
     *            The backing {@link IRawStore} for the {@link IV} to
     *            {@link BigdataValue} cache.
     * @param filter
     *            <code>true</code> iff this is in support of a DISTINCT filter.
     *            <p>
     *            Note: we do not maintain the {@link #ivCacheSchema} for a
     *            DISTINCT filter since the original solutions flow through the
     *            filter.
     * @param op
     *            The operator whose annotations are used to parameterize the
     *            creation of the backing indices for the {@link IV} to
     *            {@link BigdataValue} cache.
     */
    public IVBindingSetEncoderWithIVCache(final IRawStore store,
            final boolean filter, final BOp op) {

        super(BigdataValueFactoryImpl.getInstance(((String[]) op
                .getRequiredProperty(Predicate.Annotations.RELATION_NAME))[0]), filter);
        
        if (!filter) {

            /*
             * Setup the IV => BigdataValue mapping. This captures any cached
             * BigdataValue references encountered on IVs. This map does not store
             * duplicate entries for the same IV.
             */

            namespace = ((String[]) op
                    .getRequiredProperty(Predicate.Annotations.RELATION_NAME))[0];

            valueFactory = BigdataValueFactoryImpl.getInstance(namespace);

            ivCache.set(BTree.create(store, getIVCacheIndexMetadata(op)));

            literalExtensionIVCache.set(BTree.create(store, getLiteralExtensionIVCacheIndexMetadata(op)));
            
            blobsCache.set(BTree.create(store, getBlobsCacheIndexMetadata(op)));

            ivCacheSchema = new LinkedHashSet<IVariable<?>>();

            cache = new HashMap<IV<?, ?>, BigdataValue>();

        } else {

            namespace = null;
            
            valueFactory = null;
            
            ivCacheSchema = null;
            
            cache = null;
            
        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * This maintains the {@link IVCache} associations UNLESS
     * <i>filter:=true</code> was specified to the constructor.
     */
    @Override
    public boolean isValueCache() {

        return !filter;

    }

    /**
     * Checkpoint the {@link BTree} instance(s) used to buffer the cached
     * {@link IV} to RDF {@link Value} mappings and then re-load the them in a
     * read-only mode from their checkpoint(s). This exposes a view of the
     * {@link BTree} instance(s) which is safe for concurrent readers.
     */
    public void saveSolutionSet() {

        flush();
        
        checkpointBTree(ivCache);
        
        checkpointBTree(literalExtensionIVCache);
        
        checkpointBTree(blobsCache);
        
    }
    
    private void checkpointBTree(final AtomicReference<BTree> ref) {

        final BTree tmp = ref.get();

        if (tmp != null) {

            // Checkpoint the HTree.
            final Checkpoint checkpoint = tmp.writeCheckpoint2();

            final BTree readOnly = BTree.load(tmp.getStore(),
                    checkpoint.getCheckpointAddr(), true/* readOnly */);

            // Get a read-only view of the HTree.
            if (!ref.compareAndSet(tmp/* expect */, readOnly)) {

                throw new IllegalStateException();

            }

        }

    }

    @Override
    public void release() {

        BTree tmp2 = ivCache.getAndSet(null/* newValue */);

        if (tmp2 != null) {

            tmp2.close();

        }

        tmp2 = blobsCache.getAndSet(null/* newValue */);
        
        if (tmp2 != null) {

            tmp2.close();

        }
        
        tmp2 = literalExtensionIVCache.getAndSet(null/* newValue */);
        
        if (tmp2 != null) {
            
            tmp2.close();
            
        }


        if (ivCacheSchema != null) {

            ivCacheSchema.clear();
            
        }
        
        super.release();
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Vectored update of the internal ivCache.
     */
    @Override
    public void flush() {

        if (filter) {

            super.flush();
            
            return;
            
        }

        final BTree ivCache = this.ivCache.get();

        final BTree blobsCache = this.blobsCache.get();
        
        final BTree literalExtensionIVCache = this.literalExtensionIVCache.get();

        // Lazily resolved.
        BlobsIndexHelper h = null;

        // Used to serialize RDF {@link Value}s.
        final Id2TermTupleSerializer tupSer = (Id2TermTupleSerializer) ivCache
                .getIndexMetadata().getTupleSerializer();

        for (Map.Entry<IV<?, ?>, BigdataValue> e : cache.entrySet()) {

            final IV<?, ?> iv = e.getKey();

            final BigdataValue value = e.getValue();

            if (iv instanceof BlobIV<?>) {

                if (h == null) {
        
                    // Lazily resolved.
                    h = new BlobsIndexHelper();
                    
                }

                /*
                 * Note: The insert logic for the BLOBS index here is different
                 * (and much simpler) because we already have the exact BlobIV
                 * and we want to ensure that there is an entry under that key
                 * in the [blobsCache].
                 * 
                 * Normally, you use the helper class to do a conditional
                 * resolveOrAddValue() which assigns a collision counter for the
                 * blob, but here we already know the collision counter which
                 * was assigned by the real BLOBS index on the LexiconRelation.
                 */
                
                final IKeyBuilder keyBuilder = h.newKeyBuilder();

                final byte[] key = iv.encode(keyBuilder.reset()).getKey();

                final byte[] val = valueFactory.getValueSerializer().serialize(
                        value);

                if (!blobsCache.contains(key)) {

                    blobsCache.insert(key, val);

                }

            /**
             * BLZG-1899: we also need to encode literal extension IVs: these IVs
             *            require materialization although they are inlined, since
             *            their interpretation depends on the LexiconConfiguration
             */
            } else if (iv instanceof LiteralExtensionIV) {

                final IKeyBuilder keyBuilder = new KeyBuilder();

                final byte[] key = iv.encode(keyBuilder.reset()).getKey();
                
                final byte[] val = valueFactory.getValueSerializer().serialize(value);
                
                if (!literalExtensionIVCache.contains(key)) {
                    
                    literalExtensionIVCache.insert(key, val);
                }
                
            } else {

                final byte[] key = tupSer.serializeKey(iv);

                if (!ivCache.contains(key)) {

                    ivCache.insert(key, tupSer.serializeVal(value));

                }

            }

        }

        if (cache != null)
            cache.clear();
        
        super.flush();
    }

    /**
     * {@inheritDoc}
     * 
     * TODO If we vectored this operation it would substantially reduce its
     * costs. We would have to collect up a bunch of solutions which needed
     * resolution, then collect up the IVs which do not have cached values for
     * variables which might have values in the ivCache. We would then sort the
     * IVs and do a vectored resolution against the ivCache. Finally, the
     * solutions could be output in a chunk with their resolved Values.
     * <p>
     * If the operator is not vectored, then we should just fold it into
     * {@link #decodeSolution(byte[], int, int)}.
     */
    @SuppressWarnings("rawtypes")
    public void resolveCachedValues(final IBindingSet bset) {
        
        final BTree ivCache = this.ivCache.get();

        final BTree literalExtensionIVCache = this.literalExtensionIVCache.get();

        final BTree blobsCache = this.blobsCache.get();

        if ((ivCache == null || ivCache.getEntryCount() == 0L)
                && (literalExtensionIVCache == null || literalExtensionIVCache.getEntryCount() == 0L)
                && (blobsCache == null || blobsCache.getEntryCount() == 0L)) {

            // Nothing materialized.
            return;
            
        }
        
        final Id2TermTupleSerializer tupSer = (Id2TermTupleSerializer) ivCache
                .getIndexMetadata().getTupleSerializer();
        
        final IKeyBuilder keyBuilder = tupSer.getKeyBuilder();

        final Tuple ivCacheTuple = new Tuple(ivCache, IRangeQuery.KEYS
                | IRangeQuery.VALS);

        // Lazily initialized.
        BlobsIndexHelper h = null;
        
        final Iterator<Map.Entry<IVariable, IConstant>> itr = bset.iterator();

        while (itr.hasNext()) {

            final Map.Entry<IVariable, IConstant> e = itr.next();

            final IVariable<?> v = e.getKey();
            
            if (!ivCacheSchema.contains(v)) {
                // Nothing observed for that variable.
                continue;
            }
            
            final IV iv = (IV) e.getValue().get();

            if (iv.hasValue()) {
                // Already cached.
                continue;
            }

            keyBuilder.reset();

            if (iv instanceof BlobIV<?>) {

                final BlobIV<?> blobIV = (BlobIV<?>) iv;

                if(h == null) {
                    
                    h = new BlobsIndexHelper();
                    
                }
                
                final byte[] val = h.lookup(blobsCache, blobIV, keyBuilder);

                if (val == null) {

                    continue;

                }

                /*
                 * TODO Factor out the buffers used to do the de-serialization
                 * when we vector the resolution of IVs.
                 */
                final BigdataValue value = valueFactory.getValueSerializer()
                        .deserialize(val);

                iv.setValue(value);

            } else if (iv instanceof LiteralExtensionIV) {
                
                final LiteralExtensionIV<?> literalExtensionIv = (LiteralExtensionIV<?>) iv;
                
                IVUtility.encode(keyBuilder, literalExtensionIv);

                final byte[] key = keyBuilder.getKey();

                if (literalExtensionIVCache.lookup(key, ivCacheTuple) == null) {

                    continue;

                }
                
                final BigdataValue value = tupSer.deserialize(ivCacheTuple);

                iv.setValue(value);
                
            } else {

                IVUtility.encode(keyBuilder, iv);

                final byte[] key = keyBuilder.getKey();

                if (ivCache.lookup(key, ivCacheTuple) == null) {

                    continue;

                }

                final BigdataValue value = tupSer.deserialize(ivCacheTuple);

                iv.setValue(value);

            }
            
        }

    }
    
    @Override
    void cacheSchemaAndValue(final IVariable<?> v, final IV<?,?> iv, final boolean updateCache) {

        /**
         *  BLZG-1899: we need to materialize all IVs that require materialization;
         *             before, this condition was !iv.isInline(), which did not consider
         *             cases such as LiteralExtensionIVs that are inline but nevertheless
         *             need to be materialized
         */
        if (iv.needsMaterialization() && iv.hasValue() && !filter) {
            ivCacheSchema.add(v);
            if (updateCache && cache != null)
                cache.put(iv, iv.getValue());
        }
        
    }
    

}
