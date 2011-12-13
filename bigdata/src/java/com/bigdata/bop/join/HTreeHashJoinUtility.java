/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
/*
 * Created on Aug 30, 2011
 */

package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HTreeAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IndexAnnotations;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BloomFilterFactory;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.Tuple;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.counters.CAT;
import com.bigdata.htree.HTree;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.lexicon.BlobsIndexHelper;
import com.bigdata.rdf.lexicon.BlobsTupleSerializer;
import com.bigdata.rdf.lexicon.Id2TermTupleSerializer;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.rwstore.sector.MemStore;
import com.bigdata.striterator.Chunkerator;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.ICloseableIterator;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;
import cutthecrap.utils.striterators.Visitor;

/**
 * Utility methods to support hash index builds and hash index joins using a
 * scalable native memory data structures.
 * 
 * <h2>Vectoring and IV encoding</h2>
 * 
 * In order to provide efficient encoding and persistence of solutions on the
 * {@link HTree}, this class is written directly to the RDF data model. Rather
 * than POJO serialization, solutions are encoded as logical {@link IV}[]s in a
 * manner very similar to how we represent the keys of the statement indices.
 * <p>
 * Since this encoding does not persist the {@link IV#getValue() cache}, a
 * separate mapping must be maintained from {@link IV} to {@link BigdataValue}
 * for those {@link IV}s which have a materialized {@link BigdataValue}.
 * 
 * TODO Do a 64-bit hash version which could be used for hash indices having
 * more than 500M distinct join variable combinations. Note that at 500M
 * distinct join variable combinations we have a 1 in 4 chance of a hash
 * collision. Whether or not that turns into a cost really depends on the
 * cardinality of the solutions per distinct combination of the join variables.
 * If there is only one solution per join variable combination, then those
 * collisions will cause basically no increase in the work to be done. However,
 * if there are 50,000 solutions per distinct combination of the join variables
 * then we would be better off using a 64-bit hash code.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: HTreeHashJoinUtility.java 5568 2011-11-07 19:39:12Z thompsonbry
 */
public class HTreeHashJoinUtility implements IHashJoinUtility {

    static private final transient Logger log = Logger
            .getLogger(HTreeHashJoinUtility.class);

    /**
     * Note: If joinVars is an empty array, then the solutions will all hash to
     * ONE (1).
     */
    private static final int ONE = 1;
    
    /**
     * Return the hash code which will be used as the key given the ordered
     * as-bound values for the join variables.
     * 
     * @param joinVars
     *            The join variables.
     * @param bset
     *            The bindings whose as-bound hash code for the join variables
     *            will be computed.
     * @param ignoreUnboundVariables
     *            If a variable without a binding should be silently ignored.
     * 
     * @return The hash code.
     * 
     * @throws JoinVariableNotBoundException
     *             if there is no binding for a join variable.
     * 
     *             FIXME Does anything actually rely on the
     *             {@link JoinVariableNotBoundException}? It would seem that
     *             this exception could only be thrown if the joinvars[] was
     *             incorrectly formulated as it should only include
     *             "known bound" variables. (I think that this is related to
     *             incorrectly passing along empty solutions for named subquery
     *             hash joins.)
     */
    private static int hashCode(final IVariable<?>[] joinVars,
            final IBindingSet bset, final boolean ignoreUnboundVariables)
            throws JoinVariableNotBoundException {

        int h = ONE;

        for (IVariable<?> v : joinVars) {

            final IConstant<?> c = bset.get(v);

            if (c == null) {

                if(ignoreUnboundVariables)
                    continue;
                
                // Reject any solution which does not have a binding for a join
                // variable.
                throw new JoinVariableNotBoundException(v.getName());
                
            }
            
            // Works Ok.
            h = 31 * h + c.hashCode();
            
//          // Martyn's version.  Also works Ok.
            // @see http://burtleburtle.net/bob/hash/integer.html
//            
//            final int hc = c.hashCode();
//            h += ~(hc<<15);
//            h ^=  (hc>>10);
//            h +=  (hc<<3);
//            h ^=  (hc>>6);

        }
        
        if (log.isTraceEnabled())
            log.trace("hashCode=" + h + ", joinVars="
                    + Arrays.toString(joinVars) + " : " + bset);

        return h;

    }

    /**
     * <code>true</code> until the state is discarded by {@link #release()}.
     */
    private final AtomicBoolean open = new AtomicBoolean(true);
    
    /**
     * This basically controls the vectoring of the hash join.
     * 
     * TODO parameter from operator annotations. Note that 10k tends to put too
     * much heap pressure on the system if the source chunks happen to be
     * smallish. 1000k or 100 is probably the right value until we improve
     * vectoring of the query engine.
     */
    private final int chunkSize = 1000;//ChunkedWrappedIterator.DEFAULT_CHUNK_SIZE;

    /**
     * The schema provides the order in which the {@link IV}[] for solutions
     * stored in the hash index are encoded in the {@link HTree}. {@link IV}
     * s which are not bound are modeled by a {@link TermId#NullIV}.
     * <p>
     * Note: In order to be able to encode/decode the schema based on the
     * lazy identification of the variables which appear in solutions the
     * {@link HTree} must store variable length {@link IV}[]s since new
     * variables may be discovered at any point.
     */
    private final LinkedHashSet<IVariable<?>> schema;

    /**
     * The set of variables for which materialized {@link IV}s have been
     * observed.
     */
    private final LinkedHashSet<IVariable<?>> ivCacheSchema;

    /**
     * The type of join to be performed.
     */
    private final JoinTypeEnum joinType;
    
    /**
     * <code>true</code> iff the join is OPTIONAL.
     */
    private final boolean optional;
    
    /**
     * <code>true</code> iff this is a DISTINCT filter.
     */
    private final boolean filter;
    
//    /**
//     * The operator which was used to construct the {@link IHashJoinUtility}
//     * state.
//     * <p>
//     * Note: This is NOT necessarily the operator which is currently executing.
//     * Hash indices are often built by one operator and then consumed by
//     * other(s).
//     */
//    private final PipelineOp op;
    
    /**
     * @see HashJoinAnnotations#ASK_VAR
     */
    private final IVariable<?> askVar;
    
    /**
     * The join variables.
     */
    private final IVariable<?>[] joinVars;

    /**
     * The variables to be retained (optional, all variables are retained if
     * not specified).
     */
    private final IVariable<?>[] selectVars;

    /**
     * The join constraints (optional).
     */
    private final IConstraint[] constraints;

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
     * The backing {@link IRawStore}.
     */
    private final IRawStore store;
    
    /**
     * The hash index. The keys are int32 hash codes built from the join
     * variables. The values are an {@link IV}[], similar to the encoding in
     * the statement indices. The mapping from the index positions in the
     * {@link IV}s to the variables is specified by the {@link #schema}.
     */
    private final AtomicReference<HTree> rightSolutions = new AtomicReference<HTree>();

    /**
     * The set of distinct source solutions which joined. This set is
     * maintained iff the join is optional and is <code>null</code>
     * otherwise.
     */
    private final AtomicReference<HTree> joinSet = new AtomicReference<HTree>();
    
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
     * 
     * TODO The last committed revision of this class prior to introducing the
     * blobsCache was r5568. I need to verify that there is no performance
     * regression due to the presence of the blobsCache.
     */
    private final AtomicReference<BTree> blobsCache = new AtomicReference<BTree>();

    /**
     * The maximum #of (left,right) solution joins that will be considered
     * before failing the join. This is used IFF there are no join variables.
     * 
     * FIXME Annotation and query hint for this. Probably on
     * {@link HashJoinAnnotations}.
     */
    private final long noJoinVarsLimit = HashJoinAnnotations.DEFAULT_NO_JOIN_VARS_LIMIT;
    
    /**
     * The #of left solutions considered for a join.
     */
    private final CAT nleftConsidered = new CAT();

    /**
     * The #of right solutions considered for a join.
     */
    private final CAT nrightConsidered = new CAT();

    /**
     * The #of solution pairs considered for a join.
     */
    private final CAT nJoinsConsidered = new CAT();
    
    /**
     * The hash index.
     */
    private HTree getRightSolutions() {
        
        return rightSolutions.get();
        
    }
    
    /**
     * The set of distinct source solutions which joined. This set is
     * maintained iff the join is optional and is <code>null</code>
     * otherwise.
     */
    private HTree getJoinSet() {

        return joinSet.get();
        
    }
    
    /**
     * Human readable representation of the {@link IHashJoinUtility} metadata
     * (but not the solutions themselves).
     */
    public String toString() {

        final StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getSimpleName());
        
        sb.append("{open=" + open);
        sb.append(",joinType="+joinType);
        sb.append(",chunkSize=" + chunkSize);
        /*
         * Note: schema and ivCacheSchema are not thread-safe, so it is not safe
         * to show their state here. A concurrent modification exception could
         * easily be thrown. Not synchronizing on these things is currently
         * valued more than observing their contents outside of a debugger.
         */
//        sb.append(",schema="+schema);
//        sb.append(",ivCacheSchema="+ivCacheSchema);
//        sb.append(",optional=" + optional);
//        sb.append(",filter=" + filter);
        if (askVar != null)
            sb.append(",askVar=" + askVar);
        sb.append(",joinVars=" + Arrays.toString(joinVars));
        if (selectVars != null)
            sb.append(",selectVars=" + Arrays.toString(selectVars));
        if (constraints != null)
            sb.append(",constraints=" + Arrays.toString(constraints));
        sb.append(",namespace=" + namespace);
        sb.append(",size=" + getRightSolutionCount());
        sb.append(",considered(left=" + nleftConsidered + ",right="
                + nrightConsidered + ",joins=" + nJoinsConsidered + ")");
        if (joinSet.get() != null)
            sb.append(",joinSetSize=" + getJoinSetSize());
        if (ivCache.get() != null)
            sb.append(",ivCacheSize=" + getIVCacheSize());
        if (blobsCache.get() != null)
            sb.append(",blobCacheSize=" + getBlobsCacheSize());
        sb.append("}");
        
        return sb.toString();
        
    }

    @Override
    public boolean isEmpty() {
        
        return getRightSolutionCount() == 0;
        
    }
    
    @Override
    public long getRightSolutionCount() {

        final HTree htree = getRightSolutions();

        if (htree != null) {

            return htree.getEntryCount();

        }

        return 0L;
        
    }

    private long getJoinSetSize() {

        final HTree htree = getJoinSet();

        if (htree != null) {

            return htree.getEntryCount();

        }

        return 0L;
        
    }

    private long getIVCacheSize() {

        final BTree ndx = ivCache.get();

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

    public JoinTypeEnum getJoinType() {
        
        return joinType;
        
    }
    
    public IVariable<?> getAskVar() {
        
        return askVar;
        
    }
    
    public IVariable<?>[] getJoinVars() {

        return joinVars;
        
    }
    
    public IVariable<?>[] getSelectVars() {
        
        return selectVars;
        
    }
    
    public IConstraint[] getConstraints() {
        
        return constraints;
        
    }
    
    /**
     * Setup the {@link IndexMetadata} for {@link #rightSolutions} or
     * {@link #joinSet}.
     */
    static private IndexMetadata getIndexMetadata(final PipelineOp op) {

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        final int addressBits = op.getProperty(HTreeAnnotations.ADDRESS_BITS,
                HTreeAnnotations.DEFAULT_ADDRESS_BITS);

        final int branchingFactor = 2 ^ addressBits;
        
        final int ratio = 32; // TODO Config/tune.
        
        metadata.setAddressBits(addressBits);

        metadata.setRawRecords(op.getProperty(//
                HTreeAnnotations.RAW_RECORDS,
                HTreeAnnotations.DEFAULT_RAW_RECORDS));

        metadata.setMaxRecLen(op.getProperty(//
                HTreeAnnotations.MAX_RECLEN,
                HTreeAnnotations.DEFAULT_MAX_RECLEN));

        metadata.setWriteRetentionQueueCapacity(op.getProperty(
                IndexAnnotations.WRITE_RETENTION_QUEUE_CAPACITY,
                IndexAnnotations.DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY));

        metadata.setKeyLen(Bytes.SIZEOF_INT); // int32 hash code keys.

        @SuppressWarnings("rawtypes")
        final ITupleSerializer<?, ?> tupleSer = new DefaultTupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_INT),
                new FrontCodedRabaCoder(ratio),// keys : TODO Optimize for int32!
                new SimpleRabaCoder() // vals
        );

        metadata.setTupleSerializer(tupleSer);
        
        return metadata;

    }
    
    /**
     * Setup the {@link IndexMetadata} for {@link #ivCache}.
     * <p>
     * Note: This is basically the same setup as the ID2TERM index.
     */
    private IndexMetadata getIVCacheIndexMetadata(final PipelineOp op) {

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

        // enable raw record support.
        metadata.setRawRecords(true);

        /*
         * Very small RDF values can be inlined into the index, but after that
         * threshold we want to have the values out of line on the backing
         * store.
         * 
         * TODO Tune this and the threshold at which we use the BLOBS index
         * instead.
         */
        metadata.setMaxRecLen(16); 
        
        return metadata;

    }
    
    /**
     * Setup the {@link IndexMetadata} for {@link #blobsCache}.
     * <p>
     * Note: This is basically the same setup as the BLOBS index.
     */
    private IndexMetadata getBlobsCacheIndexMetadata(final PipelineOp op) {

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
     * 
     * @param mmgr
     *            The IMemoryManager which will back the named solution set.
     * @param op
     *            The operator whose annotation will inform construction the
     *            hash index. The {@link HTreeAnnotations} may be specified for
     *            this operator and will control the initialization of the
     *            various {@link HTree} instances.
     * @param joinType
     *            The type of join to be performed.
     * 
     * @see HTreeHashJoinAnnotations
     */
    public HTreeHashJoinUtility(final IMemoryManager mmgr, final PipelineOp op,
            final JoinTypeEnum joinType) {

        if (mmgr == null)
            throw new IllegalArgumentException();

        if (op == null)
            throw new IllegalArgumentException();

        if(joinType == null)
            throw new IllegalArgumentException();
        
//        this.op = op;
        this.joinType = joinType;
        this.optional = joinType == JoinTypeEnum.Optional;
        this.filter = joinType == JoinTypeEnum.Filter;

        // Optional variable used for (NOT) EXISTS.
        this.askVar = (IVariable<?>) op
                .getProperty(HashJoinAnnotations.ASK_VAR);

        // The join variables (required).
        this.joinVars = (IVariable<?>[]) op
                .getRequiredProperty(HashJoinAnnotations.JOIN_VARS);

        // The projected variables (optional and equal to the join variables iff
        // this is a DISTINCT filter).
        this.selectVars = filter ? joinVars : (IVariable<?>[]) op
                .getProperty(JoinAnnotations.SELECT);

        // Initialize the schema with the join variables.
        this.schema = new LinkedHashSet<IVariable<?>>();
        this.schema.addAll(Arrays.asList(joinVars));

        // The set of variables for which materialized values are observed.
        this.ivCacheSchema = filter ? null : new LinkedHashSet<IVariable<?>>();

        // The join constraints (optional).
        this.constraints = (IConstraint[]) op
                .getProperty(JoinAnnotations.CONSTRAINTS);

//        // Iff the join has OPTIONAL semantics.
//        this.optional = optional;
//        
//        // Iff this is a DISTINCT filter.
//        this.filter = filter;
        
//        // ignore unbound variables when used as a DISTINCT filter.
//        this.ignoreUnboundVariables = filter;

        if (!filter) {

            namespace = ((String[]) op
                    .getRequiredProperty(Predicate.Annotations.RELATION_NAME))[0];

            valueFactory = BigdataValueFactoryImpl.getInstance(namespace);

        } else {

            namespace = null;
            
            valueFactory = null;
            
        }
        
        /*
         * This wraps an efficient raw store interface around a child memory
         * manager created from the IMemoryManager which will back the named
         * solution set.
         */
        store = new MemStore(mmgr.createAllocationContext());

        // Will support incremental eviction and persistence.
        rightSolutions.set(HTree.create(store, getIndexMetadata(op)));

        switch (joinType) {
        case Optional:
        case Exists:
        case NotExists:
            // The join set is used to handle optionals.
            joinSet.set(HTree.create(store, getIndexMetadata(op)));
            break;
        }

        /*
         * Setup the IV => BigdataValue mapping. This captures any cached
         * BigdataValue references encountered on IVs. This map does not store
         * duplicate entries for the same IV.
         */
        if (!filter) {

            this.ivCache.set(BTree.create(store, getIVCacheIndexMetadata(op)));

            this.blobsCache.set(BTree
                    .create(store, getBlobsCacheIndexMetadata(op)));

        }

    }

    /**
     * The backing {@link IRawStore}.
     */
    public IRawStore getStore() {
    
        return store;
        
    }
    
    /**
     * Checkpoint the {@link HTree} instance(s) used to buffer the source
     * solutions ({@link #rightSolutions} and {@link #ivCache}) and then
     * re-load the them in a read-only mode from their checkpoint(s). This
     * exposes a view of the {@link HTree} which is safe for concurrent
     * readers.
     */
    public void saveSolutionSet() {

        if (!open.get())
            throw new IllegalStateException();

        checkpointHTree(rightSolutions);

        checkpointBTree(ivCache);
        checkpointBTree(blobsCache);

        /*
         * Note: DO NOT checkpoint the joinSet here. That index is not even
         * written upon until we begin to evaluate the joins, which happens
         * after we checkpoint the source solutions.
         */

    }

    /**
     * Checkpoint the join set (used to buffer the optional solutions).
     * <p>
     * Note: Since we always output the solutions which did not join from a
     * single thread as part of last pass evaluation there is no need to
     * checkpoint the {@link #joinSet}.
     */
    public void checkpointJoinSet() {

        if (!open.get())
            throw new IllegalStateException();

        checkpointHTree(joinSet);

    }

    private void checkpointHTree(final AtomicReference<HTree> ref) {

        final HTree tmp = ref.get();

        if (tmp != null) {

            // Checkpoint the HTree.
            final Checkpoint checkpoint = tmp.writeCheckpoint2();

            final HTree readOnly = HTree.load(store,
                    checkpoint.getCheckpointAddr(), true/* readOnly */);

            // Get a read-only view of the HTree.
            if (!ref.compareAndSet(tmp/* expect */, readOnly)) {

                throw new IllegalStateException();

            }

        }

    }

    private void checkpointBTree(final AtomicReference<BTree> ref) {

        final BTree tmp = ref.get();

        if (tmp != null) {

            // Checkpoint the HTree.
            final Checkpoint checkpoint = tmp.writeCheckpoint2();

            final BTree readOnly = BTree.load(store,
                    checkpoint.getCheckpointAddr(), true/* readOnly */);

            // Get a read-only view of the HTree.
            if (!ref.compareAndSet(tmp/* expect */, readOnly)) {

                throw new IllegalStateException();

            }

        }

    }

    @Override
    public void release() {

        if (open.compareAndSet(true/* expect */, false/* update */)) {
            // Already closed.
            return;
        }

        schema.clear();

        if (ivCacheSchema != null) {

            ivCacheSchema.clear();
            
        }
        
        HTree tmp = rightSolutions.getAndSet(null/* newValue */);

        if (tmp != null) {

            tmp.close();

        }

        tmp = joinSet.getAndSet(null/* newValue */);
        
        if (tmp != null) {

            tmp.close();

        }

        BTree tmp2 = ivCache.getAndSet(null/* newValue */);
        
        if (tmp2 != null) {

            tmp2.close();

        }

        tmp2 = blobsCache.getAndSet(null/* newValue */);
        
        if (tmp2 != null) {

            tmp2.close();

        }

        store.close();

    }

    @Override
    public long acceptSolutions(final ICloseableIterator<IBindingSet[]> itr,
            final BOpStats stats) {

        if (itr == null)
            throw new IllegalArgumentException();
        
        if (stats == null)
            throw new IllegalArgumentException();

        try {

        long naccepted = 0L;
        
        final HTree htree = getRightSolutions();

        final IKeyBuilder keyBuilder = htree.getIndexMetadata().getKeyBuilder();
        
        final Map<IV<?, ?>, BigdataValue> cache = new HashMap<IV<?, ?>, BigdataValue>();
        
        /*
         * Rechunk in order to have a nice fat vector size for ordered inserts.
         */
        final Iterator<IBindingSet[]> it = new Chunkerator<IBindingSet>(
                new Dechunkerator<IBindingSet>(itr), chunkSize,
                IBindingSet.class);

        final AtomicInteger vectorSize = new AtomicInteger();
        while (it.hasNext()) {

            final BS[] a = vector(it.next(), joinVars, null/* selectVars */,
                    false/* ignoreUnboundVariables */, vectorSize);

            final int n = vectorSize.get();
            
//        while (itr.hasNext()) {
//
//            final IBindingSet[] a = itr.next();

            stats.chunksIn.increment();
            stats.unitsIn.add(a.length);

//            for (IBindingSet bset : a) {
//
//                int hashCode = ONE; // default (used iff join is optional).
//                try {
//
//                    hashCode = HTreeHashJoinUtility.hashCode(joinVars, bset,
//                            ignoreUnboundVariables);
//
//                } catch (JoinVariableNotBoundException ex) {
//
//                    if (!optional) {
//                        
//                        // Drop solution;
//
//                        if (log.isTraceEnabled())
//                            log.trace(ex);
//
//                        continue;
//
//                    }
//                    
//                }
            
            for (int i = 0; i < n; i++) {

                final BS tmp = a[i];

                // Update the schema.
                updateSchema(tmp.bset);

                // Encode the key.
                final byte[] key = keyBuilder.reset().append(tmp.hashCode)
                        .getKey();

                // Encode the solution.
                final byte[] val = encodeSolution(keyBuilder, schema, cache,
                        tmp.bset);

                // Insert binding set under hash code for that key.
                htree.insert(key, val);

            }

            naccepted += a.length;

            updateIVCache(cache, ivCache.get(), blobsCache.get());

        }
        
        return naccepted;
        
        } catch(Throwable t) {
            throw launderThrowable(t);
        }

    }

    @Override
    public long filterSolutions(final ICloseableIterator<IBindingSet[]> itr,
            final BOpStats stats, final IBuffer<IBindingSet> sink) {

        if (itr == null)
            throw new IllegalArgumentException();
        
        if (stats == null)
            throw new IllegalArgumentException();

        try {
        
        long naccepted = 0L;
        
        final HTree htree = getRightSolutions();

//        final Map<IV<?, ?>, BigdataValue> cache = new HashMap<IV<?, ?>, BigdataValue>();
        
        final IKeyBuilder keyBuilder = htree.getIndexMetadata().getKeyBuilder();

        /*
         * Rechunk in order to have a nice fat vector size for ordered inserts.
         */
        final Iterator<IBindingSet[]> it = new Chunkerator<IBindingSet>(
                new Dechunkerator<IBindingSet>(itr), chunkSize,
                IBindingSet.class);

        final AtomicInteger vectorSize = new AtomicInteger();
        while (it.hasNext()) {

            final BS[] a = vector(it.next(), joinVars, selectVars,
                    true/* ignoreUnboundVariables */, vectorSize);
  
            final int n = vectorSize.get();

//        while (itr.hasNext()) {
//
//            final IBindingSet[] a = itr.next();

            stats.chunksIn.increment();
            stats.unitsIn.add(a.length);

//            for (IBindingSet bset : a) {
//                
//                // Note: Must *only* consider the projected variables.
//                bset = bset.copy(selectVars);
//
//                int hashCode = ONE; // default (used iff join is optional).
//                try {
//
//                    hashCode = HTreeHashJoinUtility.hashCode(joinVars, bset,
//                            ignoreUnboundVariables);
//
//                } catch (JoinVariableNotBoundException ex) {
//
//                    // Exception is not thrown when used as a filter.
//                    throw new AssertionError();
//                    
//                }

            for (int i = 0; i < n; i++) {

                final BS tmp = a[i];
                
                // Update the schema.
                updateSchema(tmp.bset);

                // Encode the key.
                final byte[] key = keyBuilder.reset().append(tmp.hashCode)
                        .getKey();

                // Encode the solution.
                final byte[] val = encodeSolution(keyBuilder, schema,
                        null/* cache */, tmp.bset);

                /*
                 * Search the hash index for a match.
                 */
                boolean found = false;
                
                final ITupleIterator<?> titr = htree.lookupAll(key);

                while(titr.hasNext()) {
                
                    final ITuple<?> t = titr.next();
                    
                    final ByteArrayBuffer tb = t.getValueBuffer();

                    if (0 == BytesUtil.compareBytesWithLenAndOffset(
                            0/* aoff */, val.length/* alen */, val,//
                            0/* boff */, tb.limit()/* blen */, tb.array()/* b */
                    )) {

                        found = true;

                        break;
                        
                    }
                    
                }

                if (!found) {
                
                    // Add to the hash index.
                    htree.insert(key, val);

                    // Write onto the sink.
                    sink.add(tmp.bset);

                    naccepted++;
                    
                }
                
            }

//            updateIVCache(cache, ivCache.get());

        }
        
        return naccepted;

        } catch(Throwable t) {
            throw launderThrowable(t);
        }

    }
    
    /**
     * Build up the schema. This includes all observed variables, not just those
     * declared in {@link #joinVars}
     * 
     * @param bset An observed binding set.
     */
    private void updateSchema(final IBindingSet bset) {

        @SuppressWarnings("rawtypes")
        final Iterator<IVariable> vitr = bset.vars();
        
        while (vitr.hasNext()) {
        
            schema.add(vitr.next());
            
        }

    }

    /**
     * Transfer any {@link IV} to {@link BigdataValue} mappings to the ivCache.
     * 
     * @param cache
     *            A JVM cache.
     * @param ivCache
     *            A persistence capable cache for non-{@link BlobIV}s {@link IV}
     *            s having a cached {@link BigdataValue} reference.
     * @param blobsCache
     *            A persistence capable cache for {@link BlobIV}s having a
     *            cached {@link BigdataValue} reference.
     */
    private void updateIVCache(final Map<IV<?, ?>, BigdataValue> cache,
            final BTree ivCache, final BTree blobsCache) {

        // Used to serialize RDF {@link Value}s.
        final Id2TermTupleSerializer tupSer = (Id2TermTupleSerializer) ivCache
                .getIndexMetadata().getTupleSerializer();

        for (Map.Entry<IV<?, ?>, BigdataValue> e : cache.entrySet()) {

            final IV<?, ?> iv = e.getKey();

            final BigdataValue value = e.getValue();

            if (iv instanceof BlobIV<?>) {

                final BlobsIndexHelper h = new BlobsIndexHelper();
                
                final IKeyBuilder keyBuilder = h.newKeyBuilder();

                final byte[] baseKey = h.makePrefixKey(keyBuilder.reset(),
                        value);

                final byte[] val = valueFactory.getValueSerializer().serialize(
                        value);

                h.resolveOrAddValue(blobsCache, false/* readOnly */,
                        keyBuilder, baseKey, val, null/* tmp */, null/* bucketSize */);
                
            } else {

                final byte[] key = tupSer.serializeKey(iv);

                if (!ivCache.contains(key)) {

                    ivCache.insert(key, tupSer.serializeVal(value));

                }

            }

        }

    }

    /**
     * Encode the solution as an IV[].
     * 
     * @param keyBuilder
     *            Where to format the solution.
     * @param schema
     *            The schema, which specifies the order in which the variable
     *            bindings will appear. If a variable declared in the schema is
     *            not bound in the solution, then it will be represented by a
     *            {@link TermId#NullIV}.
     * @param cache
     *            Any cached {@link BigdataValue}s on the {@link IV}s are
     *            inserted into this map (optional since the cache is not 
     *            used when we are filtering DISTINCT solutions).
     * @param bset
     *            The solution to be encoded.
     * @return The encoded solution.
     */
    private byte[] encodeSolution(final IKeyBuilder keyBuilder,
            final LinkedHashSet<IVariable<?>> schema,
            final Map<IV<?, ?>, BigdataValue> cache, final IBindingSet bset) {

        keyBuilder.reset();
        final Iterator<IVariable<?>> vitr = schema.iterator();
        while (vitr.hasNext()) {
            final IVariable<?> v = vitr.next();
            @SuppressWarnings("unchecked")
            final IConstant<IV<?, ?>> c = bset.get(v);
            if (c == null) {
                IVUtility.encode(keyBuilder, TermId.NullIV);
            } else {
                final IV<?, ?> iv = c.get();
                IVUtility.encode(keyBuilder, iv);
                if (iv.hasValue() && !filter) {
                    ivCacheSchema.add(v);
                    cache.put(iv, iv.getValue());
                }
            }
        }
        return keyBuilder.getKey();
        
    }

    /**
     * Decode a solution from an encoded {@link IV}[].
     * <p>
     * Note: The {@link IV#getValue() cached value} is NOT part of the encoded
     * data and will NOT be present in the returned {@link IBindingSet}. The
     * cached {@link BigdataValue} (if any) must be unified by consulting the
     * {@link #ivCache}.
     * 
     * @param t
     *            A tuple whose value is an encoded {@link IV}[].
     *            
     * @return The decoded {@link IBindingSet}.
     */
    private IBindingSet decodeSolution(final ITuple<?> t) {
        
        final ByteArrayBuffer b = t.getValueBuffer();
        
        return decodeSolution(b.array(), 0, b.limit());

    }

    /**
     * Decode a solution from an encoded {@link IV}[].
     * <p>
     * Note: The {@link IV#getValue() cached value} is NOT part of the encoded
     * data and will NOT be present in the returned {@link IBindingSet}. The
     * cached {@link BigdataValue} (if any) must be unified by consulting the
     * {@link #ivCache}.
     * 
     * @param val
     *            The encoded IV[].
     * @param fromOffset
     *            The starting offset.
     * @param toOffset
     *            The byte beyond the end of the encoded data.
     * 
     * @return The decoded {@link IBindingSet}.
     */
    private IBindingSet decodeSolution(final byte[] val, final int fromOffset,
            final int toOffset) {

        final IBindingSet rightSolution = new ListBindingSet();

        final IV<?, ?>[] ivs = IVUtility.decodeAll(val, fromOffset, toOffset);

        int i = 0;

        for (IVariable<?> v : schema) {

            if (i == ivs.length) {
                /*
                 * This solution does not include all variables which were
                 * eventually discovered to be part of the schema.
                 */
                break;
            }

            final IV<?, ?> iv = ivs[i++];
            
            if (iv == null) {
            
                // Not bound.
                continue;
                
            }

            rightSolution.set(v, new Constant<IV<?, ?>>(iv));

        }

        return rightSolution;

    }

    /**
     * Glue class for hash code and binding set used when the hash code is for
     * just the join variables rather than the entire binding set.
     */
    private static class BS implements Comparable<BS> {

        final private int hashCode;

        final private IBindingSet bset;

        BS(final int hashCode, final IBindingSet bset) {
            this.hashCode = hashCode;
            this.bset = bset;
        }

        @Override
        public int compareTo(final BS o) {
            if (this.hashCode < o.hashCode)
                return -1;
            if (this.hashCode > o.hashCode)
                return 1;
            return 0;
        }
        
        public String toString() {
            return getClass().getName() + "{hashCode=" + hashCode + ",bset="
                    + bset + "}";
        }
        
    }
    
    /**
     * Glue class for hash code and encoded binding set used when we already
     * have the binding set encoded.
     */
    private static class BS2 implements Comparable<BS2> {

        final private int hashCode;

        final private byte[] value;

        BS2(final int hashCode, final byte[] value) {
            this.hashCode = hashCode;
            this.value = value;
        }

        @Override
        public int compareTo(final BS2 o) {
            if (this.hashCode < o.hashCode)
                return -1;
            if (this.hashCode > o.hashCode)
                return 1;
            return 0;
        }
        
        public String toString() {
            return getClass().getName() + "{hashCode=" + hashCode + ",value="
                    + BytesUtil.toString(value) + "}";
        }
        
    }
    
    @Override
    public void hashJoin(//
            final ICloseableIterator<IBindingSet> leftItr,//
            final IBuffer<IBindingSet> outputBuffer//
            ) {

        hashJoin2(leftItr, outputBuffer, constraints);
        
    }

    @Override
    public void hashJoin2(//
            final ICloseableIterator<IBindingSet> leftItr,//
            final IBuffer<IBindingSet> outputBuffer,//
            final IConstraint[] constraints//
            ) {

        try {

            final HTree rightSolutions = this.getRightSolutions();

            if (log.isInfoEnabled()) {
                log.info("rightSolutions: #nnodes="
                        + rightSolutions.getNodeCount() + ",#leaves="
                        + rightSolutions.getLeafCount() + ",#entries="
                        + rightSolutions.getEntryCount());
            }
            
            final IKeyBuilder keyBuilder = rightSolutions.getIndexMetadata()
                    .getKeyBuilder();

            final BTree ivCache = this.ivCache.get();
            
            final BTree blobsCache = this.blobsCache.get();
            
            final Iterator<IBindingSet[]> it = new Chunkerator<IBindingSet>(
                    leftItr, chunkSize, IBindingSet.class);

            // true iff there are no join variables.
            final boolean noJoinVars = joinVars.length == 0;
            
            final AtomicInteger vectorSize = new AtomicInteger();
            while (it.hasNext()) {

                final BS[] a = vector(it.next(), joinVars,
                        null/* selectVars */,
                        false/* ignoreUnboundVariables */, vectorSize);
                
                final int n = vectorSize.get();

                nleftConsidered.add(n);
                
                int fromIndex = 0;

                while (fromIndex < n) {

                    /*
                     * Figure out how many left solutions in the current chunk
                     * have the same hash code. We will use the same iterator
                     * over the right solutions for that hash code against the
                     * HTree.
                     */
                    
                    // The next hash code to be processed.
                    final int hashCode = a[fromIndex].hashCode;

                    // scan for the first hash code which is different.
                    int toIndex = n; // assume upper bound.
                    for (int i = fromIndex + 1; i < n; i++) {
                        if (a[i].hashCode != hashCode) {
                            toIndex = i;
                            break;
                        }
                    }
                    // #of left solutions having the same hash code.
                    final int bucketSize = toIndex - fromIndex;

                    if (log.isTraceEnabled())
                        log.trace("hashCode=" + hashCode + ": #left="
                                + bucketSize + ", firstLeft=" + a[fromIndex]);

                    /*
                     * Note: all source solutions in [fromIndex:toIndex) have
                     * the same hash code. They will be vectored together.
                     */
                    // All solutions which join for that collision bucket
                    final LinkedList<BS2> joined;
                    switch (joinType) {
                    case Optional:
                    case Exists:
                    case NotExists:
                        joined = new LinkedList<BS2>();
                        break;
                    default:
                        joined = null;
                        break;
                    }
                    // #of solutions which join for that collision bucket.
                    int njoined = 0;
                    // #of solutions which did not join for that collision bucket.
                    int nrejected = 0;
                    {

                        final byte[] key = keyBuilder.reset().append(hashCode).getKey();
                        
                        // visit all source solutions having the same hash code
                        final ITupleIterator<?> titr = rightSolutions
                                .lookupAll(key);

                        long sameHashCodeCount = 0;
                        
                        while (titr.hasNext()) {

                            sameHashCodeCount++;
                            
                            final ITuple<?> t = titr.next();

                            /*
                             * Note: The map entries must be the full source
                             * binding set, not just the join variables, even
                             * though the key and equality in the key is defined
                             * in terms of just the join variables.
                             * 
                             * Note: Solutions which have the same hash code but
                             * whose bindings are inconsistent will be rejected
                             * by bind() below.
                             */
                            final IBindingSet rightSolution = decodeSolution(t);

                            nrightConsidered.increment();

                            for (int i = fromIndex; i < toIndex; i++) {

                                final IBindingSet leftSolution = a[i].bset;
                                
                                // Join.
                                final IBindingSet outSolution = BOpContext
                                        .bind(leftSolution, rightSolution,
                                                constraints,
                                                selectVars);

                                nJoinsConsidered.increment();

                                if (noJoinVars&&
                                        nJoinsConsidered.get() == noJoinVarsLimit) {

                                    if (nleftConsidered.get() > 1
                                            && nrightConsidered.get() > 1) {

                                        throw new UnconstrainedJoinException();

                                    }

                                }

                                if (outSolution == null) {
                                    
                                    nrejected++;
                                    
                                    if (log.isTraceEnabled())
                                        log.trace("Does not join"//
                                                +": hashCode="+ hashCode//
                                                + ", sameHashCodeCount="+ sameHashCodeCount//
                                                + ", #left=" + bucketSize//
                                                + ", #joined=" + njoined//
                                                + ", #rejected=" + nrejected//
                                                + ", left=" + leftSolution//
                                                + ", right=" + rightSolution//
                                                );

                                } else {

                                    njoined++;

                                    if (log.isDebugEnabled())
                                        log.debug("JOIN"//
                                            + ": hashCode=" + hashCode//
                                            + ", sameHashCodeCount="+ sameHashCodeCount//
                                            + ", #left="+ bucketSize//
                                            + ", #joined=" + njoined//
                                            + ", #rejected=" + nrejected//
                                            + ", solution=" + outSolution//
                                            );
                                
                                }

                                switch(joinType) {
                                case Normal:
                                case Optional: {
                                    if (outSolution == null) {
                                        // Join failed.
                                        continue;
                                    }

                                    // Resolve against ivCache.
                                    resolveCachedValues(ivCache, blobsCache,
                                            outSolution);
                                    
                                    // Output this solution.
                                    outputBuffer.add(outSolution);

                                    if (optional) {
                                        // Accumulate solutions to vector into
                                        // the joinSet.
                                        joined.add(new BS2(rightSolution
                                                .hashCode(), t.getValue()));
                                    }
                                    
                                    break;
                                }
                                case Exists: {
                                    /*
                                     * The right solution is output iff there is
                                     * at least one left solution which joins
                                     * with that right solution. Each right
                                     * solution is output at most one time. This
                                     * amounts to outputting the joinSet after
                                     * we have run the entire join. As long as
                                     * the joinSet does not allow duplicates it
                                     * will be contain the solutions that we
                                     * want.
                                     */
                                    if (outSolution != null) {
                                        // Accumulate solutions to vector into
                                        // the joinSet.
                                        joined.add(new BS2(rightSolution
                                                .hashCode(), t.getValue()));
                                    }
                                    break;
                                }
                                case NotExists: {
                                    /*
                                     * The right solution is output iff there
                                     * does not exist any left solution which
                                     * joins with that right solution. This
                                     * basically an optional join where the
                                     * solutions which join are not output.
                                     */
                                    if (outSolution != null) {
                                        // Accumulate solutions to vector into
                                        // the joinSet.
                                        joined.add(new BS2(rightSolution
                                                .hashCode(), t.getValue()));
                                    }
                                    break;
                                }
                                default:
                                    throw new AssertionError();
                                }

                            } // next left in the same bucket.

                        } // next rightSolution with the same hash code.

                        if (joined != null && !joined.isEmpty()) {
                            /*
                             * Vector the inserts into the [joinSet].
                             */
                            final BS2[] a2 = joined.toArray(new BS2[njoined]);
                            Arrays.sort(a2, 0, njoined);
                            for (int i = 0; i < njoined; i++) {
                                final BS2 tmp = a2[i];
                                saveInJoinSet(tmp.hashCode, tmp.value);
                            }
                        }

                    } // end block of leftSolutions having the same hash code.

                    fromIndex = toIndex;
                    
                } // next slice of source solutions with the same hash code.

            } // while(itr.hasNext()

        } catch(Throwable t) {

            throw launderThrowable(t);
            
        } finally {

            leftItr.close();

        }

    } // handleJoin

    /**
     * Vector a chunk of solutions.
     * 
     * @param leftSolutions
     *            The solutions.
     * @param joinVars
     *            The variables on which the hash code will be computed.
     * @param selectVars
     *            When non-<code>null</code>, all other variables are dropped.
     *            (This is used when we are modeling a DISTINCT solutions filter
     *            since we need to drop anything which is not part of the
     *            DISTINCT variables list.)
     * @param ignoreUnboundVariables
     *            When <code>true</code>, an unbound variable will not cause a
     *            {@link JoinVariableNotBoundException} to be thrown.
     * @param vectorSize
     *            The vector size (set by side-effect).
     * 
     * @return The vectored chunk of solutions ordered by hash code.
     */
    private BS[] vector(final IBindingSet[] leftSolutions,
            final IVariable<?>[] joinVars,
            final IVariable<?>[] selectVars,
            final boolean ignoreUnboundVariables,
            final AtomicInteger vectorSize) {

        final BS[] a = new BS[leftSolutions.length];

        int n = 0; // The #of non-dropped source solutions.

        for (int i = 0; i < a.length; i++) {

            /*
             * Note: If this is a DISINCT FILTER, then we need to drop the
             * variables which are not being considered immediately. Those
             * variables MUST NOT participate in the computed hash code.
             */

            final IBindingSet bset = selectVars == null ? leftSolutions[i]
                    : leftSolutions[i].copy(selectVars);

            // Compute hash code from bindings on the join vars.
            int hashCode = ONE;
            try {

                hashCode = HTreeHashJoinUtility.hashCode(joinVars,
                        bset, ignoreUnboundVariables);
                
            } catch (JoinVariableNotBoundException ex) {

                if (!optional) {// Drop solution
                
                    if (log.isTraceEnabled())
                        log.trace(ex);
                    
                    continue;
                    
                }
                
            }
            
            a[n++] = new BS(hashCode, bset);
            
        }

        /*
         * Sort by the computed hash code. This not only orders the accesses
         * into the HTree but it also allows us to handle all source solutions
         * which have the same hash code with a single scan of the appropriate
         * collision bucket in the HTree.
         */
        Arrays.sort(a, 0, n);
        
        // Indicate the actual vector size to the caller via a side-effect.
        vectorSize.set(n);
        
        return a;
        
    }

    /**
     * Add to 2nd hash tree of all solutions which join.
     * <p>
     * Note: the hash key is based on the entire solution (not just the join
     * variables). The values are the full encoded {@link IBindingSet}.
     */
    void saveInJoinSet(final int joinSetHashCode, final byte[] val) {

        final HTree joinSet = this.getJoinSet();

        if (true) {
            
            /*
             * Do not insert if there is already an entry for that solution in
             * the join set.
             * 
             * Note: EXISTS depends on this to have the correct cardinality. If
             * EXISTS allows duplicate solutions into the join set then having
             * multiple left solutions which satisify the EXISTS filter will
             * cause multiple copies of the right solution to be output! If you
             * change the joinSet to allow duplicates, then it MUST NOT allow
             * them for EXISTS!
             */

            final IKeyBuilder keyBuilder = joinSet.getIndexMetadata()
                    .getKeyBuilder();

            final byte[] key = keyBuilder.reset().append(joinSetHashCode)
                    .getKey();

            // visit all joinSet solutions having the same hash code
            final ITupleIterator<?> xitr = joinSet.lookupAll(key);

            while (xitr.hasNext()) {

                final ITuple<?> xt = xitr.next();

                final ByteArrayBuffer b = xt.getValueBuffer();

                if (0 == BytesUtil.compareBytesWithLenAndOffset(0/* aoff */,
                        val.length/* alen */, val/* a */, 0/* boff */,
                        b.limit()/* blen */, b.array())) {

                    return;

                }

            }
            
        }

        joinSet.insert(joinSetHashCode, val);

    }
    
    @Override
    public void outputOptionals(final IBuffer<IBindingSet> outputBuffer) {

        try {

            @SuppressWarnings({ "rawtypes", "unchecked" })
            final Constant f = askVar == null ? null : new Constant(
                    XSDBooleanIV.FALSE);

            if (log.isInfoEnabled()) {
                final HTree htree = this.getRightSolutions();
                log.info("rightSolutions: #nnodes=" + htree.getNodeCount()
                        + ",#leaves=" + htree.getLeafCount() + ",#entries="
                        + htree.getEntryCount());

                final HTree joinSet = this.getJoinSet();
                log.info("joinSet: #nnodes=" + joinSet.getNodeCount()
                        + ",#leaves=" + joinSet.getLeafCount() + ",#entries="
                        + joinSet.getEntryCount());
            }

            final HTree joinSet = getJoinSet();

            final IKeyBuilder keyBuilder = joinSet.getIndexMetadata()
                    .getKeyBuilder();

            final BTree ivCache = this.ivCache.get();

            final BTree blobsCache = this.blobsCache.get();

            // Visit all source solutions.
            final ITupleIterator<?> sitr = getRightSolutions().rangeIterator();

            while (sitr.hasNext()) {

                final ITuple<?> t = sitr.next();

                IBindingSet rightSolution = decodeSolution(t);

                // The hash code is based on the entire solution for the
                // joinSet.
                final int hashCode = rightSolution.hashCode();

                final byte[] key = keyBuilder.reset().append(hashCode).getKey();

                // Probe the join set for this source solution.
                final ITupleIterator<?> jitr = joinSet.lookupAll(key);

                boolean found = false;
                while (jitr.hasNext()) {

                    // Note: Compare full solutions, not just the hash code!

                    final ITuple<?> xt = jitr.next();

                    final ByteArrayBuffer tb = t.getValueBuffer();

                    final ByteArrayBuffer xb = xt.getValueBuffer();

                    if (0 == BytesUtil.compareBytesWithLenAndOffset(
                            0/* aoff */, tb.limit()/* alen */,
                            tb.array()/* a */, 0/* boff */,
                            xb.limit()/* blen */, xb.array())) {

                        found = true;

                        break;

                    }

                }

                if (!found) {

                    /*
                     * Since the source solution is not in the join set, output
                     * it as an optional solution.
                     */

                    if (selectVars != null) {// && selectVars.length > 0) {

                        // Only output the projected variables.
                        rightSolution = rightSolution.copy(selectVars);

                    }

                    resolveCachedValues(ivCache, blobsCache, rightSolution);

                    if (f != null) {

                        if (selectVars == null)
                            rightSolution = rightSolution.clone();
                        
                        rightSolution.set( askVar, f);

                    }

                    outputBuffer.add(rightSolution);

                }

            }

        } catch (Throwable t) {

            throw launderThrowable(t);
        }
        
    } // outputOptionals.

    @Override
    public void outputSolutions(final IBuffer<IBindingSet> out) {

        try {

            final HTree rightSolutions = getRightSolutions();

            if (log.isInfoEnabled()) {
                log.info("rightSolutions: #nnodes="
                        + rightSolutions.getNodeCount() + ",#leaves="
                        + rightSolutions.getLeafCount() + ",#entries="
                        + rightSolutions.getEntryCount());
            }

            final BTree ivCache = this.ivCache.get();

            final BTree blobsCache = this.blobsCache.get();

            // source.
            final ITupleIterator<?> solutionsIterator = rightSolutions
                    .rangeIterator();

            while (solutionsIterator.hasNext()) {

                final ITuple<?> t = solutionsIterator.next();

                IBindingSet bset = decodeSolution(t);

                if (selectVars != null) {

                    // Drop variables which are not projected.
                    bset = bset.copy(selectVars);

                }

                resolveCachedValues(ivCache, blobsCache, bset);

                out.add(bset);

            }

        } catch (Throwable t) {

            throw launderThrowable(t);

        }

    } // outputSolutions

    @Override
    public void outputJoinSet(final IBuffer<IBindingSet> out) {

        try {

            @SuppressWarnings({ "rawtypes", "unchecked" })
            final Constant t = askVar == null ? null : new Constant(
                    XSDBooleanIV.TRUE);
            
            final HTree joinSet = getJoinSet();

            if (log.isInfoEnabled()) {
                log.info("joinSet: #nnodes="
                        + joinSet.getNodeCount() + ",#leaves="
                        + joinSet.getLeafCount() + ",#entries="
                        + joinSet.getEntryCount());
            }

            final BTree ivCache = this.ivCache.get();

            final BTree blobsCache = this.blobsCache.get();

            // source.
            final ITupleIterator<?> solutionsIterator = joinSet
                    .rangeIterator();

            while (solutionsIterator.hasNext()) {

                IBindingSet bset = decodeSolution(solutionsIterator.next());

                if (selectVars != null) {

                    // Drop variables which are not projected.
                    bset = bset.copy(selectVars);

                }

                if (t != null) {

                    if (selectVars == null)
                        bset = bset.clone();

                    bset.set(askVar, t);

                }

                resolveCachedValues(ivCache, blobsCache, bset);

                out.add(bset);

            }

        } catch (Throwable t) {

            throw launderThrowable(t);

        }

    } // outputJoinSet

    /**
     * Resolve any {@link IV}s in the solution for which there are cached
     * {@link BigdataValue}s to those values.
     * 
     * @param bset
     *            A solution having {@link IV}s which need to be reunited with
     *            their cached {@link BigdataValue}s.
     * 
     *            TODO If we vectored this operation it would substantially
     *            reduce its costs. We would have to collect up a bunch of
     *            solutions which needed resolution, then collect up the IVs
     *            which do not have cached values for variables which might have
     *            values in the ivCache. We would then sort the IVs and do a
     *            vectored resolution against the ivCache. Finally, the
     *            solutions could be output in a chunk with their resolved
     *            Values.
     */
    @SuppressWarnings("rawtypes")
    private void resolveCachedValues(final BTree ivCache,
            final BTree blobsCache, final IBindingSet bset) {

        if (ivCache.getEntryCount() == 0L && blobsCache.getEntryCount() == 0L) {
            // Nothing materialized.
            return;
        }
        
        final Id2TermTupleSerializer tupSer = (Id2TermTupleSerializer) ivCache
                .getIndexMetadata().getTupleSerializer();
        
        final IKeyBuilder keyBuilder = tupSer.getKeyBuilder();

        final Tuple ivCacheTuple = new Tuple(ivCache, IRangeQuery.KEYS
                | IRangeQuery.VALS);

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

                final BlobIV<?> blobIV = (BlobIV<?>)iv; 
            
                // TODO factor out the ctor when we do the resolution of IVs.
                final byte[] val = new BlobsIndexHelper().lookup(blobsCache,
                        blobIV, keyBuilder);

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

            } else {

                keyBuilder.reset();

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

    /**
     * Return <code>true</code> iff the tuple contains a solution having the
     * same hash code.
     * 
     * @param key
     *            The hash code (as an unsigned byte[]).
     * @param t
     *            The tuple.
     * 
     * @return <code>true</code> iff the tuple has the same hash code.
     */
    static private boolean sameHashCode(byte[] key, ITuple<?> t) {

        final ByteArrayBuffer b = t.getKeyBuffer();

        return 0 == BytesUtil.compareBytesWithLenAndOffset(0/* aoff */,
                key.length/* alen */, key, 0/* boff */, b.limit()/* blen */,
                b.array());

    }

    /**
     * Copy the key (an unsigned byte[] representing an int32 hash code) from
     * the tuple into the caller's the buffer.
     * 
     * @param t
     *            The tuple.
     * @param key
     *            The caller's buffer.
     */
    static private void copyKey(ITuple<?> t, byte[] key) {
        
        final ByteArrayBuffer b = t.getKeyBuffer();
        
        System.arraycopy(b.array()/* src */, 0/* srcPos */, key/* dest */,
                0/* destPos */, key.length);
    
    }

    /**
     * Synchronize the other sources (this just verifies that the join is
     * possible given the hash code).
     * 
     * @param key
     *            A key from the first source (aka a hash code).
     * @param all
     *            All of the sources.
     * @param optional
     *            <code>true</code> iff the join is optional.
     * @return iff a join is possible for the given key (aka hash code).
     */
    static private boolean canJoin(final byte[] key,
            final HTreeHashJoinUtility[] all, final boolean optional) {
        if (optional) {
            // The join can always proceed. No need to check.
            return true;
        }
        for (int i = 1; i < all.length; i++) {
            final boolean found = all[i].getRightSolutions().contains(key);
            if (!found) {
                // No join is possible against this source.
                return false;
            }
        }
        // The join is possible against all sources.
        return true;
    }

    /**
     * Synchronize the iterators for the other sources.
     * 
     * @param key
     *            A key from the first source (aka a hash code).
     * @param all
     *            All of the sources.
     * @param oitr
     *            An array with one iterator per source. The first index
     *            position is unused and will always be <code>null</code>. The
     *            other positions will never be <code>null</code>, but the
     *            iterator in a given position might not visit any tuples if the
     *            join is optional. (For a required join, each iterator will
     *            always visit at least one tuple.)
     * @param index
     *            The index of the first source iterator to be reset. The
     *            iterator for that source and all remaining sources will be
     *            reset to read on the given key.
     * @param optional
     *            <code>true</code> iff the join is optional.
     */
    static private void synchronizeOtherSources(//
            final byte[] key,//
            final HTreeHashJoinUtility[] all,//
            final ITupleIterator<?>[] oitr,//
            final int index,//
            final boolean optional) {

        if (index < 1)
            throw new IllegalArgumentException();
        
        if (index >= all.length)
            throw new IllegalArgumentException();
        
        for (int i = index; i < all.length; i++) {
        
            oitr[i] = all[i].getRightSolutions().lookupAll(key);
            
        }
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: For the {@link HTree}, the entries are in key order. Those keys are
     * hash codes computed from the solutions using the join variables. Since
     * the keys are hash codes and not the join variable bindings, each hash
     * code identifies a collision bucket from the perspective of the merge join
     * algorithm. Of course, from the perspective of the {@link HTree} those
     * solutions are just consequective tuples readily identified using
     * {@link HTree#lookupAll(int)}.
     * 
     * FIXME Either always project everything or raise [select] into a parameter
     * for this method. We DO NOT want to only project whatever was projected by
     * the first source.
     */
    @Override
    public void mergeJoin(
			//
			final IHashJoinUtility[] others,
			final IBuffer<IBindingSet> outputBuffer,
			final IConstraint[] constraints, final boolean optional) {

        try {
        
		/*
		 * Validate arguments.
		 */

		if (others == null)
			throw new IllegalArgumentException();

		if (others.length == 0)
			throw new IllegalArgumentException();

		if (outputBuffer == null)
			throw new IllegalArgumentException();
		
		final HTreeHashJoinUtility[] all = new HTreeHashJoinUtility[others.length + 1];
		{
			all[0] = this;
			for (int i = 0; i < others.length; i++) {
				final HTreeHashJoinUtility o = (HTreeHashJoinUtility) others[i];
				if (o == null)
					throw new IllegalArgumentException();
                if (!Arrays.equals(joinVars, o.joinVars)) {
					// Must have the same join variables.
					throw new IllegalArgumentException();
				}
				all[i + 1] = o;
			}

		}

		if (isEmpty()) {
			// NOP
			return;
		}

		/*
		 * Combine constraints for each source with the given constraints.
		 */
		final IConstraint[] c = JVMHashJoinUtility.combineConstraints(
				constraints, all);

		/*
		 * MERGE JOIN
		 * 
		 * We follow the iterator on the first source. For each hash code which
		 * it visits, we synchronize iterators against the remaining sources. If
		 * the join is optional, then the iterator will be null for a source
		 * which does not have that hash code. Otherwise false is returned if
		 * any source lacks tuples for the current hash code.
		 */
		long njoined = 0, nrejected = 0;
		{

            // if optional then if there are no solutions don't try and
            // expand further, we need a place-holder object
            final Object NULL_VALUE = "NULL";

            final int nsources = all.length;
            
            final ITuple<?>[] set = new ITuple<?>[nsources + 1];

            // Visit everything in the first source.
			final Striterator sols0 = new Striterator(all[0].getRightSolutions()
					.rangeIterator());
			{

			    sols0.addFilter(new Visitor() {
                    private static final long serialVersionUID = 1L;
                    /**
                     * Set the tuple for the first source each time it advances.
                     * 
                     * @param obj
                     *            The tuple.
                     */
                    @Override
                    protected void visit(final Object obj) {
                        set[0] = (ITuple<?>) obj;
                    }

                });

                // now add in Expanders and Visitors for each remaining source.
                for (int i = 1; i < nsources; i++) {
                    // Final variables used inside inner classes.
                    final int slot = i;
                    final HTree thisTree = all[slot].getRightSolutions();

					sols0.addFilter(new Expander() {
	                    private static final long serialVersionUID = 1L;
                        /**
                         * Expansion pattern gives solutions for source @ slot.
                         * 
                         * @param obj
                         *            The tuple in set[slot-1].
                         */
						@Override
						protected Iterator<?> expand(final Object obj) {
							if (obj == NULL_VALUE) {
								assert optional;
								return new SingleValueIterator(NULL_VALUE);
							}
							// Sync itr for this source to key for prior src.
							final byte[] key2 = ((ITuple<?>) obj).getKey();
							final ITupleIterator<?> ret = thisTree.lookupAll(key2);
							if (optional && !ret.hasNext()) {
                                /*
                                 * Nothing for that key from this source. Return
                                 * a single marker value so we can proceed to
                                 * the remaining sources rather than halting.
                                 */
								return new SingleValueIterator(NULL_VALUE);
							} else {
                                /*
                                 * Iterator visiting solutions from this source
                                 * for the current key in the prior source.
                                 */
								return ret;
							}
						}

					});

					sols0.addFilter(new Visitor() {
	                    private static final long serialVersionUID = 1L;
                        /**
                         * Assign tuple to set[slot].
                         * 
                         * Note: If [obj==NULL_VALUE] then no solutions for that
                         * slot.
                         */
						@Override
						protected void visit(final Object obj) {
							set[slot] = (ITuple<?>) (obj == NULL_VALUE ? null : obj);
						}

					});
			    }
			} // end of striterator setup.

            /*
             * This will visit after all expansions. That means that we will
             * observe the cross product of the solutions from the remaining
             * sources having the same hash for each from the first source.
             * 
             * Each time we visit something, set[] is the tuple[] which
             * describes a specific set of solutions from that cross product.
             * 
             * TODO Lift out the decodeSolution() for all slots into the
             * expander pattern.
             */
			while (sols0.hasNext()) {
				sols0.next();
				IBindingSet in = all[0].decodeSolution(set[0]);
				// FIXME apply constraint to source[0] (JVM version also).
				for (int i = 1; i < set.length; i++) {

					// See if the solutions join.
                    final IBindingSet left = in;
					if (set[i] != null) {
						final IBindingSet right = all[i].decodeSolution(set[i]); 
						in = BOpContext.bind(//
								left,// 
								right,// 
								c,// TODO constraint[][]
								null//
								);
					}

					if (in == null) {
//					    if(optional) {
//					        in = left;
//					        continue;
//					    }
						// Join failed.
						break;
					}

				}
				// Accept this binding set.
				if (in != null) {
					if (log.isDebugEnabled())
						log.debug("Output solution: " + in);
					// FIXME Vector resolution of ivCache.
                    resolveCachedValues(ivCache.get(), blobsCache.get(), in);
					outputBuffer.add(in);
				}

//				// now clear set!
//				for (int i = 1; i < set.length; i++) {
//					set[i] = null;
//				}
				
			}

		}
		
        } catch(Throwable t) {
            throw launderThrowable(t);
        }

	}    
    
//    public void oldmergeJoin(//
//            final IHashJoinUtility[] others,
//            final IBuffer<IBindingSet> outputBuffer,
//            final IConstraint[] constraints,
//            final boolean optional) {
//
//        /*
//         * Validate arguments.
//         */
//
//        if (others == null)
//            throw new IllegalArgumentException();
//
//        if (others.length == 0)
//            throw new IllegalArgumentException();
//
//        if (outputBuffer == null)
//            throw new IllegalArgumentException();
//
//        final HTreeHashJoinUtility[] all = new HTreeHashJoinUtility[others.length + 1];
//        {
//            all[0] = this;
//            for (int i = 0; i < others.length; i++) {
//                final HTreeHashJoinUtility o = (HTreeHashJoinUtility) others[i];
//                if (o == null)
//                    throw new IllegalArgumentException();
//                if (!this.joinVars.equals(o.joinVars)) {
//                    // Must have the same join variables.
//                    throw new IllegalArgumentException();
//                }
//                all[i + 1] = o;
//            }
//
//        }
//
//        if(isEmpty()) {
//            // NOP
//            return;
//        }
//        
//        /*
//         * Combine constraints for each source with the given constraints.
//         */
//        final IConstraint[] c = JVMHashJoinUtility.combineConstraints(
//                constraints, all);
//
//        /*
//         * Synchronize each source.
//         * 
//         * We follow the iterator on the first source. For each hash code which
//         * it visits, we synchronize iterators against the remaining sources. If
//         * the join is optional, then the iterator will be null for a source
//         * which does not have that hash code. Otherwise false is returned if
//         * any source lacks tuples for the current hash code.
//         */
//        long njoined = 0, nrejected = 0;
//        final ITupleIterator<?> itr = all[0].getRightSolutions()
//                .rangeIterator();
//        {
//            /*
//             * Note: Initialized by the first tuple we visit. Updated each time
//             * we finish joining that solution against all solutions having the
//             * same hash code for all of the other sources.
//             */
////            int hashCode = 0;
////            ITuple<?> t = null;
//            final byte[] key = new byte[Bytes.SIZEOF_INT];
//            final ITupleIterator<?>[] oitr = new ITupleIterator[all.length];
//            ITuple<?> t = itr.next(); // Start with the first solution.
//            copyKey(t,key); // Make a copy of that key.
//            while (true) {
//                // Synchronize the other sources on the same key (aka hashCode)
//                log.error("Considering firstSource: "+decodeSolution(t));
//                if (!canJoin(key, all, optional)) {
//                    // Scan until we reach a tuple with a different key.
//                    boolean found = false;
//                    while (itr.hasNext() && !found) {
//                        t = itr.next(); // scan forward.
//                        if (!sameHashCode(key, t)) {
//                            // We have reached a solution from the first source
//                            // having a different hash code.
//                            found = true;
//                        }
//                        log.error("Skipping firstSource: "+decodeSolution(t));
//                    }
//                    if (!found) {
//                        // The first source is exhausted.
//                        return;
//                    }
//                    copyKey(t,key); // Make a copy of that key.
//                    // Attempt to resynchronize.
//                    continue;
//                }
//                /*
//                 * MERGE JOIN
//                 * 
//                 * Note: At this point we have a solution which might join with
//                 * the other sources (they all have the same hash code). We can
//                 * now decode the solution from the first source and ...
//                 * 
//                 * FIXME Must vector the resolution of the ivCache / blobsCache
//                 * for the output solutions.
//                 */
//                
//                // Setup each source.
//                synchronizeOtherSources(key, all, oitr, 1/* index */, optional);
//
//                if(true) {
//                    /*
//                     * FIXME This just demonstrates the join for the simple case
//                     * of a single 'other' source.
//                     */
//                    final IBindingSet leftSolution = all[0].decodeSolution(t);
//                    final ITupleIterator<?> aitr = oitr[1];
//                    while(aitr.hasNext()) {
//                        final ITuple<?> rightTuple = aitr.next();
//                        final IBindingSet rightSolution = all[1].decodeSolution(rightTuple);
//                        // Join.
//                        final IBindingSet outSolution = BOpContext
//                                .bind(leftSolution, rightSolution,
//                                        c,
//                                        null//selectVars
//                                        );
//
//                        if (outSolution == null) {
//                            if(optional) {
//                                outputBuffer.add(leftSolution);
//            					System.err.println("Output solution: " + leftSolution);
//                                if (log.isDebugEnabled())
//                                    log.debug("OPT : #joined="
//                                            + njoined + ", #rejected="
//                                            + nrejected + ", solution="
//                                            + outSolution);
//                            } else {
//                                nrejected++;
//                                if (log.isTraceEnabled())
//                                    log.trace("FAIL: #joined=" + njoined
//                                            + ", #rejected=" + nrejected
//                                            + ", leftSolution=" + leftSolution
//                                            + ", rightSolution="
//                                            + rightSolution);
//                            }
//                        } else {
//                            njoined++;
//                            outputBuffer.add(outSolution);
//        					System.err.println("Output solution: " + outSolution);
//                            if (log.isDebugEnabled())
//                                log.debug("JOIN: #joined="
//                                        + njoined + ", #rejected="
//                                        + nrejected + ", solution="
//                                        + outSolution);
//                        }
//                    }
//                }
//                
//                // Decode the solution.
////                final IBindingSet b = decodeSolution(t);
////                mergeJoin(b, oitr, c, optional);
//
//                if (!itr.hasNext()) {
//                    // The first source is exhausted.
//                    return;
//                }
//                // Skip to the next tuple from that source.
//                itr.next();
//                copyKey(t, key); // Make a copy of that key.
//
//            }
//
//        }
//
//    }

    /**
     * Adds metadata about the {@link IHashJoinUtility} state to the stack
     * trace.
     * 
     * @param t
     *            The thrown error.
     * 
     * @return The laundered exception.
     * 
     * @throws Exception
     */
    private RuntimeException launderThrowable(final Throwable t) {

        final String msg = "cause=" + t + ", state=" + toString();

        log.error(msg);

        return new RuntimeException(msg, t);

    }

}
