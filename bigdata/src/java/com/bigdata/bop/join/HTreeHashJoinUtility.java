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
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BloomFilterFactory;
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
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.htree.HTree;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.lexicon.Id2TermTupleSerializer;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.rwstore.sector.MemStore;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.Chunkerator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Utility methods to support hash joins using an {@link HTree}. There are two
 * core steps, plus one additional step if the join is optional. The hash join
 * logically has a <em>Left Hand Side</em> (LHS) and a Right Hand Side (RHS).
 * The RHS is used to build up a hash index which is then probed for each LHS
 * solution. The LHS is generally an access path scan, which is done once. A
 * hash join therefore provides an alternative to a nested index join in which
 * we visit the access path once, probing the hash index for solutions which
 * join.
 * <dl>
 * <dt>Accept solutions</dt>
 * <dd>This step builds the hash index, also known as the RHS (Right Hand Side).
 * </dd>
 * <dt>hash join</dt>
 * <dd>The hash join considers each left solution in turn and outputs solutions
 * which join. If optionals are required, this step also builds an hash index
 * (the <i>joinSet</i>) over the right solutions which did join.</dd>
 * <dt>Output optionals</dt>
 * <dd>The RHS hash index is scanned and the <i>joinSet</i> is probed to
 * identify right solutions which did not join with any left solution. Those
 * solutions are output as "optionals".</dd>
 * </dl>
 * 
 * TODO Javadoc review.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HTreeHashJoinUtility {

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
     * 
     * @return The hash code.
     * 
     * @throws JoinVariableNotBoundException
     *             if there is no binding for a join variable.
     */
    private static int hashCode(final IVariable<?>[] joinVars,
            final IBindingSet bset) throws JoinVariableNotBoundException {

        int h = ONE;

        for (IVariable<?> v : joinVars) {

            final IConstant<?> c = bset.get(v);

            if (c == null) {

                // Reject any solution which does not have a binding for a join
                // variable.

                throw new JoinVariableNotBoundException(v.getName());
                
            }

//            final int ch = (int) c.hashCode() ^ (c.hashCode() >>> 32);
//
//            h = 31 * h + ch;
            h ^= c.hashCode();
            
        }
        
        if (log.isTraceEnabled())
            log.trace("hashCode=" + h + ", joinVars="
                    + Arrays.toString(joinVars) + " : " + bset);

        return h;

    }

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
    public final LinkedHashSet<IVariable<?>> schema;
    
    /**
     * <code>true</code> iff the join is OPTIONAL.
     * 
     * @see JoinAnnotations#
     * 
     */
    public final boolean optional;
    
    /**
     * The join variables.
     * 
     * @see HashJoinAnnotations#JOIN_VARS
     */
    public final IVariable<?>[] joinVars;

    /**
     * The variables to be retained (optional, all variables are retained if
     * not specified).
     */
    public final IVariable<?>[] selectVars;

    /**
     * The join constraints (optional).
     */
    public final IConstraint[] constraints;

    /**
     * The backing {@link IRawStore}.
     */
    private final IRawStore store;
    public IRawStore getStore() {
        return store;
    }
    
    /**
     * The hash index. The keys are int32 hash codes built from the join
     * variables. The values are an {@link IV}[], similar to the encoding in
     * the statement indices. The mapping from the index positions in the
     * {@link IV}s to the variables is specified by the {@link #schema}.
     */
    private final AtomicReference<HTree> rightSolutions = new AtomicReference<HTree>();
    public HTree getRightSolutions() { // TODO Does not need to be visible once hash join utility method signature is changed.
        return rightSolutions.get();
    }
    
    /**
     * The set of distinct source solutions which joined. This set is
     * maintained iff the join is optional and is <code>null</code>
     * otherwise.
     */
    private final AtomicReference<HTree> joinSet = new AtomicReference<HTree>();
    public HTree getJoinSet() { // TODO Does not need to be visible once hash join utility method signature is changed.
        return joinSet.get();
    }
    
    /**
     * The {@link IV}:{@link BigdataValue} mapping. This captures any cached
     * BigdataValue references encountered on {@link IV}s. This map does not
     * store duplicate entries for the same {@link IV}.
     * <p>
     * Note: This is precisely the same mapping we use for the ID2TERM
     * index.
     * 
     * TODO Either we need a blobCache as well or we need to rematerialize
     * blobs in the rightSolutions after a hash join.
     */
    private final AtomicReference<BTree> ivCache = new AtomicReference<BTree>();

    /**
     * <code>true</code> until the state is discarded by {@link #release()}.
     */
    private final AtomicBoolean open = new AtomicBoolean(true);
    
    /**
     * Setup the {@link IndexMetadata} for {@link #rightSolutions} or
     * {@link #joinSet}.
     */
    static private IndexMetadata getIndexMetadata(final PipelineOp op) {

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        final int addressBits = op.getProperty(HTreeAnnotations.ADDRESS_BITS,
                HTreeAnnotations.DEFAULT_ADDRESS_BITS);

        final int branchingFactor = 2 ^ addressBits;
        
        final int ratio = 32;
        
        metadata.setAddressBits(addressBits);

        metadata.setRawRecords(op.getProperty(//
                HTreeAnnotations.RAW_RECORDS,
                HTreeAnnotations.DEFAULT_RAW_RECORDS));

        metadata.setMaxRecLen(op.getProperty(//
                HTreeAnnotations.MAX_RECLEN,
                HTreeAnnotations.DEFAULT_MAX_RECLEN));

        metadata.setWriteRetentionQueueCapacity(4000);// FIXME CONFIG

        metadata.setKeyLen(Bytes.SIZEOF_INT); // int32 hash code keys.

        @SuppressWarnings("rawtypes")
        final ITupleSerializer<?, ?> tupleSer = new DefaultTupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_INT),
                new FrontCodedRabaCoder(ratio),// keys : TODO Optimize for int32!
                new SimpleRabaCoder() // vals : FIXME IV[] coder (stmt indices).
        );

        metadata.setTupleSerializer(tupleSer);
        
        return metadata;

    }
    
    /**
     * Setup the {@link IndexMetadata} for {@link #ivCache}.
     * <p>
     * Note: This is basically the same setup as the ID2TERM index.
     */
    static private IndexMetadata getIVCacheIndexMetadata(final PipelineOp op) {

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        final int branchingFactor = 256;// TODO Config.
        
        final int ratio = 32;
        
        metadata.setBranchingFactor(branchingFactor);

        metadata.setWriteRetentionQueueCapacity(4000);// FIXME CONFIG

        final String namespace = ((String[]) op
                .getRequiredProperty(Predicate.Annotations.RELATION_NAME))[0];

        final BigdataValueFactory valueFactory = BigdataValueFactoryImpl
                .getInstance(namespace);

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
     * 
     * @param mmgr
     *            The IMemoryManager which will back the named solution set.
     * @param op
     *            The operator whose annotation will inform construction the
     *            hash index. The {@link HTreeAnnotations} may be specified
     *            for this operator and will control the initialization of
     *            the various {@link HTree} instances.
     * @param optional
     *            <code>true</code> iff the join is optional.
     * 
     * @see HTreeAnnotations
     */
    public HTreeHashJoinUtility(final IMemoryManager mmgr,
            final PipelineOp op, final boolean optional) {

        if (mmgr == null)
            throw new IllegalArgumentException();

        if (op == null)
            throw new IllegalArgumentException();

        // The join variables (required).
        this.joinVars = (IVariable<?>[]) op
                .getRequiredProperty(HashJoinAnnotations.JOIN_VARS);

        // The projected variables (optional).
        this.selectVars = (IVariable<?>[]) op
                .getProperty(JoinAnnotations.SELECT);
        
        // Initialize the schema with the join variables.
        this.schema = new LinkedHashSet<IVariable<?>>();
        this.schema.addAll(Arrays.asList(joinVars));

        // The join constraints (optional).
        this.constraints = (IConstraint[]) op
                .getProperty(JoinAnnotations.CONSTRAINTS);

        // Iff the join has OPTIONAL semantics.
        this.optional = optional;
        
        /*
         * This wraps an efficient raw store interface around a child memory
         * manager created from the IMemoryManager which will back the named
         * solution set.
         */
        store = new MemStore(mmgr.createAllocationContext());

        // Will support incremental eviction and persistence.
        rightSolutions.set(HTree.create(store, getIndexMetadata(op)));

        if (optional) {

            // The join set is used to handle optionals.
            joinSet.set(HTree.create(store, getIndexMetadata(op)));

        }

        /*
         * Setup the IV => BigdataValue mapping. This captures any cached
         * BigdataValue references encountered on IVs. This map does not
         * store duplicate entries for the same IV.
         */
        this.ivCache.set(BTree.create(store, getIVCacheIndexMetadata(op)));

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

    /**
     * Discard the {@link HTree} data.
     */
    public void release() {

        if (open.compareAndSet(true/* expect */, false/* update */)) {
            // Already closed.
            return;
        }

        schema.clear();

        HTree tmp = rightSolutions.getAndSet(null/* newValue */);

        if (tmp != null) {

            tmp.close();

        }

        tmp = joinSet.getAndSet(null/* newValue */);
        
        if (tmp != null) {

            tmp.close();

        }

        final BTree tmp2 = ivCache.getAndSet(null/* newValue */);
        
        if (tmp2 != null) {

            tmp2.close();

        }

        store.close();

    }

    /**
     * Buffer solutions on an {@link HTree}.
     * <p>
     * When <code>optional:=true</code>, solutions which do not have a binding
     * for one or more of the join variables will be inserted into the hash
     * index anyway using <code>hashCode:=1</code>. This allows the solutions to
     * be discovered when we scan the hash index and the set of solutions which
     * did join to identify the optional solutions.
     * 
     * @param itr
     *            The source from which the solutions will be drained.
     * @param stats
     *            The statistics to be updated as the solutions are buffered on
     *            the hash index.
     * @param state
     *            The {@link HTreeHashJoinState}.
     * 
     * @return The #of solutions that were buffered.
     * 
     *         FIXME Does anything actually rely on the
     *         {@link JoinVariableNotBoundException}? It would seem that this
     *         exception could only be thrown if the joinvars[] was incorrectly
     *         formulated as it should only include "known bound" variables. (I
     *         think that this is related to incorrectly passing along empty
     *         solutions for named subquery hash joins.)
     */
    public long acceptSolutions(final ICloseableIterator<IBindingSet[]> itr,
            final BOpStats stats) {

        if (itr == null)
            throw new IllegalArgumentException();
        
        if (stats == null)
            throw new IllegalArgumentException();

        long n = 0L;
        
        final HTree htree = rightSolutions.get();

        final Map<IV<?, ?>, BigdataValue> cache = new HashMap<IV<?, ?>, BigdataValue>();
        
        final IKeyBuilder keyBuilder = new KeyBuilder(16);
        
        while (itr.hasNext()) {

            final IBindingSet[] a = itr.next();

            stats.chunksIn.increment();
            stats.unitsIn.add(a.length);

            for (IBindingSet bset : a) {

                int hashCode = ONE; // default (used iff join is optional).
                try {

                    hashCode = HTreeHashJoinUtility.hashCode(joinVars, bset);

                } catch (JoinVariableNotBoundException ex) {

                    if (!optional) {
                        
                        // Drop solution;

                        if (log.isDebugEnabled())
                            log.debug(ex);

                        continue;

                    }
                    
                }

                // Update the schema.
                updateSchema(bset);

                // Insert binding set under hash code for that key.
                htree.insert(hashCode,
                        encodeSolution(keyBuilder, schema, cache, bset));
                
            }

            n += a.length;

            updateIVCache(cache, ivCache.get());

        }
        
        return n;

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
     *            A persistence capable cache.
     */
    private static void updateIVCache(final Map<IV<?, ?>, BigdataValue> cache,
            final BTree ivCache) {

        // Used to serialize RDF {@link Value}s.
        final Id2TermTupleSerializer tupSer = (Id2TermTupleSerializer) ivCache
                .getIndexMetadata().getTupleSerializer();

        for (Map.Entry<IV<?, ?>, BigdataValue> e : cache.entrySet()) {

            final IV<?, ?> iv = e.getKey();

            final byte[] key = tupSer.serializeKey(iv);

            if (!ivCache.contains(key)) {

                final BigdataValue value = e.getValue();

                ivCache.insert(key, tupSer.serializeVal(value));

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
     *            inserted into this map.
     * @param bset
     *            The solution to be encoded.
     * @return The encoded solution.
     */
    private static byte[] encodeSolution(final IKeyBuilder keyBuilder,
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
                if (iv.hasValue()) {
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
     * Do a hash join between a stream of source solutions (left) and a hash
     * index (right). For each left solution, the hash index (right) is probed
     * for possible matches (solutions whose as-bound values for the join
     * variables produce the same hash code). Possible matches are tested for
     * consistency and the constraints (if any) are applied. Solutions which
     * join are written on the caller's buffer.
     * 
     * @param leftItr
     *            A stream of solutions to be joined against the hash index
     *            (left).
     * @param outputBuffer
     *            Where to write the solutions which join.
     * @param joinVars
     *            The join variables (required). Solutions which do not have
     *            bindings for the join variables will NOT join. If an empty
     *            array is specified then all solutions will have a hash code of
     *            ONE (1) and the join will degrade to a full N x M comparison.
     *            Only solutions which are consistent with one another and with
     *            the optional constraints will actually join, but the join will
     *            do much more work to find those solutions.
     * @param selectVars
     *            The variables to be retained (optional, all variables are
     *            retained if not specified).
     * @param constraints
     *            Constraints on the solutions (optional, may be
     *            <code>null</code>).
     * @param rightSolutions
     *            A hash index already built over some multiset of solutions
     *            (right).
     * @param joinSet
     *            A hash index to be populated with (right) solutions which
     *            join. This is only required when <code>optional:=true</code>.
     *            The resulting hash index is used to detect the optional
     *            solutions in a separate step.
     * @param optional
     *            <code>true</code> iff the optional solutions must also be
     *            output, in which case the <i>joinSet</i> is required and will
     *            be populated by this method.
     * @param leftIsPipeline
     *            <code>true</code> iff <i>left</i> is a solution from upstream
     *            in the query pipeline. Otherwise, <i>right</i> is the upstream
     *            solution.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/233 (Inline access
     *      path).
     * 
     * @see BOpContext#solutions(com.bigdata.striterator.IChunkedIterator,
     *      com.bigdata.bop.IPredicate, IVariable[], BaseJoinStats)
     */
    public void hashJoin(//
            final ICloseableIterator<IBindingSet> leftItr,//
            final IBuffer<IBindingSet> outputBuffer,//
            final boolean leftIsPipeline//
            ) {

        try {

            if (log.isDebugEnabled()) {
                final HTree htree = this.rightSolutions.get();
                log.debug("rightSolutions: #nnodes=" + htree.getNodeCount()
                        + ",#leaves=" + htree.getLeafCount() + ",#entries="
                        + htree.getEntryCount());
            }
            
            // TODO parameter from operator annotations.
            final int chunkSize = ChunkedWrappedIterator.DEFAULT_CHUNK_SIZE;

            final HTree rightSolutions = this.rightSolutions.get();
            
            final Iterator<IBindingSet[]> it = new Chunkerator<IBindingSet>(
                    leftItr, chunkSize, IBindingSet.class);

            final AtomicInteger vectorSize = new AtomicInteger();
            while (it.hasNext()) {

                final BS[] a = vector(it.next(), vectorSize);
                
                final int n = vectorSize.get();

                int fromIndex = 0;

                while (fromIndex < n) {

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
                     * the same hash code.
                     */
                    {

                        // visit all source solutions having the same hash code
                        @SuppressWarnings("unchecked")
                        final ITupleIterator<IBindingSet> titr = rightSolutions
                                .lookupAll(hashCode);

                        while (titr.hasNext()) {

                            final ITuple<IBindingSet> t = titr.next();

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

                            for (int i = fromIndex; i < toIndex; i++) {

                                final IBindingSet leftSolution = a[i].bset;
                                
                                // Join.
                                final IBindingSet outSolution = BOpContext
                                        .bind(leftSolution, rightSolution,
                                                leftIsPipeline,
                                                constraints,
                                                selectVars);

                                if (outSolution == null) {

                                    if (log.isTraceEnabled())
                                        log.trace("Does not join: hashCode="
                                                + hashCode + ", left="
                                                + leftSolution + ", right="
                                                + rightSolution);

                                    // Join failed.
                                    continue;

                                }

                                if (log.isDebugEnabled())
                                    log.debug("Output solution: " + outSolution);

                                // Accept this binding set.
                                outputBuffer.add(outSolution);

                                if (optional) {

                                    // TODO Vector these inserts.
                                    saveInJoinSet(rightSolution.hashCode(),
                                            t.getValue());

                                }

                            }

                        } // next rightSolution with the same hash code.

                    } // end block of leftSolutions having the same hash code.

                    fromIndex = toIndex;
                    
                } // next slice of source solutions with the same hash code.

            } // while(itr.hasNext()

        } finally {

            leftItr.close();

        }

    } // handleJoin

    /**
     * Vector a chunk of solutions.
     * 
     * @param leftSolutions
     *            The solutions.
     * @param vectorSize
     *            The vector size (set by side-effect).
     *            
     * @return The vectored chunk of solutions ordered by hash code.
     */
    private BS[] vector(final IBindingSet[] leftSolutions,
            final AtomicInteger vectorSize) {

        final BS[] a = new BS[leftSolutions.length];

        int n = 0; // The #of non-dropped source solutions.

        for (int i = 0; i < a.length; i++) {

            // Compute hash code from bindings on the join vars.
            int hashCode;
            try {
                hashCode = HTreeHashJoinUtility.hashCode(joinVars,
                        leftSolutions[i]);
                a[n++] = new BS(hashCode, leftSolutions[i]);
            } catch (JoinVariableNotBoundException ex) {
                // Drop solution
                if (log.isDebugEnabled())
                    log.debug(ex);
                continue;
            }
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
     * Note: the hash key is based on the entire solution for this htree.
     * 
     * TODO This code does allows duplicate solutions into the joinSet. There is
     * code below which does not permit this. We might need to refactor that
     * code into a utility method (together with how we obtain the appropriate
     * hash code) and use it to filter out duplicates for the joinSet and/or the
     * rightSolutions.
     * 
     * FIXME Use the same encoding here that we do for IVs in
     * {@link #rightSolutions} compute the hash code based on ALL IVs, not just
     * those in the {@link #joinVars}. (It would help if
     * {@link #hashCode(IVariable[], IBindingSet)} accepted a <code>null</code>
     * for the join variables for this use case (no, just do an alternative
     * version which is simpler).
     */
    void saveInJoinSet(final int hashCode, final byte[] val) {

        joinSet.get().insert(hashCode, val);

//            final int joinSetHashCode = rightSolution.hashCode();
//            
//            // visit all joinSet solutions having the same hash code
//            @SuppressWarnings("unchecked")
//            final ITupleIterator<IBindingSet> xitr = joinSet
//                    .lookupAll(joinSetHashCode);
    //
//            boolean found = false;
//            while (!found && xitr.hasNext() ) {
    //
//                final ITuple<IBindingSet> xt = xitr.next();
    //
//                /*
//                 * Note: The map entries must be the full source
//                 * binding set, not just the join variables, even
//                 * though the key and equality in the key is defined
//                 * in terms of just the join variables.
//                 * 
//                 * Note: Solutions which have the same hash code but
//                 * whose bindings are inconsistent will be rejected
//                 * by bind() below.
//                 */
//                final IBindingSet aSolution = xt.getObject();
//                
//                if (rightSolution.equals(aSolution)) {
    //
//                    if (log.isDebugEnabled())
//                        log.debug("Solution already in joinSet: "
//                                + rightSolution);
//                    
//                    found = true;
//                    
//                    break;
//                    
//                }
    //
//            }
    //
//            if (!found) {
//             
//                joinSet.insert(rightSolution);
//                
//                if (log.isDebugEnabled())
//                    log.debug("Solution added to joinSet: "
//                            + rightSolution);
//                
//            }
       }
    
    /**
     * Identify and output the optional solutions. Optionals are identified
     * using a <i>joinSet</i> containing each right solution which joined with
     * at least one left solution. The total set of right solutions is then
     * scanned once. For each right solution, we probe the <i>joinSet</i>. If
     * the right solution did not join, then it is output now as an optional
     * join.
     * 
     * @param outputBuffer
     *            Where to write the optional solutions.
     * @param rightSolutions
     *            The hash index (right).
     * @param joinSet
     *            The set of distinct right solutions which joined. This set is
     *            maintained iff the join is optional.
     * @param selectVars
     *            The variables to be retained (optional, all variables are
     *            retained if not specified).
     */
    public void outputOptionals(final IBuffer<IBindingSet> outputBuffer) {

        if (log.isInfoEnabled()) {
            final HTree htree = this.rightSolutions.get();
            log.info("rightSolutions: #nnodes=" + htree.getNodeCount()
                    + ",#leaves=" + htree.getLeafCount() + ",#entries="
                    + htree.getEntryCount());
            
            final HTree joinSet = this.joinSet.get();
            log.info("joinSet: #nnodes=" + joinSet.getNodeCount() + ",#leaves="
                    + joinSet.getLeafCount() + ",#entries="
                    + joinSet.getEntryCount());
        }

        // Visit all source solutions.
        @SuppressWarnings("unchecked")
        final ITupleIterator<IBindingSet> sitr = rightSolutions.get()
                .rangeIterator();
        
        while(sitr.hasNext()) {
            
            final ITuple<IBindingSet> t = sitr.next();
            
            IBindingSet rightSolution = decodeSolution(t);

            // The hash code is based on the entire solution for the joinSet.
            final int hashCode = rightSolution.hashCode();
            
            // Probe the join set for this source solution.
            @SuppressWarnings("unchecked")
            final ITupleIterator<IBindingSet> jitr = joinSet.get()
                    .lookupAll(hashCode);

            if (!jitr.hasNext()) {

                /*
                 * Since the source solution is not in the join set, output it
                 * as an optional solution.
                 */

                if (selectVars != null) {// && selectVars.length > 0) {
                
                    // Only output the projected variables.
                    rightSolution = rightSolution.copy(selectVars);
                    
                }

                resolveCachedValues(rightSolution);
                
                outputBuffer.add(rightSolution);

            }

        }

    } // handleOptionals.

    /**
     * Output the solutions buffered in the hash index.
     * 
     * @param out
     *            Where to write the solutions.
     */
    public void outputSolutions(final IBuffer<IBindingSet> out) {

        if (log.isInfoEnabled()) {
            final HTree htree = rightSolutions.get();
            log.info("rightSolutions: #nnodes=" + htree.getNodeCount()
                    + ",#leaves=" + htree.getLeafCount() + ",#entries="
                    + htree.getEntryCount());
        }
        
        // source.
        @SuppressWarnings("unchecked")
        final ITupleIterator<IBindingSet> solutionsIterator = rightSolutions
                .get().rangeIterator();

        while (solutionsIterator.hasNext()) {

            final ITuple<IBindingSet> t = solutionsIterator.next();

            IBindingSet bset = decodeSolution(t);

            if (selectVars != null) {

                // Drop variables which are not projected.
                bset = bset.copy(selectVars);

            }

            resolveCachedValues(bset);
            
            out.add(bset);

        }

    }

    /**
     * Resolve any {@link IV}s in the solution for which there are cached
     * {@link BigdataValue}s to those values.
     * 
     * @param bset
     * 
     *            TODO This could be expensive. We could use an {@link HTree}
     *            for this as easily, however we might have similar lookup
     *            costs. The only way to real reduce those costs with a scalable
     *            data structure is to vector the lookups, which means vectoring
     *            inside of the routines to output the solutions or the optional
     *            solutions. An estimate of the cost imposed by the resolution
     *            of the cached IVs could be provided by using a JVM collection
     *            for the ivCache.
     */
    @SuppressWarnings("rawtypes")
    private void resolveCachedValues(final IBindingSet bset) {

        final BTree ivCache = this.ivCache.get();

        final Id2TermTupleSerializer tupSer = (Id2TermTupleSerializer) ivCache
                .getIndexMetadata().getTupleSerializer();
        
        final IKeyBuilder keyBuilder = tupSer.getKeyBuilder();

        final Tuple t = new Tuple(ivCache, IRangeQuery.KEYS|IRangeQuery.VALS);

        final Iterator<Map.Entry<IVariable, IConstant>> itr = bset.iterator();

        while (itr.hasNext()) {

            final Map.Entry<IVariable, IConstant> e = itr.next();

            final IV iv = (IV) e.getValue().get();

            if (iv.hasValue()) {
                // Already cached.
                continue;
            }
            
            keyBuilder.reset();

            IVUtility.encode(keyBuilder, iv);
            
            final byte[] key = keyBuilder.getKey();

            if (ivCache.lookup(key, t) == null) {

                continue;

            }
            
            final BigdataValue value = tupSer.deserialize(t);
            
            iv.setValue(value);
            
        }

    }

}
