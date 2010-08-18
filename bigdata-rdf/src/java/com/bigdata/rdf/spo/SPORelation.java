/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 21, 2008
 */

package com.bigdata.rdf.spo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.Var;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BloomFilterFactory;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.filter.FilterConstructor;
import com.bigdata.btree.filter.TupleFilter;
import com.bigdata.btree.isolation.IConflictResolver;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.btree.proc.LongAggregator;
import com.bigdata.btree.raba.codec.EmptyRabaValueCoder;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLock;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.lexicon.ITermIVFilter;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.JustIndexWriteProc.WriteJustificationsProcConstructor;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.relation.AbstractRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.AbstractSolutionBuffer.InsertSolutionBuffer;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.EmptyChunkedIterator;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.striterator.IKeyOrder;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * The {@link SPORelation} handles all things related to the indices
 * representing the triples stored in the database. Statements are first
 * converted to term identifiers using the {@link LexiconRelation} and then
 * inserted into the statement indices in parallel. There is one statement index
 * for each of the three possible access paths for a triple store. The key is
 * formed from the corresponding permutation of the subject, predicate, and
 * object, e.g., {s,p,o}, {p,o,s}, and {o,s,p} for triples or {s,p,o,c}, etc for
 * quads. The statement type (inferred, axiom, or explicit) and the optional
 * statement identifier are stored under the key. All state for a statement is
 * replicated in each of the statement indices.
 * 
 *  * @todo integration with package providing magic set rewrites of rules in order
 *       to test whether or not a statement is still provable when it is
 *       retracted during TM. this will reduce the cost of loading data, since
 *       much of that is writing the justifications index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPORelation extends AbstractRelation<ISPO> {

    protected static final transient Logger log = Logger
            .getLogger(SPORelation.class);

    private final Set<String> indexNames;

    private final int keyArity;

    /**
     * The arity of the key for the statement indices: <code>3</code> is a
     * triple store, with or without statement identifiers; <code>4</code> is a
     * quad store, which does not support statement identifiers as the 4th
     * position of the (s,p,o,c) is interpreted as context and located in the
     * B+Tree statement index key rather than the value associated with the key.
     */
    public int getKeyArity() {
        
        return keyArity;
        
    }

    /**
     * Hard references for the possible statement indices. The index into the
     * array is {@link SPOKeyOrder#index()}.
     */
    private final IIndex[] indices;
    
    /** Hard reference to the justifications index iff used. */
    private volatile IIndex just;

    /**
     * Constant for the {@link SPORelation} namespace component.
     * <p>
     * Note: To obtain the fully qualified name of an index in the
     * {@link SPORelation} you need to append a "." to the relation's namespace,
     * then this constant, then a "." and then the local name of the index.
     * 
     * @see AbstractRelation#getFQN(IKeyOrder)
     */
    public static final String NAME_SPO_RELATION = "spo";
    
    private static final transient String NAME_JUST = "JUST";
    
    /**
     * This is used to conditionally enable the logic to retract justifications
     * when the corresponding statements is retracted.
     */
    final public boolean justify;

    /**
     * This is used to conditionally disable all but a single statement index
     * (aka access path).
     */
    final public boolean oneAccessPath;

    /**
     * <code>true</code> iff the SPO index will maintain a bloom filter.
     * 
     * @see Options#BLOOM_FILTER
     */
    final protected boolean bloomFilter;
    
    /**
     * When <code>true</code> the database will support statement identifiers.
     * A statement identifier is a unique 64-bit integer taken from the same
     * space as the term identifiers and which uniquely identifiers a statement
     * in the database regardless of the graph in which that statement appears.
     * The purpose of statement identifiers is to allow statements about
     * statements without recourse to RDF style reification.
     */
    final public boolean statementIdentifiers;

    /**
     * When <code>true</code> the database will support statement identifiers.
     * <p>
     * A statement identifier is a unique 64-bit integer taken from the same
     * space as the term identifiers and which uniquely identifiers a statement
     * in the database regardless of the graph in which that statement appears.
     * The purpose of statement identifiers is to allow statements about
     * statements without recourse to RDF style reification.
     * <p>
     * Only explicit statements will have a statement identifier. Statements
     * made about statements using their statement identifiers will
     * automatically be retracted if a statement they describe is retracted (a
     * micro form of truth maintenance that is always enabled when statement
     * identifiers are enabled).
     */
    public boolean getStatementIdentifiers() {
        
        return statementIdentifiers;
        
    }

    public SPORelation(final IIndexManager indexManager,
            final String namespace, final Long timestamp,
            final Properties properties) {

        super(indexManager, namespace, timestamp, properties);
        
        /*
         * Reads off the property for the inference engine that tells us whether
         * or not the justification index is being used. This is used to
         * conditionally enable the logic to retract justifications when the
         * corresponding statements is retracted.
         */

        this.justify = Boolean.parseBoolean(getProperty(
                AbstractTripleStore.Options.JUSTIFY,
                AbstractTripleStore.Options.DEFAULT_JUSTIFY));

        this.oneAccessPath = Boolean.parseBoolean(getProperty(
                AbstractTripleStore.Options.ONE_ACCESS_PATH,
                AbstractTripleStore.Options.DEFAULT_ONE_ACCESS_PATH));

        this.statementIdentifiers = Boolean.parseBoolean(getProperty(
                AbstractTripleStore.Options.STATEMENT_IDENTIFIERS,
                AbstractTripleStore.Options.DEFAULT_STATEMENT_IDENTIFIERS));

        this.keyArity = Boolean.valueOf(getProperty(
                AbstractTripleStore.Options.QUADS,
                AbstractTripleStore.Options.DEFAULT_QUADS)) ? 4 : 3;

        if (statementIdentifiers && keyArity == 4) {

            throw new UnsupportedOperationException(
                    AbstractTripleStore.Options.QUADS
                            + " does not support the provenance mode ("
                            + AbstractTripleStore.Options.STATEMENT_IDENTIFIERS
                            + ")");

        }

        this.bloomFilter = Boolean.parseBoolean(getProperty(
                AbstractTripleStore.Options.BLOOM_FILTER,
                AbstractTripleStore.Options.DEFAULT_BLOOM_FILTER));

        // declare the various indices.
        {
         
            final Set<String> set = new HashSet<String>();

            if (keyArity == 3) {

                // three indices for a triple store and the have ids in [0:2].
                this.indices = new IIndex[3];
                
                if (oneAccessPath) {

                    set.add(getFQN(SPOKeyOrder.SPO));

                } else {

                    set.add(getFQN(SPOKeyOrder.SPO));

                    set.add(getFQN(SPOKeyOrder.POS));

                    set.add(getFQN(SPOKeyOrder.OSP));

                }

            } else {
                
                // six indices for a quad store w/ ids in [3:8].
                this.indices = new IIndex[SPOKeyOrder.MAX_INDEX_COUNT];

                if (oneAccessPath) {

                    set.add(getFQN(SPOKeyOrder.SPOC));
                    
                } else {

                    for (int i = SPOKeyOrder.FIRST_QUAD_INDEX; i <= SPOKeyOrder.LAST_QUAD_INDEX; i++) {

                        set.add(getFQN(SPOKeyOrder.valueOf(i)));

                    }

                }
                
            }

            /*
             * Note: We need the justifications index in the [indexNames] set
             * since that information is used to request the appropriate index
             * locks when running rules as mutation program in the LDS mode.
             */
            if (justify) {

                set.add(getNamespace() + "." + NAME_JUST);

            }

            this.indexNames = Collections.unmodifiableSet(set);

        }

        // Note: Do not eagerly resolve the indices.
        
//        {
//            
//            final boolean inlineTerms = Boolean.parseBoolean(getProperty(
//                    AbstractTripleStore.Options.INLINE_TERMS,
//                    AbstractTripleStore.Options.DEFAULT_INLINE_TERMS));
//
//            lexiconConfiguration = new LexiconConfiguration(inlineTerms);
//            
//        }
        
    }
    
    /**
     * Strengthened return type.
     */
    @Override
    public AbstractTripleStore getContainer() {

        return (AbstractTripleStore) super.getContainer();
        
    }

    /**
     * @todo This should use GRS row scan in the GRS for the SPORelation
     *       namespace. It is only used by the {@link LocalTripleStore}
     *       constructor and a unit test's main() method.  This method
     *       IS NOT part of any public API at this time.
     */
    public boolean exists() {

        for(String name : getIndexNames()) {
            
            if (getIndex(name) == null)
                return false;
            
        }
        
        return true;
        
    }

    public void create() {
      
        final IResourceLock resourceLock = acquireExclusiveLock();

        try {

            // create the relation declaration metadata.
            super.create();

            final IIndexManager indexManager = getIndexManager();

            if (keyArity == 3) {

                // triples
                
                if (oneAccessPath) {

                    indexManager
                            .registerIndex(getStatementIndexMetadata(SPOKeyOrder.SPO));

                } else {

                    for (int i = SPOKeyOrder.FIRST_TRIPLE_INDEX; i <= SPOKeyOrder.LAST_TRIPLE_INDEX; i++) {

                        indexManager
                                .registerIndex(getStatementIndexMetadata(SPOKeyOrder
                                        .valueOf(i)));

                    }

                }

            } else {

                // quads
                
                if(oneAccessPath) {

                    indexManager
                            .registerIndex(getStatementIndexMetadata(SPOKeyOrder.SPOC));

                } else {

                    for (int i = SPOKeyOrder.FIRST_QUAD_INDEX; i <= SPOKeyOrder.LAST_QUAD_INDEX; i++) {

                        indexManager
                                .registerIndex(getStatementIndexMetadata(SPOKeyOrder
                                        .valueOf(i)));

                    }

                }
                
            }

            if (justify) {

                final String fqn = getNamespace() + "." + NAME_JUST;

                indexManager.registerIndex(getJustIndexMetadata(fqn));

            }

//            lookupIndices();

        } finally {

            unlock(resourceLock);

        }

    }

    /*
     * @todo force drop of all indices rather than throwing an exception if an
     * index does not exist?
     */
    public void destroy() {

        final IResourceLock resourceLock = acquireExclusiveLock();

        try {

            final IIndexManager indexManager = getIndexManager();

            // clear hard references.
            for (int i = 0; i < indices.length; i++) {
             
                indices[i] = null;
                
            }

            // drop indices.
            for(String name : getIndexNames()) {

                indexManager.dropIndex(name);
                
            }
            
//            if (justify) {
//
//                indexManager.dropIndex(getNamespace() + "."+ NAME_JUST);
                just = null;
//
//            }

            // destroy the relation declaration metadata.
            super.destroy();

        } finally {

            unlock(resourceLock);

        }
        
    }

    /**
     * Overridden to return the hard reference for the index, which is cached
     * the first time it is resolved. This class does not eagerly resolve the
     * indices to (a) avoid a performance hit when running in a context where
     * the index view is not required; and (b) to avoid exceptions when running
     * as an {@link ITx#UNISOLATED} {@link AbstractTask} where the index was not
     * declared and hence can not be materialized.
     */
    @Override
    public IIndex getIndex(final IKeyOrder<? extends ISPO> keyOrder) {

        final int n = ((SPOKeyOrder) keyOrder).index();

        IIndex ndx = indices[n];

        if (ndx == null) {

            synchronized (indices) {

                if ((ndx = indices[n] = super.getIndex(keyOrder)) == null) {

                    throw new IllegalArgumentException(keyOrder.toString());

                }

            }

        }
        
        return ndx;

    }

    final public SPOKeyOrder getPrimaryKeyOrder() {
        
        return keyArity == 3 ? SPOKeyOrder.SPO : SPOKeyOrder.SPOC;
        
    }
    
    final public IIndex getPrimaryIndex() {
        
        return getIndex(getPrimaryKeyOrder());
        
    }

    /**
     * The optional index on which {@link Justification}s are stored.
     * 
     * @todo The Justifications index is not a regular index of the SPORelation.
     *       In fact, it is a relation for proof chains and is not really of the
     *       SPORelation at all and should probably be moved onto its own
     *       JRelation. The presence of the Justification index on the
     *       SPORelation would cause problems for methods which would like to
     *       enumerate the indices, except that we just silently ignore its
     *       presence in those methods (it is not in the index[] for example).
     *       <p>
     *       This would cause the justification index namespace to change to be
     *       a peer of the SPORelation namespace.
     */
    final public IIndex getJustificationIndex() {

        if (!justify)
            return null;

        if (just == null) {

            synchronized (this) {

                // attempt to resolve the index and set the index reference.
                if ((just = super.getIndex(getNamespace() + "." + NAME_JUST)) == null) {

                    throw new IllegalStateException();

                }

            }

        }

        return just;

    }

    /**
     * Return an iterator that will visit the distinct (s,p,o) tuples in the
     * source iterator. The context and statement type information will be
     * stripped from the visited {@link ISPO}s. The iterator will be backed by a
     * {@link BTree} on a {@link TemporaryStore} and will use a bloom filter for
     * fast point tests. The {@link BTree} and the source iterator will be
     * closed when the returned iterator is closed.
     * 
     * @param src
     *            The source iterator.
     * 
     * @return The filtered iterator.
     */
    public ICloseableIterator<ISPO> distinctSPOIterator(
            final ICloseableIterator<ISPO> src) {

        if (!src.hasNext())
            return new EmptyChunkedIterator<ISPO>(SPOKeyOrder.SPO);

        return new DistinctSPOIterator(this, src);
        
    }

    /**
     * Return a new unnamed {@link BTree} instance for the
     * {@link SPOKeyOrder#SPO} key order backed by a {@link TemporaryStore}. The
     * index will only store (s,p,o) triples (not quads) and will not store
     * either the SID or {@link StatementEnum}. This is a good choice when you
     * need to impose a "distinct" filter on (s,p,o) triples.
     * 
     * @param bloomFilter
     *            When <code>true</code>, a bloom filter is enabled for the
     *            index. The bloom filter provides fast correct rejection tests
     *            for point lookups up to ~2M triples and then shuts off
     *            automatically. See {@link BloomFilterFactory#DEFAULT} for more
     *            details.
     * 
     * @return The SPO index.
     */
    public BTree getSPOOnlyBTree(final boolean bloomFilter) {
        
        final TemporaryStore tempStore = getIndexManager().getTempStore();
        
        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        // leading key compression works great.
        final IRabaCoder leafKeySer = DefaultTupleSerializer
                .getDefaultLeafKeysCoder();

        // nothing is stored under the values.
        final IRabaCoder leafValSer = EmptyRabaValueCoder.INSTANCE;
        
        // setup the tuple serializer.
        metadata.setTupleSerializer(new SPOTupleSerializer(SPOKeyOrder.SPO,
                leafKeySer, leafValSer));

        if(bloomFilter) {

            // optionally enable the bloom filter.
            metadata.setBloomFilterFactory(BloomFilterFactory.DEFAULT);
            
        }

        final BTree ndx = BTree.create(tempStore, metadata); 
        
        return ndx;
        
    }
    
    /**
     * Overrides for the statement indices.
     */
    protected IndexMetadata getStatementIndexMetadata(final SPOKeyOrder keyOrder) {

        final IndexMetadata metadata = newIndexMetadata(getFQN(keyOrder));

        // leading key compression works great.
        final IRabaCoder leafKeySer = DefaultTupleSerializer
                .getDefaultLeafKeysCoder();

        final IRabaCoder leafValSer;
        if (!statementIdentifiers) {

            /*
             * Note: this value coder does not know about statement identifiers.
             * Therefore it is turned off if statement identifiers are enabled.
             */

            leafValSer = new FastRDFValueCoder2();
//            leafValSer = SimpleRabaCoder.INSTANCE;

        } else {

            /*
             * The default is canonical huffman coding, which is relatively slow
             * and does not achieve very good compression on term identifiers.
             */
            leafValSer = DefaultTupleSerializer.getDefaultValuesCoder();

            /*
             * @todo This is much faster than huffman coding, but less space
             * efficient. However, it appears that there are some cases where
             * SIDs are enabled but only the flag bits are persisted. What
             * gives?
             */
//            leafValSer = new FixedLengthValueRabaCoder(1 + 8);
            
        }
        
        metadata.setTupleSerializer(new SPOTupleSerializer(keyOrder,
                leafKeySer, leafValSer));

        if (bloomFilter && keyOrder.equals(SPOKeyOrder.SPO)) {
            
            /*
             * Enable the bloom filter for the SPO index only.
             * 
             * Note: This SPO index is used any time we have an access path that
             * is a point test. Therefore this is the only index for which it
             * makes sense to maintain a bloom filter.
             * 
             * Note: The maximum error rate (maxP) applies to the mutable BTree
             * only. For scale-out indices, there is one mutable BTree per index
             * partition and a new (empty) BTree is allocated each time the live
             * journal for the index partitions overflows.
             */

//            // good performance up to ~2M triples.
//            final int n = 1000000; // 1M
//            final double p = 0.01;
//            final double maxP = 0.20;

//            // good performance up to ~20M triples.
//            final int n = 10000000; // 10M
//            final double p = 0.05;
//            final double maxP = 0.20;

//            final BloomFilterFactory factory = new BloomFilterFactory(n, p, maxP);
            
            final BloomFilterFactory factory = BloomFilterFactory.DEFAULT;
            
            if (log.isInfoEnabled())
                log.info("Enabling bloom filter for SPO index: " + factory);
            
            metadata.setBloomFilterFactory( factory );
            
        }
        
        if(TimestampUtility.isReadWriteTx(getTimestamp())) {

            // enable isolatable indices.
            metadata.setIsolatable(true);

            /*
             * This tests to see whether or not the axiomsClass is NoAxioms. If
             * so, then we understand inference to be disabled and can use a
             * conflict resolver for plain triples, plain quads, or even plain
             * sids (SIDs are assigned by the lexicon using unisolated
             * operations).
             * 
             * @todo we should have an explicit "no inference" property. this
             * jumps through hoops since we can not call getAxioms() on the
             * AbstractTripleStore has been created, and SPORelation#create()
             * is invoked from within AbstractTripleStore#create().  When
             * adding that property, update a bunch of unit tests and code
             * which tests on the axioms class or BaseAxioms#isNone().
             */
            if (NoAxioms.class.getName().equals(
                    getContainer().getProperties().getProperty(
                            AbstractTripleStore.Options.AXIOMS_CLASS,
                            AbstractTripleStore.Options.DEFAULT_AXIOMS_CLASS))) {

                metadata.setConflictResolver(new SPOWriteWriteResolver());
                
            }
            
        }
        
        return metadata;

    }

    /**
     * Conflict resolver for add/add conflicts and retract/retract conflicts for
     * any of (triple store, triple store with SIDs or quad store) but without
     * inference. For an add/retract conflict, the writes can not be reconciled
     * and the transaction can not be validated. Write-write conflict
     * reconciliation is not supported if inference is enabled. The truth
     * maintenance behavior makes this too complex.
     * 
     * @todo It is really TM, not inference, which makes this complicated.
     *       However, if we allow statement type information into the statement
     *       index values then we must also deal with that information in the
     *       conflict resolver.
     * 
     * @todo In both cases where we can resolve the conflict, the tuple could be
     *       withdrawn from the writeSet since the desired tuple is already in
     *       the unisolated index rather than causing it to be touched on the
     *       unisolated index. This would have the advantage of minimizing
     *       writes on the unisolated index.
     */
    private static class SPOWriteWriteResolver implements IConflictResolver {

        /**
         * 
         */
        private static final long serialVersionUID = -1591732801502917983L;

        public SPOWriteWriteResolver() {

        }
        
        public boolean resolveConflict(IIndex writeSet, ITuple txTuple,
                ITuple currentTuple) throws Exception {

            /*
             * Note: In fact, retract-retract conflicts SHOULD NOT be resolved
             * because retracts are often used to remove an old property value
             * when a new value will be assigned for that property. For example,
             * 
             * tx1: -red, +green
             * 
             * tx2: -red, +blue
             * 
             * if we resolve the retract-retract conflict, then we will get
             * {green,blue} after the transactions run, rather than either
             * {green} or {blue}.
             */
//            if (txTuple.isDeletedVersion() && currentTuple.isDeletedVersion()) {
//
////                System.err.println("Resolved retract/retract conflict");
//                
//                // retract/retract is not a conflict.
//                return true;
//
//            }

            if (!txTuple.isDeletedVersion() && !currentTuple.isDeletedVersion()) {

//                System.err.println("Resolved add/add conflict");

                // add/add is not a conflict.
                return true;

            }

            /*
             * Note: We don't need to materialize the SPOs to resolve the
             * conflict, but this is how you would do that.
             */
            // final ISPO txSPO = (ISPO) txTuple.getObject();
            //                
            // final ISPO currentSPO = (ISPO) txTuple.getObject();

            // either delete/add or add/delete is a conflict.
            return false;

        }

    }

    /**
     * Overrides for the {@link IRawTripleStore#getJustificationIndex()}.
     */
    protected IndexMetadata getJustIndexMetadata(final String name) {

        final IndexMetadata metadata = newIndexMetadata(name);

        metadata.setTupleSerializer(new JustificationTupleSerializer(keyArity));

        return metadata;

    }

    public Set<String> getIndexNames() {

        return indexNames;
        
    }

    /**
     * Return an iterator visiting each {@link IKeyOrder} maintained by this
     * relation.
     */
    public Iterator<SPOKeyOrder> statementKeyOrderIterator() {

        switch(keyArity) {
        case 3:
            
            if (oneAccessPath)
                return SPOKeyOrder.spoOnlyKeyOrderIterator();
            
            return SPOKeyOrder.tripleStoreKeyOrderIterator();
            
        case 4:
            
            if (oneAccessPath)
                return SPOKeyOrder.spocOnlyKeyOrderIterator();
            
            return SPOKeyOrder.quadStoreKeyOrderIterator();
            
        default:
            
            throw new AssertionError();
            
        }

    }

    /**
     * Return the access path for a triple pattern.
     * 
     * @param s
     * @param p
     * @param o
     * @throws UnsupportedOperationException
     *             unless the {@link #getKeyArity()} is <code>3</code>.
     * 
     * @deprecated by {@link #getAccessPath(IV, IV, IV, IV)}
     */
    public IAccessPath<ISPO> getAccessPath(final IV s, final IV p, final IV o) {

        if (keyArity != 3)
            throw new UnsupportedOperationException();

        return getAccessPath(s, p, o, null/* c */, null/* filter */);

    }

    /**
     * Return the access path for a triple or quad pattern with an optional
     * filter.
     */
    public IAccessPath<ISPO> getAccessPath(final IV s, final IV p,
            final IV o, final IV c) {
        
        return getAccessPath(s, p, o, c, null/*filter*/);
        
    }

    /**
     * Return the access path for a triple or quad pattern with an optional
     * filter (core implementation). All arguments are optional. Any bound
     * argument will restrict the returned access path. For a triple pattern,
     * <i>c</i> WILL BE IGNORED as there is no index over the statement
     * identifiers, even when they are enabled. For a quad pattern, any argument
     * MAY be bound.
     * 
     * @param s
     *            The subject position (optional).
     * @param p
     *            The predicate position (optional).
     * @param o
     *            The object position (optional).
     * @param c
     *            The context position (optional and ignored for a triple
     *            store).
     * @param filter
     *            The filter (optional).
     * 
     * @return The best access path for that triple or quad pattern.
     * 
     * @throws UnsupportedOperationException
     *             for a triple store without statement identifiers if the
     *             <i>c</i> is non-{@link #NULL}.
     */
    @SuppressWarnings("unchecked")
    public IAccessPath<ISPO> getAccessPath(final IV s, final IV p,
            final IV o, final IV c, IElementFilter<ISPO> filter) {
        
        final IVariableOrConstant<IV> S = (s == null ? Var.var("s")
                : new Constant<IV>(s));

        final IVariableOrConstant<IV> P = (p == null ? Var.var("p")
                : new Constant<IV>(p));

        final IVariableOrConstant<IV> O = (o == null ? Var.var("o")
                : new Constant<IV>(o));
        
        IVariableOrConstant<IV> C = null;

        switch (keyArity) {

        case 3:
            if (!statementIdentifiers && c != null) {
                /*
                 * The 4th position should never become bound for a triple store
                 * without statement identifiers.
                 */
                throw new UnsupportedOperationException();
            }
            break;
        case 4:
            C = (c == null ? Var.var("c") : new Constant<IV>(c));
            break;
        default:
            throw new AssertionError();

        }

        return getAccessPath(new SPOPredicate(new String[] { getNamespace() },
                -1, // partitionId
                S, P, O, C,
                false, // optional
                filter,//
                null // expander
                ));
        
    }

    /**
     * Return the {@link IAccessPath} that is most efficient for the specified
     * predicate based on an analysis of the bound and unbound positions in the
     * predicate.
     * <p>
     * Note: When statement identifiers are enabled, the only way to bind the
     * context position is to already have an {@link SPO} on hand. There is no
     * index which can be used to look up an {@link SPO} by its context and the
     * context is always a blank node.
     * <p>
     * Note: This method is a hot spot, especially when the maximum parallelism
     * for subqueries is large. A variety of caching techniques are being
     * evaluated to address this.
     * 
     * @param pred
     *            The predicate.
     * 
     * @return The best access path for that predicate.
     */
    public IAccessPath<ISPO> getAccessPath(final IPredicate<ISPO> predicate) {

        /*
         * Note: Query is faster w/o cache on all LUBM queries.
         * 
         * @todo Optimization could reuse a caller's SPOAccessPath instance,
         * setting only the changed data on the fromKey/toKey.  That could
         * be done with setS(long), setO(long), setP(long) methods.  The
         * filter could be modified in the same manner.  That could cut down
         * on allocation costs, formatting the from/to keys, etc.
         */

//        if (predicate.getPartitionId() != -1) {
//
//            /*
//             * Note: This handles a read against a local index partition.
//             * 
//             * Note: This does not work here because it has the federation's
//             * index manager rather than the data service's index manager. That
//             * is because we always resolve relations against the federation
//             * since their metadata is stored in the global row store. Maybe
//             * this could be changed if we developed the concept of a
//             * "relation shard" accessed the metadata via a catalog and which
//             * was aware that only one index shard could be resolved locally.
//             */
//
//            return getAccessPathForIndexPartition(predicate);
//
//        }

        return _getAccessPath(predicate);
              
    }

    /**
     * Isolates the logic for selecting the {@link SPOKeyOrder} from the
     * {@link SPOPredicate} and then delegates to
     * {@link #getAccessPath(IKeyOrder, IPredicate)}.
     */
    final private SPOAccessPath _getAccessPath(final IPredicate<ISPO> predicate) {

        final SPOKeyOrder keyOrder = SPOKeyOrder.getKeyOrder(predicate, keyArity);
        
        final SPOAccessPath accessPath = getAccessPath(keyOrder, predicate);

        if (log.isDebugEnabled())
            log.debug(accessPath.toString());

        //            System.err.println("new access path: pred="+predicate);

        return accessPath;

    }

    /**
     * This handles a request for an access path that is restricted to a
     * specific index partition.
     * <p>
     * Note: This path is used with the scale-out JOIN strategy, which
     * distributes join tasks onto each index partition from which it needs to
     * read. Those tasks constrain the predicate to only read from the index
     * partition which is being serviced by that join task.
     * <p>
     * Note: Since the relation may materialize the index views for its various
     * access paths, and since we are restricted to a single index partition and
     * (presumably) an index manager that only sees the index partitions local
     * to a specific data service, we create an access path view for an index
     * partition without forcing the relation to be materialized.
     * <p>
     * Note: Expanders ARE NOT applied in this code path. Expanders require a
     * total view of the relation, which is not available during scale-out
     * pipeline joins.
     * 
     * @param indexManager
     *            This MUST be the data service local index manager so that the
     *            returned access path will read against the local shard.
     * @param predicate
     *            The predicate.
     * 
     * @throws IllegalArgumentException
     *             if either argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             unless the {@link IIndexManager} is a <em>local</em> index
     *             manager providing direct access to the specified shard.
     * @throws IllegalArgumentException
     *             unless the predicate identifies a specific shard using
     *             {@link IPredicate#getPartitionId()}.
     * 
     * @todo Raise this method into the {@link IRelation} interface.
     */
    public IAccessPath<ISPO> getAccessPathForIndexPartition(
            final IIndexManager indexManager, //
            final IPredicate<ISPO> predicate//
            ) {

// Note: This is the federation's index manager _always_.
//        final IIndexManager indexManager = getIndexManager();

        if (indexManager == null)
            throw new IllegalArgumentException();

        if (indexManager instanceof IBigdataFederation<?>) {

            /*
             * This will happen if you fail to re-create the JoinNexus within
             * the target execution environment.
             * 
             * This is disallowed because the predicate specifies an index
             * partition and expects to have access to the local index objects
             * for that index partition. However, the index partition is only
             * available when running inside of the ConcurrencyManager and when
             * using the IndexManager exposed by the ConcurrencyManager to its
             * tasks.
             */

            throw new IllegalArgumentException(
                    "Expecting a local index manager, not: "
                            + indexManager.getClass().toString());

        }
        
        if (predicate == null)
            throw new IllegalArgumentException();

        final int partitionId = predicate.getPartitionId();

        if (partitionId == -1) // must be a valid partition identifier.
            throw new IllegalArgumentException();

        // @todo This condition should probably be an error since the expander will be ignored.
//        if (predicate.getSolutionExpander() != null)
//            throw new IllegalArgumentException();

        if (predicate.getRelationCount() != 1) {

            /*
             * This is disallowed. The predicate must be reading on a single
             * local index partition, not a view comprised of more than one
             * index partition.
             * 
             * @todo In fact, we could allow a view here as long as all parts of
             * the view are local. That would be relevant when the other view
             * component was a shard of a focusStore for parallel decomposition
             * of RDFS closure, etc.
             */
            
            throw new IllegalStateException();
            
        }
        
        final String namespace = getNamespace();//predicate.getOnlyRelationName();

        /*
         * Find the best access path for that predicate.
         */
        final SPOKeyOrder keyOrder = SPOKeyOrder.getKeyOrder(predicate,
                keyArity);

        // The name of the desired index partition.
        final String name = DataService.getIndexPartitionName(namespace + "."
                + keyOrder.getIndexName(), predicate.getPartitionId());

        /*
         * Note: whether or not we need both keys and values depends on the
         * specific index/predicate.
         * 
         * Note: If the timestamp is a historical read, then the iterator will
         * be read only regardless of whether we specify that flag here or not.
         */
//      * Note: We can specify READ_ONLY here since the tail predicates are not
//      * mutable for rule execution.
        final int flags = IRangeQuery.KEYS | IRangeQuery.VALS;// | IRangeQuery.READONLY;

        final long timestamp = getTimestamp();//getReadTimestamp();
        
        // MUST be a local index view.
        final ILocalBTreeView ndx = (ILocalBTreeView) indexManager
                .getIndex(name, timestamp);

        return new SPOAccessPath(indexManager, timestamp, predicate,
                keyOrder, ndx, flags, getChunkOfChunksCapacity(),
                getChunkCapacity(), getFullyBufferedReadThreshold()).init();

    }
    
    /**
     * Core impl.
     * 
     * @param keyOrder
     *            The natural order of the selected index (this identifies the
     *            index).
     * @param predicate
     *            The predicate specifying the query constraint on the access
     *            path.
     * 
     * @return The access path.
     */
    public SPOAccessPath getAccessPath(final IKeyOrder<ISPO> keyOrder,
            final IPredicate<ISPO> predicate) {

        if (keyOrder == null)
            throw new IllegalArgumentException();
        
        if (predicate == null)
            throw new IllegalArgumentException();
        
        final IIndex ndx = getIndex(keyOrder);

        if (ndx == null) {
        
            throw new IllegalArgumentException("no index? relation="
                    + getNamespace() + ", timestamp=" + getTimestamp()
                    + ", keyOrder=" + keyOrder + ", pred=" + predicate
                    + ", indexManager=" + getIndexManager());
            
        }

        /*
         * Note: The PARALLEL flag here is an indication that the iterator may
         * run in parallel across the index partitions. This only effects
         * scale-out and only for simple triple patterns since the pipeline join
         * does something different (it runs inside the index partition using
         * the local index, not the client's view of a distributed index).
         * 
         * @todo Introducing the PARALLEL flag here will break semantics when
         * the rule is supposed to be "stable" (repeatable order) or when the
         * access path is supposed to be fully ordered. What we really need to
         * do is take the "stable" flag from the rule and transfer it onto the
         * predicates in that rule so we can enforce stable execution for
         * scale-out. Of course, that would kill any parallelism for scale-out
         * :-) This shows up as an issue when using SLICEs since the rule that
         * we execute has to have stable results for someone to page through
         * those results using LIMIT/OFFSET.
         */
        final int flags = IRangeQuery.KEYS
                | IRangeQuery.VALS
                | (TimestampUtility.isReadOnly(getTimestamp()) ? IRangeQuery.READONLY
                        : 0)
                | IRangeQuery.PARALLEL
                ;
        
        final AbstractTripleStore container = getContainer();
        
        final int chunkOfChunksCapacity = container.getChunkOfChunksCapacity();

        final int chunkCapacity = container.getChunkCapacity();

        final int fullyBufferedReadThreshold = container.getFullyBufferedReadThreshold();
        
        return new SPOAccessPath(this, predicate, keyOrder, ndx, flags,
                chunkOfChunksCapacity, chunkCapacity,
                fullyBufferedReadThreshold).init();
        
    }
    
//    public long getElementCount(boolean exact) {
//
//        final IIndex ndx = getIndex(SPOKeyOrder.SPO);
//        
//        if (exact) {
//        
//            return ndx.rangeCountExact(null/* fromKey */, null/* toKey */);
//            
//        } else {
//            
//            return ndx.rangeCount(null/* fromKey */, null/* toKey */);
//            
//        }
//        
//    }

    /**
     * Efficient scan of the distinct term identifiers that appear in the first
     * position of the keys for the statement index corresponding to the
     * specified {@link IKeyOrder}. For example, using {@link SPOKeyOrder#POS}
     * will give you the term identifiers for the distinct predicates actually
     * in use within statements in the {@link SPORelation}.
     * 
     * @param keyOrder
     *            The selected index order.
     * 
     * @return An iterator visiting the distinct term identifiers.
     */
    public IChunkedIterator<IV> distinctTermScan(final IKeyOrder<ISPO> keyOrder) {

        return distinctTermScan(keyOrder,/* termIdFilter */null);
        
    }

    /**
     * Efficient scan of the distinct term identifiers that appear in the first
     * position of the keys for the statement index corresponding to the
     * specified {@link IKeyOrder}. For example, using {@link SPOKeyOrder#POS}
     * will give you the term identifiers for the distinct predicates actually
     * in use within statements in the {@link SPORelation}.
     * 
     * @param keyOrder
     *            The selected index order.
     * 
     * @return An iterator visiting the distinct term identifiers.
     * 
     * @todo add the ability to specify {@link IRangeQuery#PARALLEL} here for
     *       fast scans across multiple shards when chunk-wise order is Ok.
     */
    public IChunkedIterator<IV> distinctTermScan(
            final IKeyOrder<ISPO> keyOrder, final ITermIVFilter termIdFilter) {

        final FilterConstructor<SPO> filter = new FilterConstructor<SPO>();
        
        /*
         * Layer in the logic to advance to the tuple that will have the
         * next distinct term identifier in the first position of the key.
         */
        filter.addFilter(new DistinctTermAdvancer(keyArity));

        if (termIdFilter != null) {

            /*
             * Layer in a filter for only the desired term types.
             */
            
            filter.addFilter(new TupleFilter<SPO>() {

                private static final long serialVersionUID = 1L;

                @Override
                protected boolean isValid(final ITuple<SPO> tuple) {

                    final byte[] key = tuple.getKey();
                    
                    final IV iv = IVUtility.decode(key);
                    
                    return termIdFilter.isValid(iv);

                }

            });

        }

        @SuppressWarnings("unchecked")
        final Iterator<IV> itr = new Striterator(getIndex(keyOrder)
                .rangeIterator(null/* fromKey */, null/* toKey */,
                        0/* capacity */, IRangeQuery.KEYS | IRangeQuery.CURSOR,
                        filter)).addFilter(new Resolver() {
                    
                    private static final long serialVersionUID = 1L;
                    
                    /**
                     * Resolve SPO key to IV.
                     */
                    @Override
                    protected IV resolve(Object obj) {
                        
                        final byte[] key = ((ITuple) obj).getKey();
                        
                        return IVUtility.decode(key);
                        
                    }
                    
                });

        return new ChunkedWrappedIterator<IV>(itr);
                
    }
	/**
     * Efficient scan of the distinct term identifiers that appear in the first
     * position of the keys for the statement index corresponding to the
     * specified {@link IKeyOrder}. For example, using {@link SPOKeyOrder#POS}
     * will give you the term identifiers for the distinct predicates actually
     * in use within statements in the {@link SPORelation}.
     * 
     * @param keyOrder
     *            The selected index order.
     * 
     * @return An iterator visiting the distinct term identifiers.
     */
    public IChunkedIterator<IV> distinctMultiTermScan(
            final IKeyOrder<ISPO> keyOrder, IV[] knownTerms) {

        return distinctMultiTermScan(keyOrder, knownTerms,/* termIdFilter */null);

    }

    /**
     * Efficient scan of the distinct term identifiers that appear in the first
     * position of the keys for the statement index corresponding to the
     * specified {@link IKeyOrder}. For example, using {@link SPOKeyOrder#POS}
     * will give you the term identifiers for the distinct predicates actually
     * in use within statements in the {@link SPORelation}.
     * 
     * @param keyOrder
     *            The selected index order.
     * @param knownTerms
     *            An array of term identifiers to be interpreted as bindings
     *            using the <i>keyOrder</i>.
     * @param termIdFilter
     *            An optional filter.
     * 
     * @return An iterator visiting the distinct term identifiers.
     * 
     * @todo add the ability to specify {@link IRangeQuery#PARALLEL} here for
     *       fast scans across multiple shards when chunk-wise order is Ok.
     */
    public IChunkedIterator<IV> distinctMultiTermScan(
            final IKeyOrder<ISPO> keyOrder, final IV[] knownTerms,
            final ITermIVFilter termIdFilter) {

        final FilterConstructor<SPO> filter = new FilterConstructor<SPO>();
        final int nterms = knownTerms.length;

        final IKeyBuilder keyBuilder = KeyBuilder.newInstance();
        
        for (int i = 0; i < knownTerms.length; i++) {
            knownTerms[i].encode(keyBuilder);
        }
        
        final byte[] fromKey = knownTerms.length == 0 ? null 
                : keyBuilder.getKey();
        
        final byte[] toKey = fromKey == null ? null 
                : SuccessorUtil.successor(fromKey.clone());
        
        /*
         * Layer in the logic to advance to the tuple that will have the next
         * distinct term identifier in the first position of the key.
         */
        filter.addFilter(new DistinctMultiTermAdvancer(getKeyArity(), nterms));

        if (termIdFilter != null) {

            /*
             * Layer in a filter for only the desired term types.
             */

            filter.addFilter(new TupleFilter<SPO>() {

                private static final long serialVersionUID = 1L;

                @Override
                protected boolean isValid(final ITuple<SPO> tuple) {

                    final byte[] key = tuple.getKey();
                    
                    final int pos = knownTerms.length;
                    
                    final IV iv = IVUtility.decode(key, pos+1)[pos];
                    
                    return termIdFilter.isValid(iv);

                }

            });

        }

        @SuppressWarnings("unchecked")
        final Iterator<IV> itr = new Striterator(getIndex(keyOrder)
                .rangeIterator(fromKey, toKey,
                        0/* capacity */, IRangeQuery.KEYS | IRangeQuery.CURSOR,
                        filter)).addFilter(new Resolver() {

            private static final long serialVersionUID = 1L;

            /**
             * Resolve SPO key to IV.
             */
            @Override
            protected IV resolve(Object obj) {
                
                final byte[] key = ((ITuple) obj).getKey();
                
                final int pos = knownTerms.length;
                
                return IVUtility.decode(key, pos+1)[pos];
                
            }
            
        });

        return new ChunkedWrappedIterator<IV>(itr);

    }

    public SPO newElement(final IPredicate<ISPO> predicate,
            final IBindingSet bindingSet) {

        if (predicate == null)
            throw new IllegalArgumentException();
        
        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final IV s = asBound(predicate, 0, bindingSet);

        final IV p = asBound(predicate, 1, bindingSet);

        final IV o = asBound(predicate, 2, bindingSet);

        final SPO spo = new SPO(s, p, o, StatementEnum.Inferred);
        
        if(log.isDebugEnabled())
            log.debug(spo.toString());
        
        return spo;
        
    }

    public Class<ISPO> getElementClass() {

        return ISPO.class;

    }
   
   /**
     * Extract the bound value from the predicate. When the predicate is not
     * bound at that index, then extract its binding from the binding set.
     * 
     * @param pred
     *            The predicate.
     * @param index
     *            The index into that predicate.
     * @param bindingSet
     *            The binding set.
     *            
     * @return The bound value.
     */
    @SuppressWarnings("unchecked")
    private IV asBound(final IPredicate<ISPO> pred, final int index,
            final IBindingSet bindingSet) {

        final IVariableOrConstant<IV> t = pred.get(index);

        final IConstant<IV> c;
        if(t.isVar()) {
            
            c = bindingSet.get((IVariable) t);
            
        } else {
            
            c = (IConstant<IV>)t;
            
        }

        return c.get();

    }

//    /**
//     * Return a buffer onto which a multi-threaded process may write chunks of
//     * elements to be written on the relation asynchronously. Chunks will be
//     * combined by a {@link BlockingBuffer} for greater efficiency. The buffer
//     * should be {@link BlockingBuffer#close() closed} once no more data will be
//     * written. This buffer may be used whether or not statement identifiers are
//     * enabled and will eventually delegate its work to
//     * {@link AbstractTripleStore#addStatements(AbstractTripleStore, boolean, IChunkedOrderedIterator, IElementFilter)}
//     * <p>
//     * The returned {@link BlockingBuffer} is thread-safe and is intended for
//     * high concurrency use cases such as bulk loading data in which multiple
//     * threads need to write on the relation concurrently. The use of this
//     * buffer can substantially increase throughput for such use cases owing to
//     * its ability to combine chunks together before they are scattered to the
//     * indices. The effect is most pronounced for scale-out deployments when
//     * each write would normally be scattered to a large number of index
//     * partitions. By combining the chunks before they are scattered, the writes
//     * against the index partitions can be larger. Increased throughput results
//     * both from issuing fewer RMI requests, each of which must sit in a queue,
//     * and from having more data in each request which results in more efficient
//     * ordered writes on each index partition.
//     * 
//     * @param chunkSize
//     *            The desired chunk size for a write operation (this is an
//     *            explicit parameter since the desirable chunk size for a write
//     *            can be much larger than the desired chunk size for a read).
//     * 
//     * @return A write buffer. The {@link Future} on the blocking buffer is the
//     *         task draining the buffer and writing on the statement indices. It
//     *         may be used to wait until the writes are stable on the federation
//     *         or to cancel any outstanding writes.
//     */
//    synchronized public BlockingBuffer<ISPO[]> newWriteBuffer(final int chunkSize) {
//
//        final BlockingBuffer<ISPO[]> writeBuffer = new BlockingBuffer<ISPO[]>(
//                getChunkOfChunksCapacity(), chunkSize/*getChunkCapacity()*/,
//                getChunkTimeout(), TimeUnit.MILLISECONDS);
//
//        final Future<Void> future = getExecutorService().submit(
//                new ChunkConsumerTask(writeBuffer.iterator()));
//
//        writeBuffer.setFuture(future);
//
//        return writeBuffer;
//
//    }
//
//    /**
//     * Consumes elements from the source iterator, converting them into chunks
//     * on a {@link BlockingBuffer}. The consumer will drain the chunks from the
//     * buffer.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    private class ChunkConsumerTask implements Callable<Void> {
//        
//        /**
//         * The source which this task is draining.
//         * <p>
//         * Note: DO NOT close this iterator from within {@link #call()} - that
//         * would cause the task to interrupt itself!
//         */
//        private final IAsynchronousIterator<ISPO[]> src;
//        
//        public ChunkConsumerTask(final IAsynchronousIterator<ISPO[]> src) {
//
//            if (src == null)
//                throw new IllegalArgumentException();
//            
//            this.src = src;
//
//        }
//            
//        public Void call() throws Exception {
//
//            long nchunks = 0;
//            long nelements = 0;
//
//            while (src.hasNext()) {
//
//                final ISPO[] chunk = src.next();
//
//                nchunks++;
//                nelements += chunk.length;
//
//                if (log.isDebugEnabled())
//                    log.debug("#chunks=" + nchunks + ", chunkSize="
//                            + chunk.length + ", nelements=" + nelements);
//
//                getContainer()
//                        .addStatements(chunk, chunk.length, null/* filter */);
//
//            }
//
//            if (log.isInfoEnabled())
//                log.info("Done: #chunks=" + nchunks + ", #elements="
//                        + nelements);
//
//            return null;
//
//        }
//
//    }

    /**
     * Inserts {@link SPO}s, writing on the statement indices in parallel.
     * <p>
     * Note: This does NOT write on the justifications index. If justifications
     * are being maintained then the {@link ISolution}s MUST report binding
     * sets and an {@link InsertSolutionBuffer} MUST be used that knows how to
     * write on the justifications index AND delegate writes on the statement
     * indices to this method.
     * <p>
     * Note: This does NOT assign statement identifiers. The {@link SPORelation}
     * does not have direct access to the {@link LexiconRelation} and the latter
     * is responsible for assigning term identifiers. Code that writes explicit
     * statements onto the statement indices MUST use
     * {@link AbstractTripleStore#addStatements(AbstractTripleStore, boolean, IChunkedOrderedIterator, IElementFilter)},
     * which knows how to generate the statement identifiers. In turn, that
     * method will delegate each "chunk" to this method.
     */
    public long insert(final IChunkedOrderedIterator<ISPO> itr) {

        try {
            
            long n = 0;
            
            while(itr.hasNext()) {
                
                final ISPO[] a = itr.nextChunk();
                
                n += insert( a, a.length, null/*filter*/ );
                
            }
         
            return n;
            
        } finally {
            
            itr.close();
            
        }
        
    }
    
    /**
     * Deletes {@link SPO}s, writing on the statement indices in parallel.
     * <p>
     * Note: The {@link ISPO#isModified()} flag is set by this method.
     * <p>
     * Note: This does NOT write on the justifications index. If justifications
     * are being maintained then the {@link ISolution}s MUST report binding sets
     * and an {@link InsertSolutionBuffer} MUST be used that knows how to write
     * on the justifications index AND delegate writes on the statement indices
     * to this method.
     * <p>
     * Note: This does NOT perform truth maintenance!
     * <p>
     * Note: This does NOT compute the closure for statement identifiers
     * (statements that need to be deleted because they are about a statement
     * that is being deleted).
     * 
     * @see AbstractTripleStore#removeStatements(IChunkedOrderedIterator,
     *      boolean)
     * @see SPOAccessPath#removeAll()
     */
    public long delete(final IChunkedOrderedIterator<ISPO> itr) {

        try {
            
            long n = 0;
            
            while(itr.hasNext()) {
                
                final ISPO[] a = itr.nextChunk();
                
                n += delete(a, a.length);
                
            }
         
            return n;
            
        } finally {
            
            itr.close();
            
        }
        
    }

    /**
     * Deletes {@link SPO}s, writing on the statement indices in parallel.
     * <p>
     * Note: The {@link ISPO#isModified()} flag is set by this method.
     * <p>
     * Note: This does NOT write on the justifications index. If justifications
     * are being maintained then the {@link ISolution}s MUST report binding sets
     * and an {@link InsertSolutionBuffer} MUST be used that knows how to write
     * on the justifications index AND delegate writes on the statement indices
     * to this method.
     * <p>
     * Note: This does NOT perform truth maintenance!
     * <p>
     * Note: This does NOT compute the closure for statement identifiers
     * (statements that need to be deleted because they are about a statement
     * that is being deleted).
     * 
     * Note: The statements are inserted into each index in parallel. We clone
     * the statement[] and sort and bulk load each statement index in parallel
     * using a thread pool. All mutation to the statement indices goes through
     * this method.
     * 
     * @param a
     *            An {@link ISPO}[] of the statements to be written onto the
     *            statement indices. For each {@link ISPO}, the
     *            {@link ISPO#isModified()} flag will be set iff the tuple
     *            corresponding to the statement was : (a) inserted; (b) updated
     *            (state change, such as to the {@link StatementEnum} value), or
     *            (c) removed.
     * @param numStmts
     *            The #of elements of that array that will be written.
     * @param filter
     *            An optional filter on the elements to be written.
     * 
     * @return The mutation count.
     */
    public long insert(final ISPO[] a, final int numStmts,
            final IElementFilter<ISPO> filter) {

        if (a == null)
            throw new IllegalArgumentException();
        
        if (numStmts > a.length)
            throw new IllegalArgumentException();
        
        if (numStmts == 0)
            return 0L;

        final long begin = System.currentTimeMillis();

        if(log.isDebugEnabled()) {
            
            log.debug("indexManager="+getIndexManager());
            
        }
        
        // time to sort the statements.
        final AtomicLong sortTime = new AtomicLong(0);

        // time to generate the keys and load the statements into the
        // indices.
        final AtomicLong insertTime = new AtomicLong(0);

        final AtomicLong mutationCount = new AtomicLong(0);
        
        final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(3);

        /*
         * When true, mutations on the primary index (SPO or SPOC) will be
         * reported. That metadata is used to set the isModified flag IFF the
         * tuple corresponding to the statement in the indices was (a) inserted;
         * (b) modified; or (c) removed.
         */
        final boolean reportMutation = true;
        
        if (keyArity == 3) {

            tasks.add(new SPOIndexWriter(this, a, numStmts, false/* clone */,
                    SPOKeyOrder.SPO, SPOKeyOrder.SPO.isPrimaryIndex(),
                    filter, sortTime, insertTime, mutationCount,
                    reportMutation));
    
            if (!oneAccessPath) {
    
                tasks.add(new SPOIndexWriter(this, a, numStmts, true/* clone */,
                        SPOKeyOrder.POS, SPOKeyOrder.POS.isPrimaryIndex(),
                        filter, sortTime, insertTime, mutationCount,
                        false/*reportMutation*/));
    
                tasks.add(new SPOIndexWriter(this, a, numStmts, true/* clone */,
                        SPOKeyOrder.OSP, SPOKeyOrder.OSP.isPrimaryIndex(),
                        filter, sortTime, insertTime, mutationCount,
                        false/*reportMutation*/));
    
            }

        } else {

            tasks.add(new SPOIndexWriter(this, a, numStmts, false/* clone */,
                    SPOKeyOrder.SPOC, SPOKeyOrder.SPOC.isPrimaryIndex(),
                    filter, sortTime, insertTime,
                    mutationCount, reportMutation));

            if (!oneAccessPath) {

                tasks.add(new SPOIndexWriter(this, a, numStmts,
                        true/* clone */, SPOKeyOrder.POCS,
                        SPOKeyOrder.POCS.isPrimaryIndex(),
                        filter, sortTime,
                        insertTime, mutationCount, false/* reportMutation */));

                tasks.add(new SPOIndexWriter(this, a, numStmts,
                        true/* clone */, SPOKeyOrder.OCSP, 
                        SPOKeyOrder.OCSP.isPrimaryIndex(),
                        filter, sortTime,
                        insertTime, mutationCount, false/* reportMutation */));

                tasks.add(new SPOIndexWriter(this, a, numStmts,
                        true/* clone */, SPOKeyOrder.CSPO,
                        SPOKeyOrder.CSPO.isPrimaryIndex(),
                        filter, sortTime,
                        insertTime, mutationCount, false/* reportMutation */));

                tasks.add(new SPOIndexWriter(this, a, numStmts,
                        true/* clone */, SPOKeyOrder.PCSO, 
                        SPOKeyOrder.PCSO.isPrimaryIndex(),
                        filter, sortTime,
                        insertTime, mutationCount, false/* reportMutation */));

                tasks.add(new SPOIndexWriter(this, a, numStmts,
                        true/* clone */, SPOKeyOrder.SOPC,
                        SPOKeyOrder.SOPC.isPrimaryIndex(),
                        filter, sortTime,
                        insertTime, mutationCount, false/* reportMutation */));

            }
            
        }

        // if(numStmts>1000) {
        //
        // log.info("Writing " + numStmts + " statements...");
        //                    
        // }

        final List<Future<Long>> futures;
/*
        final long elapsed_SPO;
        final long elapsed_POS;
        final long elapsed_OSP;
*/
        try {

            futures = getExecutorService().invokeAll(tasks);

            for (int i = 0; i < tasks.size(); i++) {
                
                futures.get(i).get();

            }
/*
            elapsed_SPO = futures.get(0).get();
            if (!oneAccessPath) {
                elapsed_POS = futures.get(1).get();
                elapsed_OSP = futures.get(2).get();
            } else {
                elapsed_POS = 0;
                elapsed_OSP = 0;
            }
*/
        } catch (InterruptedException ex) {

            throw new RuntimeException(ex);

        } catch (ExecutionException ex) {

            throw new RuntimeException(ex);

        }

        final long elapsed = System.currentTimeMillis() - begin;

        if (log.isInfoEnabled() && numStmts > 1000) {

            log.info("Wrote " + numStmts + " statements (mutationCount="
                    + mutationCount + ") in " + elapsed + "ms" //
                    + "; sort=" + sortTime + "ms" //
                    + ", keyGen+insert=" + insertTime + "ms" //
//                    + "; spo=" + elapsed_SPO + "ms" //
//                    + ", pos=" + elapsed_POS + "ms" //
//                    + ", osp=" + elapsed_OSP + "ms" //
            );

        }

        return mutationCount.get();
        
    }

    /**
     * Delete the {@link SPO}s from the statement indices. Any justifications
     * for those statements will also be deleted. The {@link ISPO#isModified()}
     * flag is set by this method if the {@link ISPO} was pre-existing in the
     * database and was therefore deleted by this operation.
     * 
     * @param stmts
     *            The {@link SPO}s.
     * @param numStmts
     *            The #of elements in that array to be processed.
     * 
     * @return The #of statements that were removed (mutationCount).
     */
    public long delete(final ISPO[] stmts, final int numStmts) {

        if (stmts == null)
            throw new IllegalArgumentException();

        if (numStmts < 0 || numStmts > stmts.length)
            throw new IllegalArgumentException();

        if (numStmts == 0)
            return 0L;
        
        final long begin = System.currentTimeMillis();

        // The time to sort the data.
        final AtomicLong sortTime = new AtomicLong(0);

        // The time to delete the statements from the indices.
        final AtomicLong writeTime = new AtomicLong(0);

        // The mutation count.
        final AtomicLong mutationCount = new AtomicLong(0);

        final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(3);

        /*
         * When true, mutations on the primary index (SPO or SPOC) will be
         * reported. That metadata is used to set the isModified flag IFF the
         * tuple corresponding to the statement in the indices was (a) inserted;
         * (b) modified; or (c) removed.
         */
        final boolean reportMutation = true;

        if (keyArity == 3) {

            tasks.add(new SPOIndexRemover(this, stmts, numStmts,
                    SPOKeyOrder.SPO, SPOKeyOrder.SPO.isPrimaryIndex(), 
                    false/* clone */, sortTime, writeTime,
                    mutationCount, reportMutation));

            if (!oneAccessPath) {

                tasks.add(new SPOIndexRemover(this, stmts, numStmts,
                        SPOKeyOrder.POS, SPOKeyOrder.POS.isPrimaryIndex(),
                        true/* clone */, sortTime, writeTime,
                        mutationCount,
                        false/* reportMutation */));

                tasks.add(new SPOIndexRemover(this, stmts, numStmts,
                        SPOKeyOrder.OSP, SPOKeyOrder.OSP.isPrimaryIndex(),
                        true/* clone */, sortTime, writeTime,
                        mutationCount, false/* reportMutation */));

            }

        } else {

            tasks.add(new SPOIndexRemover(this, stmts, numStmts,
                    SPOKeyOrder.SPOC, SPOKeyOrder.SPOC.isPrimaryIndex(),
                    false/* clone */, sortTime, writeTime,
                    mutationCount, reportMutation));

            if (!oneAccessPath) {

                tasks.add(new SPOIndexRemover(this, stmts, numStmts,
                        SPOKeyOrder.POCS,SPOKeyOrder.POCS.isPrimaryIndex(), 
                        true/* clone */, sortTime, writeTime,
                        mutationCount, false/* reportMutation */));

                tasks.add(new SPOIndexRemover(this, stmts, numStmts,
                        SPOKeyOrder.OCSP, SPOKeyOrder.OCSP.isPrimaryIndex(),
                        true/* clone */, sortTime, writeTime,
                        mutationCount, false/* reportMutation */));

                tasks.add(new SPOIndexRemover(this, stmts, numStmts,
                        SPOKeyOrder.CSPO, SPOKeyOrder.CSPO.isPrimaryIndex(),
                        true/* clone */, sortTime, writeTime,
                        mutationCount, false/* reportMutation */));

                tasks.add(new SPOIndexRemover(this, stmts, numStmts,
                        SPOKeyOrder.PCSO, SPOKeyOrder.PCSO.isPrimaryIndex(),
                        true/* clone */, sortTime, writeTime,
                        mutationCount, false/* reportMutation */));

                tasks.add(new SPOIndexRemover(this, stmts, numStmts,
                        SPOKeyOrder.SOPC, SPOKeyOrder.SOPC.isPrimaryIndex(),
                        true/* clone */, sortTime, writeTime,
                        mutationCount, false/* reportMutation */));

            }

        }

        if (justify) {

            /*
             * Also retract the justifications for the statements.
             */

            tasks.add(new JustificationRemover(this, stmts, numStmts,
                    true/* clone */, sortTime, writeTime));

        }

        final List<Future<Long>> futures;
        /*
        final long elapsed_SPO;
        final long elapsed_POS;
        final long elapsed_OSP;
        final long elapsed_JST;
        */

        try {

            futures = getExecutorService().invokeAll(tasks);
            
            for (int i = 0; i < tasks.size(); i++) {
                
                futures.get(i).get();
                
            }
/*
            elapsed_SPO = futures.get(0).get();

            if (!oneAccessPath) {

                elapsed_POS = futures.get(1).get();

                elapsed_OSP = futures.get(2).get();

            } else {

                elapsed_POS = 0;

                elapsed_OSP = 0;

            }

            if (justify) {

                elapsed_JST = futures.get(3).get();

            } else {

                elapsed_JST = 0;

            }
*/
        } catch (InterruptedException ex) {

            throw new RuntimeException(ex);

        } catch (ExecutionException ex) {

            throw new RuntimeException(ex);

        }

        final long elapsed = System.currentTimeMillis() - begin;

        if (log.isInfoEnabled() && numStmts > 1000) {

            log.info("Removed " + numStmts + " in " + elapsed
                    + "ms; sort=" + sortTime + "ms, keyGen+delete="
                    + writeTime + "ms" 
//                    + "; spo=" + elapsed_SPO + "ms, pos="
//                    + elapsed_POS + "ms, osp=" + elapsed_OSP
//                    + "ms, jst=" + elapsed_JST
                    );

        }

        return mutationCount.get();
        
    }
    
    /**
     * Adds justifications to the store.
     * 
     * @param itr
     *            The iterator from which we will read the {@link Justification}s
     *            to be added. The iterator is closed by this operation.
     * 
     * @return The #of {@link Justification}s written on the justifications
     *         index.
     * 
     * @todo a lot of the cost of loading data is writing the justifications.
     *       SLD/magic sets will relieve us of the need to write the
     *       justifications since we can efficiently prove whether or not the
     *       statements being removed can be entailed from the remaining
     *       statements. Any statement which can still be proven is converted to
     *       an inference. Since writing the justification chains is such a
     *       source of latency, SLD/magic sets will translate into an immediate
     *       performance boost for data load.
     */
    public long addJustifications(final IChunkedIterator<Justification> itr) {

        try {

            if (!itr.hasNext())
                return 0;

            final long begin = System.currentTimeMillis();

//            /*
//             * Note: This capacity estimate is based on N longs per SPO, one
//             * head, and 2-3 SPOs in the tail. The capacity will be extended
//             * automatically if necessary.
//             */
//
//            final KeyBuilder keyBuilder = new KeyBuilder(IRawTripleStore.N
//                    * (1 + 3) * Bytes.SIZEOF_LONG);

            long nwritten = 0;

            final IIndex ndx = getJustificationIndex();
            
            final JustificationTupleSerializer tupleSer = (JustificationTupleSerializer) ndx
                    .getIndexMetadata().getTupleSerializer();

            while (itr.hasNext()) {

                final Justification[] a = itr.nextChunk();

                final int n = a.length;

                // sort into their natural order.
                Arrays.sort(a);

                final byte[][] keys = new byte[n][];

                for (int i = 0; i < n; i++) {

//                    final Justification jst = a[i];

                    keys[i] = tupleSer.serializeKey(a[i]);//jst.getKey(keyBuilder);

                }

                /*
                 * sort into their natural order.
                 * 
                 * @todo is it faster to sort the Justification[] or the keys[]?
                 * See above for the alternative.
                 */
                // Arrays.sort(keys,UnsignedByteArrayComparator.INSTANCE);

                final LongAggregator aggregator = new LongAggregator();

                ndx.submit(0/* fromIndex */, n/* toIndex */, keys,
                                null/* vals */,
                                WriteJustificationsProcConstructor.INSTANCE,
                                aggregator);

                nwritten += aggregator.getResult();

            }

            final long elapsed = System.currentTimeMillis() - begin;

            if (log.isInfoEnabled())
                log.info("Wrote " + nwritten + " justifications in " + elapsed
                        + " ms");

            return nwritten;

        } finally {

            itr.close();

        }

    }

    /**
     * Dumps the specified index.
     */
    @SuppressWarnings("unchecked")
    public StringBuilder dump(final IKeyOrder<ISPO> keyOrder) {

        final StringBuilder sb = new StringBuilder();

        final IPredicate<ISPO> pred = new SPOPredicate(
                new String[] { getNamespace() }, -1, // partitionId
                Var.var("s"),//
                Var.var("p"),//
                Var.var("o"),//
                keyArity == 3 ? null : Var.var("c"),//
                false, // optional
                null, // filter,
                null // expander
        );

        final IChunkedOrderedIterator<ISPO> itr = getAccessPath(keyOrder, pred)
                .iterator();

        try {

            while (itr.hasNext()) {

                sb.append(itr.next());

                sb.append("\n");

            }

        } finally {

            itr.close();

        }

        return sb;

    }

    /**
     * The {@link ILexiconConfiguration} instance, which will determine how
     * terms are encoded and decoded in the key space.
    private ILexiconConfiguration lexiconConfiguration;
     */

    /**
     * See {@link ILexiconConfiguration#isInline(DTE)}.  Delegates to the
     * {@link #lexiconConfiguration} instance.
    public boolean isInline(DTE dte) {
        return lexiconConfiguration.isInline(dte);
    }
     */

    /**
     * See {@link ILexiconConfiguration#isLegacyEncoding()}.  Delegates to the
     * {@link #lexiconConfiguration} instance.
    public boolean isLegacyEncoding() {
        return lexiconConfiguration.isLegacyEncoding();
    }
     */
    
    /**
     * Return the {@link #lexiconConfiguration} instance.  Used to determine
     * how to encode and decode terms in the key space.
    public ILexiconConfiguration getLexiconConfiguration() {
        return lexiconConfiguration;
    }
     */
    
}
