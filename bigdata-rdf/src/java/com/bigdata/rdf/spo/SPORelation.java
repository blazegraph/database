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
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.LongAggregator;
import com.bigdata.btree.IDataSerializer.NoDataSerializer;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLock;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.WriteJustificationsProc.WriteJustificationsProcConstructor;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.relation.AbstractRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IChunkedIterator;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.accesspath.IKeyOrder;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.relation.rule.Var;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.AbstractSolutionBuffer.InsertSolutionBuffer;
import com.bigdata.resources.DefaultSplitHandler;
import com.bigdata.service.DataService;
import com.bigdata.service.IClientIndex;

/**
 * The {@link SPORelation} handles all things related to the indices
 * representing the triples stored in the database. Statements are first
 * converted to term identifiers using the {@link LexiconRelation} and then
 * inserted into the statement indices in parallel. There is one statement index
 * for each of the three possible access paths for a triple store. The key is
 * formed from the corresponding permutation of the subject, predicate, and
 * object, e.g., {s,p,o}, {p,o,s}, and {o,s,p}. The statement type (inferred,
 * axiom, or explicit) and the optional statement identifer are stored under the
 * key. All state for a statement is replicated in each of the statement
 * indices.
 * 
 * @todo When materializing a relation, such as the {@link SPORelation} or the
 *       {@link LexiconRelation}, on a {@link DataService} we may not want to
 *       have all indices resolved eager. The {@link AbstractTask} will actually
 *       return <code>null</code> rather than throwing an exception, but eager
 *       resolution of the indices will force {@link IClientIndex}s to spring
 *       into existance when we might only want a single index for the relation.
 * 
 * @todo integration with package providing magic set rewrites of rules in order
 *       to test whether or not a statement is still provable when it is
 *       retracted during TM. this will reduce the cost of loading data, since
 *       much of that is writing the justifications index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPORelation extends AbstractRelation<SPO> {

    protected static final Logger log = Logger.getLogger(SPORelation.class);
    
    private transient final long NULL = IRawTripleStore.NULL;
    
    private final Set<String> indexNames;
    
    public SPORelation(IIndexManager indexManager,
            String namespace, Long timestamp, Properties properties) {

        super(indexManager, namespace, timestamp, properties);

        {
            
            /*
             * Reads off the property for the inference engine that tells us
             * whether or not the justification index is being used. This is
             * used to conditionally enable the logic to retract justifications
             * when the corresponding statements is retracted.
             */

            this.justify = Boolean.parseBoolean(properties.getProperty(
                    Options.JUSTIFY, Options.DEFAULT_JUSTIFY));

            log.info(Options.JUSTIFY + "=" + justify);
            
        }
        
        {
        
            this.oneAccessPath = Boolean.parseBoolean(properties.getProperty(
                    Options.ONE_ACCESS_PATH, Options.DEFAULT_ONE_ACCESS_PATH));

            log.info(Options.ONE_ACCESS_PATH + "=" + oneAccessPath);
            
        }

        {
        
            this.statementIdentifiers = Boolean.parseBoolean(properties
                .getProperty(Options.STATEMENT_IDENTIFIERS,
                        Options.DEFAULT_STATEMENT_IDENTIFIERS));

            log.info(Options.STATEMENT_IDENTIFIERS + "=" + statementIdentifiers);
            
        }

        {

            branchingFactor = Integer
                    .parseInt(properties.getProperty(Options.BRANCHING_FACTOR,
                            Options.DEFAULT_BRANCHING_FACTOR));

            if (branchingFactor < BTree.MIN_BRANCHING_FACTOR) {

                throw new IllegalArgumentException(Options.BRANCHING_FACTOR
                        + " must be at least " + BTree.MIN_BRANCHING_FACTOR);

            }

            log.info(Options.BRANCHING_FACTOR + "=" + branchingFactor);

        }

        {
            
            final Set<String> set = new HashSet<String>();

            if(oneAccessPath) {

                set.add(getFQN(SPOKeyOrder.SPO));
                
            } else {

                set.add(getFQN(SPOKeyOrder.SPO));

                set.add(getFQN(SPOKeyOrder.POS));

                set.add(getFQN(SPOKeyOrder.OSP));
            
            }
            
            if(justify) {
             
                set.add(getNamespace()+NAME_JUST);
                
            }

            this.indexNames = Collections.unmodifiableSet(set);

        }
        
        /*
         * Note: if full transactions are to be used then the statement indices
         * and the justification indices should be assigned the transaction
         * identifier.
         */

        if(oneAccessPath) {
            
            spo      = indexManager.getIndex(getFQN(SPOKeyOrder.SPO), timestamp);
            pos      = null;
            osp      = null;
            
        } else {
            
            spo      = indexManager.getIndex(getFQN(SPOKeyOrder.SPO), timestamp);
            pos      = indexManager.getIndex(getFQN(SPOKeyOrder.POS), timestamp);
            osp      = indexManager.getIndex(getFQN(SPOKeyOrder.OSP), timestamp);
            
        }

        if(justify) {

            just     = indexManager.getIndex(getNamespace()+NAME_JUST, timestamp);
            
        } else {
            
            just = null;
            
        }

    }

    /**
     * Strengthened return type.
     */
    public AbstractTripleStore getContainer() {
        
        return (AbstractTripleStore)super.getContainer();
        
    }

    public boolean exists() {
        
        if (oneAccessPath && spo == null)
            return false;
        
        if (spo == null || pos == null || osp == null)
            return false;
        
        if (justify && just == null)
            return false;
        
        return true;
        
    }
    
    /*
     * @todo create missing indices rather than throwing an exception if an
     * index does not exist? (if a statement index is missing, then it really
     * needs to be rebuilt from one of the other statement indics. if you loose
     * the justifications index then you need to re-compute the database at once
     * closure of the store).
     */
    public void create() {

        final IResourceLock resourceLock = getIndexManager()
                .getResourceLockManager().acquireExclusiveLock(getNamespace());

        try {

            // create the relation declaration metadata.
            super.create();

            final IIndexManager indexManager = getIndexManager();

            if (oneAccessPath) {

                final IndexMetadata spoMetadata = getStatementIndexMetadata(SPOKeyOrder.SPO);

                indexManager.registerIndex(spoMetadata);

                spo = indexManager.getIndex(getFQN(SPOKeyOrder.SPO),
                        getTimestamp());

            } else {

                final IndexMetadata spoMetadata = getStatementIndexMetadata(SPOKeyOrder.SPO);

                final IndexMetadata posMetadata = getStatementIndexMetadata(SPOKeyOrder.POS);

                final IndexMetadata ospMetadata = getStatementIndexMetadata(SPOKeyOrder.OSP);

                indexManager.registerIndex(spoMetadata);

                indexManager.registerIndex(posMetadata);

                indexManager.registerIndex(ospMetadata);

                spo = indexManager.getIndex(getFQN(SPOKeyOrder.SPO),
                        getTimestamp());

                pos = indexManager.getIndex(getFQN(SPOKeyOrder.POS),
                        getTimestamp());

                osp = indexManager.getIndex(getFQN(SPOKeyOrder.OSP),
                        getTimestamp());

            }

            if (justify) {

                final IndexMetadata justMetadata = getJustIndexMetadata(getNamespace()
                        + NAME_JUST);

                indexManager.registerIndex(justMetadata);

                just = indexManager.getIndex(getNamespace() + NAME_JUST,
                        getTimestamp());

            }

        } finally {

            resourceLock.unlock();

        }

    }

    /*
     * @todo force drop of all indices rather than throwing an exception if an
     * index does not exist?
     */
    public void destroy() {

        final IResourceLock resourceLock = getIndexManager()
                .getResourceLockManager().acquireExclusiveLock(getNamespace());

        try {

            final IIndexManager indexManager = getIndexManager();

            if (oneAccessPath) {

                indexManager.dropIndex(getFQN(SPOKeyOrder.SPO));
                spo = null;

            } else {

                indexManager.dropIndex(getFQN(SPOKeyOrder.SPO));
                spo = null;

                indexManager.dropIndex(getFQN(SPOKeyOrder.POS));
                pos = null;

                indexManager.dropIndex(getFQN(SPOKeyOrder.OSP));
                osp = null;

            }

            if (justify) {

                indexManager.dropIndex(getNamespace() + NAME_JUST);
                just = null;

            }

            // destroy the relation declaration metadata.
            super.destroy();

        } finally {

            resourceLock.unlock();

        }
        
    }
    private IIndex spo;
    private IIndex pos;
    private IIndex osp;
    private IIndex just;

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
     * The branching factor for indices registered by this class.
     * 
     * @see com.bigdata.journal.Options#BRANCHING_FACTOR
     */
    final protected int branchingFactor;

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

    final public IIndex getSPOIndex() {

        if (spo == null)
            throw new IllegalStateException();

        return spo;

    }

    final public IIndex getPOSIndex() {

        if (oneAccessPath)
            return null;

        if (pos == null)
            throw new IllegalStateException();

        return pos;

    }

    final public IIndex getOSPIndex() {

        if (oneAccessPath)
            return null;
        
        if (osp == null)
            throw new IllegalStateException();

        return osp;

    }

    final public IIndex getJustificationIndex() {

        if (!justify)
            return null;

        if (just == null)
            throw new IllegalStateException();

        return just;

    }
    
    public String getFQN(IKeyOrder<? extends SPO> keyOrder) {
        
        return getNamespace() + ((SPOKeyOrder)keyOrder).getIndexName();
        
    }
    
    /**
     * Shared {@link IndexMetadata} configuration.
     * 
     * @param name
     *            The index name.
     * 
     * @return A new {@link IndexMetadata} object for that index.
     */
    protected IndexMetadata getIndexMetadata(String name) {

        final IndexMetadata metadata = new IndexMetadata(name, UUID.randomUUID());

        metadata.setBranchingFactor(branchingFactor);

        /*
         * Note: Mainly used for torture testing.
         */
        if(false){
            
            // An override that makes a split very likely.
            final ISplitHandler splitHandler = new DefaultSplitHandler(
                    10 * Bytes.kilobyte32, // minimumEntryCount
                    50 * Bytes.kilobyte32, // entryCountPerSplit
                    1.5, // overCapacityMultiplier
                    .75, // underCapacityMultiplier
                    20 // sampleRate
            );
            
            metadata.setSplitHandler(splitHandler);
            
        }
                
        return metadata;

    }

    /**
     * Overrides for the statement indices.
     */
    protected IndexMetadata getStatementIndexMetadata(SPOKeyOrder keyOrder) {

        final IndexMetadata metadata = getIndexMetadata(getFQN(keyOrder));

        metadata.setLeafKeySerializer(FastRDFKeyCompression.N3);

        if (!statementIdentifiers) {

            /*
             * FIXME this value serializer does not know about statement
             * identifiers. Therefore it is turned off if statement identifiers
             * are enabled. Examine some options for value compression for the
             * statement indices when statement identifiers are enabled.
             */

            metadata.setLeafValueSerializer(new FastRDFValueCompression());

        }
        
        metadata.setTupleSerializer(new SPOTupleSerializer(keyOrder));

        return metadata;

    }

    /**
     * Overrides for the {@link IRawTripleStore#getJustificationIndex()}.
     */
    protected IndexMetadata getJustIndexMetadata(String name) {

        final IndexMetadata metadata = getIndexMetadata(name);

        metadata.setLeafValueSerializer(NoDataSerializer.INSTANCE);

        metadata.setTupleSerializer(new JustificationTupleSerializer(IRawTripleStore.N));

        return metadata;

    }

    public Set<String> getIndexNames() {

        return indexNames;
        
    }
    
    /**
     * 
     * @param s
     * @param p
     * @param o
     */
    public IAccessPath<SPO> getAccessPath(final long s, final long p, final long o) {
     
        return getAccessPath(s, p, o, null/*filter*/);
        
    }

    /**
     * 
     * @param s
     * @param p
     * @param o
     * @param filter Optional filter to be evaluated close to the data.
     * @return
     */
    @SuppressWarnings("unchecked")
    public IAccessPath<SPO> getAccessPath(final long s, final long p,
            final long o, IElementFilter<SPO> filter) {

//      return new AccessPath(this, KeyOrder.get(s, p, o), s, p, o);

        final IVariableOrConstant<Long> S = (s == NULL ? Var.var("s")
                : new Constant<Long>(s));

        final IVariableOrConstant<Long> P = (p == NULL ? Var.var("p")
                : new Constant<Long>(p));

        final IVariableOrConstant<Long> O = (o == NULL ? Var.var("o")
                : new Constant<Long>(o));
        
        return getAccessPath(new SPOPredicate(new String[] { getNamespace() },
                S, P, O, filter));
        
    }

    /**
     * Return the {@link IAccessPath} that is most efficient for the specified
     * predicate based on an analysis of the bound and unbound positions in the
     * predicate.
     * 
     * @return The best access path for that predicate.
     */
    public IAccessPath<SPO> getAccessPath(final IPredicate<SPO> predicate) {

        if (predicate == null)
            throw new IllegalArgumentException();
        
        final long s = predicate.get(0).isVar() ? NULL : (Long) predicate.get(0).get();
        final long p = predicate.get(1).isVar() ? NULL : (Long) predicate.get(1).get();
        final long o = predicate.get(2).isVar() ? NULL : (Long) predicate.get(2).get();

        final IAccessPath<SPO> accessPath;
        
        if (s != NULL && p != NULL && o != NULL) {

            accessPath = getAccessPath(SPOKeyOrder.SPO, predicate);

        } else if (s != NULL && p != NULL) {

            accessPath = getAccessPath(SPOKeyOrder.SPO, predicate);

        } else if (s != NULL && o != NULL) {

            accessPath = getAccessPath(SPOKeyOrder.OSP, predicate);

        } else if (p != NULL && o != NULL) {

            accessPath = getAccessPath(SPOKeyOrder.POS, predicate);

        } else if (s != NULL) {

            accessPath = getAccessPath(SPOKeyOrder.SPO, predicate);

        } else if (p != NULL) {

            accessPath = getAccessPath(SPOKeyOrder.POS, predicate);

        } else if (o != NULL) {

            accessPath = getAccessPath(SPOKeyOrder.OSP, predicate);

        } else {

            accessPath = getAccessPath(SPOKeyOrder.SPO, predicate);

        }
        
        if (log.isDebugEnabled()) {

            log.debug(accessPath.toString());
            
        }
        
        return accessPath;
        
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
     * @return The access path.
     */
    public SPOAccessPath getAccessPath(IKeyOrder<SPO> keyOrder,
            IPredicate<SPO> predicate) {

        if (keyOrder == null)
            throw new IllegalArgumentException();
        
        if (predicate == null)
            throw new IllegalArgumentException();
        
        final IIndex ndx = getIndex(keyOrder);

        if (ndx == null) {
        
            throw new IllegalArgumentException("no index? relation="
                    + getNamespace() + ", timestamp=" + getTimestamp()
                    + ", keyOrder=" + keyOrder + ", pred=" + predicate);
            
        }
        
        final int flags = IRangeQuery.KEYS | IRangeQuery.VALS;
        
        return new SPOAccessPath(this, predicate, keyOrder, ndx, flags).init();
        
    }
    
    public long getElementCount(boolean exact) {

        final IIndex ndx = getIndex(SPOKeyOrder.SPO);
        
        if (exact) {
        
            return ndx.rangeCountExact(null/* fromKey */, null/* toKey */);
            
        } else {
            
            return ndx.rangeCount(null/* fromKey */, null/* toKey */);
            
        }
        
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
    public IChunkedIterator<Long> distinctTermScan(IKeyOrder<SPO> keyOrder) {

        return new DistinctTermScanner(getExecutorService(), getIndex(keyOrder))
                .iterator();

    }
    
    @SuppressWarnings("unchecked")
    public SPO newElement(IPredicate predicate, IBindingSet bindingSet) {

        if (predicate == null)
            throw new IllegalArgumentException();
        
        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final IPredicate<ISPO> pred = (IPredicate<ISPO>) predicate;

        final long s = asBound(pred, 0, bindingSet);

        final long p = asBound(pred, 1, bindingSet);

        final long o = asBound(pred, 2, bindingSet);

        final SPO spo = new SPO(s, p, o, StatementEnum.Inferred);
        
        if(log.isDebugEnabled()) {
            
            log.debug(spo.toString());
            
        }
        
        return spo;
        
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
    private long asBound(IPredicate<ISPO> pred, int index, IBindingSet bindingSet) {

        final IVariableOrConstant<Long> t = pred.get(index);

        final IConstant<Long> c;
        if(t.isVar()) {
            
            c = bindingSet.get((IVariable) t);
            
        } else {
            
            c = (IConstant<Long>)t;
            
        }

        return c.get().longValue();

    }
    
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
    public long insert(IChunkedOrderedIterator<SPO> itr) {

        try {
            
            long n = 0;
            
            while(itr.hasNext()) {
                
                final SPO[] a = itr.nextChunk();
                
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
     * Note: This does NOT write on the justifications index. If justifications
     * are being maintained then the {@link ISolution}s MUST report binding
     * sets and an {@link InsertSolutionBuffer} MUST be used that knows how to
     * write on the justifications index AND delegate writes on the statement
     * indices to this method.
     * <p>
     * Note: This does NOT perform truth maintenance!
     * <p>
     * Note: This does NOT compute the closure for statement identifiers
     * (statements that need to be deleted because they are about a statement
     * that is being deleted).
     * 
     * @see AbstractTripleStore#removeStatements(IChunkedOrderedIterator, boolean)
     * @see SPOAccessPath#removeAll()
     */
    public long delete(IChunkedOrderedIterator<SPO> itr) {

        try {
            
            long n = 0;
            
            while(itr.hasNext()) {
                
                final SPO[] a = itr.nextChunk();
                
                n += delete(a, a.length);
                
            }
         
            return n;
            
        } finally {
            
            itr.close();
            
        }
        
    }

    /**
     * Note: The statements are inserted into each index in parallel. We clone
     * the statement[] and sort and bulk load each statement index in parallel
     * using a thread pool.
     * 
     * @param a
     *            An {@link SPO}[].
     * @param numStmts
     *            The #of elements of that array that will be written.
     * @param filter
     *            An optional filter on the elements to be written.
     * 
     * @return The mutation count.
     * 
     * @todo raise the filter into the caller?
     */
    public long insert(SPO[] a, int numStmts, IElementFilter<SPO> filter) {

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

        tasks.add(new SPOIndexWriter(this, a, numStmts, false/* clone */,
                SPOKeyOrder.SPO, filter, sortTime, insertTime, mutationCount));

        if (!oneAccessPath) {

            tasks.add(new SPOIndexWriter(this, a, numStmts, true/* clone */,
                    SPOKeyOrder.POS, filter, sortTime, insertTime, mutationCount));

            tasks.add(new SPOIndexWriter(this, a, numStmts, true/* clone */,
                    SPOKeyOrder.OSP, filter, sortTime, insertTime, mutationCount));

        }

        // if(numStmts>1000) {
        //
        // log.info("Writing " + numStmts + " statements...");
        //                    
        // }

        final List<Future<Long>> futures;
        final long elapsed_SPO;
        final long elapsed_POS;
        final long elapsed_OSP;

        try {

            futures = getExecutorService().invokeAll(tasks);

            elapsed_SPO = futures.get(0).get();
            if (!oneAccessPath) {
                elapsed_POS = futures.get(1).get();
                elapsed_OSP = futures.get(2).get();
            } else {
                elapsed_POS = 0;
                elapsed_OSP = 0;
            }

        } catch (InterruptedException ex) {

            throw new RuntimeException(ex);

        } catch (ExecutionException ex) {

            throw new RuntimeException(ex);

        }

        long elapsed = System.currentTimeMillis() - begin;

        if (numStmts > 1000) {

            log.info("Wrote " + numStmts + " statements (mutationCount="
                    + mutationCount + ") in " + elapsed + "ms" //
                    + "; sort=" + sortTime + "ms" //
                    + ", keyGen+insert=" + insertTime + "ms" //
                    + "; spo=" + elapsed_SPO + "ms" //
                    + ", pos=" + elapsed_POS + "ms" //
                    + ", osp=" + elapsed_OSP + "ms" //
            );

        }

        return mutationCount.get();
        
    }

    /**
     * Delete the {@link SPO}s from the statement indices. Any justifications
     * for those statements will also be deleted.
     * 
     * @param stmts
     *            The {@link SPO}s.
     * @param numStmts
     *            The #of elements in that array to be processed.
     * 
     * @return The #of statements that were removed (mutationCount).
     * 
     * FIXME This needs to return the mutationCount. Resolve what is actually
     * being reported. I expect that {@link BatchRemove} only removes those
     * statements that it finds and that there is no constraint in place to
     * assure that this method only sees {@link SPO}s known to exist (but
     * perhaps it does since you can only do this safely for explicit
     * statements).
     */
    public long delete(SPO[] stmts, int numStmts) {
        
        final long begin = System.currentTimeMillis();

        // The time to sort the data.
        final AtomicLong sortTime = new AtomicLong(0);

        // The time to delete the statements from the indices.
        final AtomicLong writeTime = new AtomicLong(0);

        // The mutation count.
        final AtomicLong mutationCount = new AtomicLong(0);

        final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(3);

        tasks.add(new SPOIndexRemover(this, stmts, numStmts,
                SPOKeyOrder.SPO, false/* clone */, sortTime, writeTime));

        if (!oneAccessPath) {

            tasks
                    .add(new SPOIndexRemover(this, stmts, numStmts,
                            SPOKeyOrder.POS, true/* clone */, sortTime,
                            writeTime));

            tasks
                    .add(new SPOIndexRemover(this, stmts, numStmts,
                            SPOKeyOrder.OSP, true/* clone */, sortTime,
                            writeTime));

        }

        if (justify) {

            /*
             * Also retract the justifications for the statements.
             */

            tasks.add(new JustificationRemover(this, stmts, numStmts,
                    true/* clone */, sortTime, writeTime));

        }

        final List<Future<Long>> futures;
        final long elapsed_SPO;
        final long elapsed_POS;
        final long elapsed_OSP;
        final long elapsed_JST;

        try {

            futures = getExecutorService().invokeAll(tasks);

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

        } catch (InterruptedException ex) {

            throw new RuntimeException(ex);

        } catch (ExecutionException ex) {

            throw new RuntimeException(ex);

        }

        long elapsed = System.currentTimeMillis() - begin;

        if (numStmts > 1000) {

            log.info("Removed " + numStmts + " in " + elapsed
                    + "ms; sort=" + sortTime + "ms, keyGen+delete="
                    + writeTime + "ms; spo=" + elapsed_SPO + "ms, pos="
                    + elapsed_POS + "ms, osp=" + elapsed_OSP
                    + "ms, jst=" + elapsed_JST);

        }

        return numStmts;
        
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
    public long addJustifications(IChunkedIterator<Justification> itr) {

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
    public StringBuilder dump(IKeyOrder<SPO> keyOrder) {
        
        final StringBuilder sb = new StringBuilder();
        
        {
            
            final IPredicate<SPO> pred = new SPOPredicate(getNamespace(), Var
                    .var("s"), Var.var("p"), Var.var("o"));

            final IChunkedOrderedIterator<SPO> itr = getAccessPath(keyOrder,
                    pred).iterator();

            try {

                while (itr.hasNext()) {

                    sb.append(itr.next());

                    sb.append("\n");

                }
                
            } finally {

                itr.close();

            }

        }

        return sb;

    }

//    /**
//     * A constant corresponding to {@link IRawTripleStore#NULL}.
//     */
//    private final static transient IConstant<Long> CONST_NULL = new Constant<Long>(
//            IRawTripleStore.NULL);

}
