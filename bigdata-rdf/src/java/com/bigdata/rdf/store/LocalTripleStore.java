/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jan 3, 2007
 */

package com.bigdata.rdf.store;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.CognitiveWeb.extser.LongPacker;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.sail.SailException;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.AbstractTupleFilterator.CompletionScan;
import com.bigdata.btree.KeyBuilder.StrengthEnum;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.inf.AbstractRuleNestedSubquery;
import com.bigdata.rdf.inf.IN;
import com.bigdata.rdf.inf.Id;
import com.bigdata.rdf.inf.InferenceEngine;
import com.bigdata.rdf.inf.Pred;
import com.bigdata.rdf.inf.Rule;
import com.bigdata.rdf.inf.Triple;
import com.bigdata.rdf.inf.Rule.Var;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.SPOBlockingBuffer;
import com.bigdata.rdf.util.RdfKeyBuilder;
import com.bigdata.search.FullTextIndex;
import com.bigdata.service.DataService;
import com.bigdata.service.LocalDataServiceClient;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * A triple store based on the <em>bigdata</em> architecture.
 * 
 * FIXME This class will probably be replaced by the use of the
 * {@link ScaleOutTripleStore} and the {@link LocalDataServiceClient}. This
 * combination will support full concurrency on local indices on the same
 * {@link DataService} and therefore should offer all of the performance
 * advantages once the JOINs are optimized, the one place where it will always
 * remain weak in comparison is when performing index point tests or batch API
 * operations for small N. Those cases will always incur a higher overhead.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LocalTripleStore extends AbstractLocalTripleStore implements ITripleStore {

    protected final Journal store;

    public Journal getStore() {
        
        return store;
        
    }
    
    /*
     * At this time is is valid to hold onto a reference during a given load or
     * query operation but not across commits (more accurately, not across
     * overflow() events). Eventually we will have to put a client api into
     * place that hides the routing of btree api operations to the journals and
     * segments for each index partition.
     */
    private IIndex ndx_term2Id;
    private IIndex ndx_id2Term;
    private IIndex ndx_spo;
    private IIndex ndx_pos;
    private IIndex ndx_osp;
    private IIndex ndx_just;
    
    /*
     * Note: At this time it is valid to hold onto a reference during a given
     * load or query operation but not across commits (more accurately, not
     * across overflow() events).
     */
    
    final public IIndex getTerm2IdIndex() {

        if(!lexicon) return null;
        
        if(ndx_term2Id!=null) return ndx_term2Id;
        
        IIndex ndx = store.getIndex(name_term2Id);
        
        if (ndx == null) {
            
            ndx_term2Id = ndx = store.registerIndex(name_term2Id, BTree.create(
                    store, getTerm2IdIndexMetadata(name_term2Id)));
            
        }
        
        return ndx;
        
    }

    final public IIndex getId2TermIndex() {

        if(!lexicon) return null;

        if (ndx_id2Term != null)
            return ndx_id2Term;

        IIndex ndx = store.getIndex(name_id2Term);

        if (ndx == null) {

            ndx_id2Term = ndx = store.registerIndex(name_id2Term, BTree.create(
                    store, getId2TermIndexMetadata(name_id2Term)));

        }

        return ndx;

    }

    /**
     * Returns and creates iff necessary a scalable restart safe index for RDF
     * {@link _Statement statements}.
     * 
     * @param name
     *            The name of the index.
     * 
     * @return The index.
     * 
     * @see #name_spo
     * @see #name_pos
     * @see #name_osp
     */
    private IIndex getStatementIndex(String name) {

        IIndex ndx = store.getIndex(name);

        if (ndx == null) {
                
                ndx = store.registerIndex(name, BTree.create(store,
                    getStatementIndexMetadata(name)));

        }

        return ndx;

    }

    final public IIndex getSPOIndex() {

        if(ndx_spo!=null) return ndx_spo;

        return ndx_spo = getStatementIndex(name_spo);
        
    }
    
    final public IIndex getPOSIndex() {

        if(ndx_pos!=null) return ndx_pos;

        return ndx_pos = getStatementIndex(name_pos);

    }
    
    final public IIndex getOSPIndex() {
        
        if(ndx_osp!=null) return ndx_osp;

        return ndx_osp = getStatementIndex(name_osp);

    }

    final public IIndex getJustificationIndex() {

        if (ndx_just != null)
            return ndx_just;

        IIndex ndx = store.getIndex(name_just);

        if (ndx == null) {
            
            ndx_just = ndx = store.registerIndex(name_just, BTree.create(store,
                    getJustIndexMetadata(name_just)));

        }

        return ndx;

    }

    /**
     * The backing embedded database.
     */
    final public IJournal getJournal() {
        
        return store;
        
    }
    
    /**
     * Delegates the operation to the backing store.
     */
    final public void commit() {
     
        final long begin = System.currentTimeMillis();

        store.commit();

        final long elapsed = System.currentTimeMillis() - begin;

        if (log.isInfoEnabled())
            log.info("commit: commit latency="+elapsed+"ms");

    }

    final public void abort() {
                
        super.abort();
        
        /*
         * Discard the hard references to the indices since they may have
         * uncommitted writes. The indices will be re-loaded from the store the
         * next time they are used.
         */
        
        ndx_term2Id = null;
        ndx_id2Term = null;
        ndx_spo = null;
        ndx_pos = null;
        ndx_osp = null;
        ndx_just = null;
        
        // discard the write sets.
        store.abort();

    }
    
    final public boolean isStable() {
        
        return store.isStable();
        
    }
    
    final public boolean isReadOnly() {
        
        return store.isReadOnly();
        
    }

    final public void clear() {
        
        if(lexicon) {
        
            store.dropIndex(name_id2Term); ndx_id2Term = null;
            store.dropIndex(name_term2Id); ndx_term2Id = null;
            
            if(textIndex) {
                
                getSearchEngine().clear();
                
            }
            
        }

        if(oneAccessPath) {
        
            store.dropIndex(name_spo); ndx_spo = null;
            
        } else {
            
            store.dropIndex(name_spo); ndx_spo = null;
            store.dropIndex(name_pos); ndx_pos = null;
            store.dropIndex(name_osp); ndx_osp = null;
            
        }
        
        if (justify) {

            store.dropIndex(name_just); ndx_just = null;
            
        }
        
        registerIndices();
        
    }
    
    final public void close() {
        
        super.close();
        
        store.shutdown();
        
    }
    
    final public void closeAndDelete() {
        
        super.closeAndDelete();
        
        store.closeAndDelete();
        
    }

    /**
     * Options understood by the {@link LocalTripleStore}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends AbstractTripleStore.Options {
       
    }
    
    /**
     * Create or re-open a triple store using a local embedded database.
     */
    public LocalTripleStore(Properties properties) {

        super(properties);
        
        store = new /*Master*/Journal(properties);

        registerIndices();
        
    }
    
    /**
     * Note: This may be used to force eager registration of the indices such
     * that they are always on hand for {@link #asReadCommittedView()}.
     */
    protected void registerIndices() {

        getTerm2IdIndex();
        
        getId2TermIndex();
        
        getSPOIndex();
        
        getPOSIndex();
        
        getOSPIndex();
        
        getJustificationIndex();
        
        if (textIndex)
            getSearchEngine();
        
        /*
         * Note: A commit is required in order for a read-committed view to have
         * access to the registered indices.
         */
        
        commit();
        
    }

    private WeakReference<ReadCommittedTripleStore> readCommittedRef;
    
    /**
     * A factory returning the singleton read-committed view of the database.
     */
    public ReadCommittedTripleStore asReadCommittedView() {

        synchronized(this) {
        
            ReadCommittedTripleStore view = readCommittedRef == null ? null
                    : readCommittedRef.get();
            
            if(view == null) {
                
                view = new ReadCommittedTripleStore(this);
                
                readCommittedRef = new WeakReference<ReadCommittedTripleStore>(view);
                
            }
            
            return view; 
        
        }
        
    }
    
    /**
     * This store is NOT safe for concurrent operations.
     */
    public boolean isConcurrent() {

        return false;
        
    }

    /**
     * JOIN supporting {@link LocalTripleStore#match(Literal[], URI[], URI)}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class MatchRule extends AbstractRuleNestedSubquery {

        public MatchRule(InferenceEngine inf,Var lit, long[] preds, Id cls) {

        super(  new Triple(var("s"), var("t"), lit), //
                new Pred[] { //
                    new Triple(var("s"), var("p"), lit),//
                    new Triple(var("s"), inf.rdfType, var("t"),ExplicitSPOFilter.INSTANCE), //
                    new Triple(var("t"), inf.rdfsSubClassOf, cls) //
                    },
                new IConstraint[] {
                    new IN(var("p"), preds) // p IN preds
                    });

        }
        
    }

    /**
     * Specialized JOIN operator using the full text index to identify possible
     * completions of the given literals for which there exists a subject
     * <code>s</code> such that:
     * 
     * <pre>
     * SELECT ?s, ?t, ?lit
     *     (?lit completionOf, lits)
     * AND (?s ?p ?lit)
     * AND (?s rdf:type ?t)
     * AND (?t rdfs:subClassOf cls)
     * WHERE
     *     p IN {preds}
     * 
     * 
     * </pre>
     * 
     * Note: The JOIN runs asynchronously.
     * 
     * @param lits
     *            One or more literals. The completions of these literals will
     *            be discovered using the {@link FullTextIndex}. (A completion
     *            is any literal having one of the given literals as a prefix
     *            and includes an exact match on the litteral as a degenerate
     *            case.)
     * @param preds
     *            One or more predicates that link the subjects of interest to
     *            the completions of the given literals. Typically this array
     *            will include <code>rdf:label</code>.
     * @param cls
     *            All subjects visited by the iterator will be instances of this
     *            class.
     * 
     * @return An iterator visiting bindings sets. Each binding set will have
     *         bound values for <code>s</code>, <code>t</code>, and
     *         <code>lit</code> where those variables are defined per the
     *         pseudo-code JOIN above.
     * 
     * @throws InterruptedException
     *             if the operation is interrupted.
     * 
     * @todo Re-factor into an operation that can run against the scale-out
     *       indices (the {@link #completionScan(Literal[])} presumes that it
     *       has a local {@link BTree}).
     */
    public Iterator<Map<String, Value>> match(Literal[] lits, URI[] preds,
            URI cls) {
       
        if (lits == null || lits.length == 0)
            throw new IllegalArgumentException();

        if (preds == null || preds.length == 0)
            throw new IllegalArgumentException();
        
        if (cls == null)
            throw new IllegalArgumentException();
        
        /*
         * Translate the predicates into term identifiers.
         * 
         * @todo batch translate.
         */
        final long[] _preds = new long[preds.length];
        {
            
            int nknown = 0;
            
            for(int i=0; i<preds.length; i++) {
                
                final long tid = getTermId(preds[i]);

                if (tid != NULL)
                    nknown++;
                
                _preds[i] = tid;
                
            }
            
            if (nknown == 0) {
                
                log.warn("No known predicates: preds="+preds);
                
                return EmptyIterator.DEFAULT;
                
            }
            
        }
        
        /*
         * Translate the class constraint into a term identifier.
         * 
         * @todo batch translate with the predicates (above).
         */
        final long _cls = getTermId(cls);
        
        if (_cls == NULL) {
         
            log.warn("Unknown class: class="+cls);
            
            return EmptyIterator.DEFAULT;
            
        }

        final Iterator<Long> termIdIterator = completionScan(lits);

//        // the subject.
//        final Var s = Rule.var("s");
//
//        // an explicit type for that subject.
//        final Var t= Rule.var("t");

        // the term identifier for the completed literal.
        final Var lit = Rule.var("lit");

        // bindings used to specialize the rule for each completed literal.
        final Map<Var,Long> bindings = new HashMap<Var,Long>();

        // instantiate the rule.
        final Rule r = new MatchRule(getInferenceEngine(), lit, _preds, new Id(_cls));


        /*
         * Buffer on which the rule will write. The caller will pull from the
         * buffer. If the buffer is full, then the rules writing on the buffer
         * will block until the caller reads another result from the buffer.
         * 
         * @todo config capacity parameter.
         */
        final SPOBlockingBuffer buffer = new SPOBlockingBuffer(this,
                null /* filter */, 10000/* capacity */);

        final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>(); 
        
        // specialize and apply to each completed literal.
        while (termIdIterator.hasNext()) {

            final long tid = termIdIterator.next();

            bindings.put(lit, tid);
            
            final Rule tmp = r.specialize(bindings, null/*constraints*/);
            
            tasks.add(new Callable<Void>() {

                public Void call() {

                    // run the rule.
                    tmp.apply(false/* justify */, null/* focusStore */,
                            LocalTripleStore.this/* database */, buffer);
                    
                    return null;

                }
            });

        }

        /*
         * Now submit one task that will run everything in [tasks]. This will
         * log errors but since it runs asynchronously the caller will not see
         * any exceptions thrown.
         */
        /*Future f = */getThreadPool().submit(new Runnable() {

            public void run() {

                final List<Future<Void>> futures;
                try {

                    if(log.isInfoEnabled()) {

                        log.info("Running "+tasks.size()+" rules");
                        
                    }
                    
                    /*
                     * Note: This will DEADLOCK if the thread pool has only a
                     * single worker thread (there should always be more than
                     * one worker) since the tasks are being submitted for
                     * execution by one worker and there needs to be at least
                     * one additional worker for the tasks to progress.
                     * 
                     * The timeout guarentees that the task will eventually
                     * abort rather than deadlock.
                     */
                    
                    futures = getThreadPool().invokeAll(tasks, 10,
                            TimeUnit.SECONDS);
                    
                    if(log.isInfoEnabled()) {
                        
                        log.info("Ran "+tasks.size()+" rules");
                        
                    }
                    
                } catch (InterruptedException e) {
                    
                    log.warn("Interrupted: " + e, e);

                    return;
                    
                } finally {

                    log.info("Closing buffer");
                    
                    /*
                     * Close the buffer on which the rules are writing. This
                     * also closes the iterator on which the caller is reading.
                     */

                    buffer.close();

                }
                
                for(Future<Void> f : futures) {

                    try {

                        f.get();
                        
                    } catch (Throwable t) {

                        log.error("Problem running query: " + t, t);
                        
                    }
                    
                }
                
                log.info("All rules ran Ok.");
                
            }
            
        });

        return new BindingSetIterator(new BigdataStatementIteratorImpl(this,
                buffer.iterator()));
        
    }
    
    /**
     * Converts what appears to be a set of statements into a binding set for
     * {@link LocalTripleStore#match(Literal[], URI[], URI)}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo this should be replaced by a general purpose binding set mechanism
     *       that can supports JOINs on the KB and more general JOINs on bigdata
     *       indices. There is a relationship to the {@link ITupleSerializer}
     *       since the binding set can have a different "shape" / "type" from
     *       the tuples in the indices that participate in the join. There is
     *       also a relationship to the byte[][] keys range iterator variant,
     *       such as the {@link CompletionScan}, since unrolling the inner loop
     *       of the join for efficient distribution against remote index
     *       partitions will require some similar data (a byte[][] keys) and
     *       logic (either advancing the scan using a cursor or in fact using a
     *       series of {@link ITupleIterator} to perform the scans, perhaps in
     *       parallel).
     */
    private class BindingSetIterator implements Iterator<Map<String,Value>> {

        private final BigdataStatementIterator src;

        private boolean open = true;

        public BindingSetIterator(BigdataStatementIterator src) {
            
            if (src == null)
                throw new IllegalArgumentException();
            
            this.src = src;
            
        }
        
        public boolean hasNext() {

            try {

                final boolean hasNext = src.hasNext();
                
                if(!hasNext && open) {
                    
                    open = false;
                    
                    src.close();
                    
                }
                
                return hasNext;
                
            } catch (SailException e) {
                
                throw new RuntimeException(e);
                
            }
            
        }

        public Map<String, Value> next() {

            final Statement s;
            try {

                s = src.next();
                
            } catch (SailException e) {
                
                throw new RuntimeException(e);
                
            }
            
            final Map<String,Value> bindings = new HashMap<String,Value>();
            
            bindings.put("s",s.getSubject());

            bindings.put("t",s.getPredicate());
            
            bindings.put("lit",(Literal)s.getObject());
            
            return bindings;
            
        }

        public void remove() {

            throw new UnsupportedOperationException();

        }
        
    };
    
    /**
     * A completion scan of all literals having any of the given literal as a
     * prefix.
     * 
     * @param lit
     *            A literal.
     * 
     * @return An iterator visiting the term identifiers for the matching
     *         {@link Literal}s.
     */
    public Iterator<Long> completionScan(Literal lit) {

        if (lit == null)
            throw new IllegalArgumentException();
        
        return completionScan(new Literal[] { lit });
        
    }
    
    /**
     * A completion scan of all literals having any of the given literals as a
     * prefix.
     * 
     * @param lits An array of literals.
     * 
     * @return An iterator visiting the term identifiers for the matching
     *         {@link Literal}s.
     */
    public Iterator<Long> completionScan(Literal[] lits) {

        if (lits == null || lits.length == 0)
            throw new IllegalArgumentException();

        if(log.isInfoEnabled()) {

            log.info("#lits="+lits.length);
            
        }

        /*
         * The KeyBuilder used to form the prefix keys.
         * 
         * Note: The prefix keys are formed with IDENTICAL strength. This is
         * necessary in order to match all keys in the index since it causes the
         * secondary characteristics to NOT be included in the prefix key even
         * if they are present in the keys in the index.
         */
        final RdfKeyBuilder prefixKeyBuilder;
        {

            Properties properties = new Properties();

            properties.setProperty(KeyBuilder.Options.STRENGTH,
                    StrengthEnum.Primary.toString());

            prefixKeyBuilder = new RdfKeyBuilder(KeyBuilder
                    .newUnicodeInstance(properties));

        }

        /*
         * Formulate the keys[].
         * 
         * Note: Each key is encoded with the appropriate bytes to indicate the
         * kind of literal (plain, languageCode, or datatype literal).
         * 
         * Note: The key builder was choosen to only encode the PRIMARY
         * characteristics so that we obtain a prefix[] suitable for the
         * completion scan.
         */

        final byte[][] keys = new byte[lits.length][];

        for (int i = 0; i < lits.length; i++) {

            final Literal lit = lits[i];

            if (lit == null)
                throw new IllegalArgumentException();

            keys[i] = prefixKeyBuilder.value2Key(lit);

        }

        final BTree ndx = (BTree) getTerm2IdIndex();

        final Iterator<Long> termIdIterator = new Striterator(new CompletionScan(//
                (ITupleCursor) ndx
                                .rangeIterator(null/* fromKey */,
                                        null/* toKey */, 0/* capacity */,
                                        IRangeQuery.DEFAULT
                                                | IRangeQuery.CURSOR, null/* filter */), //
                keys)).addFilter(new Resolver() {

                    /**
                     * Decode the value which is the term identifier.
                     */
            @Override
            protected Object resolve(Object arg0) {

                ITuple tuple = (ITuple)arg0;
                
                long tid;
                try {
                    tid = LongPacker.unpackLong(tuple.getValueStream());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                
                return tid;
            }
        });

        return termIdIterator;
            
    }
    
    /**
     * A read-committed view of a read-write triple store. Data committed on the
     * read-write triple store will become visible in this view. The view does
     * NOT support any mutation operations.
     * 
     * @todo The {@link ScaleOutTripleStore} uses unisolated writes so it always
     *       has read-committed semantics so it does not make sense to implement
     *       a read-committed view for that class. However, the manner in which
     *       it chooses the read-behavior point MAY change in order to support
     *       long-running data load operations.
     * 
     * @see LocalTripleStore#asReadCommittedView()
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ReadCommittedTripleStore extends AbstractLocalTripleStore {

        private final LocalTripleStore db;
        
        public IIndexManager getStore() {
            
            return db.getStore();
            
        }
        
        /**
         * Create a read-committed view of a read-write triple store.
         * 
         * @param db
         *            The read-write triple store.
         */
        protected ReadCommittedTripleStore(LocalTripleStore db) {

            super(db.getProperties());
        
            this.db = db;
            
        }

        /**
         * True iff the backing database is stable.
         */
        public boolean isStable() {

            return db.isStable();
            
        }

        /**
         * The view is read-only, but it is updated each time there is a commit
         * against the database (read-committed semantics).
         */
        public boolean isReadOnly() {
            
            return true;
            
        }
        
        /**
         * @throws UnsupportedOperationException always.
         */
        public void abort() {

            throw new UnsupportedOperationException();
            
        }

        /**
         * @throws UnsupportedOperationException always.
         */
        public void clear() {

            throw new UnsupportedOperationException();
            
        }

        /** NOP */
        public void close() {
            
        }

        /**
         * @throws UnsupportedOperationException always.
         */
        public void closeAndDelete() {

            throw new UnsupportedOperationException();
            
        }

        /**
         * @throws UnsupportedOperationException always.
         */
        public void commit() {

            throw new UnsupportedOperationException();
            
        }

        /**
         * @throws UnsupportedOperationException always.
         */
        public long addTerm(Value value) {

            throw new UnsupportedOperationException();
            
        }

        /**
         * This will resolve terms that are pre-existing but will throw an
         * exception if there is an attempt to define a new term.
         * 
         * @throws UnsupportedOperationException
         *             if there is an attempt to write on an index.
         */
        public void addTerms(_Value[] terms, int numTerms) {

            super.addTerms(terms, numTerms);
            
        }

        public IIndex getTerm2IdIndex() {
            
            return db.store.getIndex(name_term2Id, ITx.READ_COMMITTED);
            
        }

        public IIndex getId2TermIndex() {
        
            return db.store.getIndex(name_id2Term, ITx.READ_COMMITTED);

        }

        public IIndex getSPOIndex() {

            return db.store.getIndex(name_spo, ITx.READ_COMMITTED);

        }
        
        public IIndex getPOSIndex() {

            return db.store.getIndex(name_pos, ITx.READ_COMMITTED);
            
        }

        public IIndex getOSPIndex() {

            return db.store.getIndex(name_osp, ITx.READ_COMMITTED);

        }

        public IIndex getJustificationIndex() {

            return db.store.getIndex(name_just, ITx.READ_COMMITTED);

        }

        /**
         * This store is safe for concurrent operations (but it only supports
         * read operations).
         */
        public boolean isConcurrent() {

            return true;
            
        }

        /** FIXME This should be a read-committed view of the search index. */
        public FullTextIndex getSearchEngine() {

            return super.getSearchEngine();
            
//            // ... namespace once used by localtriplestore.
//            final String namespace = "";
//
//            IIndex ndx = db.store.getIndex(namespace+name_freeText, ITx.READ_COMMITTED);
//
//            return new FullTextIndex(getProperties(), namespace,
//                    getStore(), Executors
//                            .newSingleThreadExecutor(DaemonThreadFactory
//                                    .defaultThreadFactory()));
            
        }
        
    }

}
