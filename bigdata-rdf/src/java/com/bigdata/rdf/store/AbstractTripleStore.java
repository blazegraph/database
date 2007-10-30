/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on May 21, 2007
 */

package com.bigdata.rdf.store;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.vocabulary.OWL;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;
import org.openrdf.vocabulary.XmlSchema;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.ReadOnlyFusedView;
import com.bigdata.btree.UnicodeKeyBuilder;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.isolation.IIsolatableIndex;
import com.bigdata.isolation.IsolatableFusedView;
import com.bigdata.journal.ConcurrentJournal;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.Rule;
import com.bigdata.rdf.model.OptimizedValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.rio.IRioLoader;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.RioLoaderEvent;
import com.bigdata.rdf.rio.RioLoaderListener;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPOFilter;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.Justification;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOArrayIterator;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.spo.SPOIterator;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;
import com.bigdata.service.ClientIndexView;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * Abstract base class that implements logic for the {@link ITripleStore}
 * interface that is invariant across the choice of the backing store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractTripleStore implements ITripleStore, IRawTripleStore {

    /**
     * Used to generate the compressed sort keys for the
     * {@link #getTermIdIndex()}.
     */
    final protected RdfKeyBuilder keyBuilder;
    
    /**
     * A copy of properties used to configure the {@link ITripleStore}.
     */
    final protected Properties properties;
    
    final public Properties getProperties() {
        
        /*
         * wrap them up so that people can not easily mess with the initial
         * properties.
         */

        return new Properties(properties);
        
    }
    
    protected AbstractTripleStore(Properties properties) {
        
        // Copy the properties object.
        this.properties = (Properties)properties.clone();
        
        // setup namespace mapping for serialization utility methods.
        addNamespace(RDF.NAMESPACE, "rdf");
        addNamespace(RDFS.NAMESPACE, "rdfs");
        addNamespace(OWL.NAMESPACE, "owl");
        addNamespace(XmlSchema.NAMESPACE, "xsd");

        keyBuilder = new RdfKeyBuilder(new UnicodeKeyBuilder(createCollator(),
                Bytes.kilobyte32));
        
    }
    
    /**
     * @todo Consider making this a {@link ThreadLocal} to avoid hassels with
     *       access by multiple threads. Note however that only the term:id
     *       index requires Unicode support.
     */
    final public RdfKeyBuilder getKeyBuilder() {
        
        return keyBuilder;
        
    }
    
    /**
     * Create and return a new collator object responsible for encoding unicode
     * strings into sort keys.
     * 
     * @return A new collator object.
     * 
     * @todo define the means to configure the key builder for desired unicode
     *       support. the configuration should be restart-safe and must be
     *       shared by all clients for the same triple store. (Actually, I think
     *       that the {@link KeyBuilder} needs to be able to use different
     *       collation sequences for different keys - the main example here is
     *       of course a language code literal where the key contains the
     *       langauge code in order to partition literals using different
     *       language families, and possible different collation sequences, into
     *       different parts of the key space).
     */
    final protected RuleBasedCollator createCollator() {
        
        // choose a collator for the default locale.
        RuleBasedCollator collator = (RuleBasedCollator) Collator
                .getInstance(Locale.getDefault());

        /*
         * Primary uses case folding and produces smaller sort strings.
         * 
         * Secondary does not fold case.
         * 
         * Tertiary is the default.
         * 
         * Identical is also allowed.
         * 
         * @todo handle case folding - currently the indices complain, e.g., for
         * wordnet that a term already exists with a given id "Yellow Pages" vs
         * "yellow pages". Clearly the logic to fold case needs to extend
         * further if it is to work.
         */
//        collator.setStrength(Collator.PRIMARY);
//        collator.setStrength(Collator.SECONDARY);

        return collator;
        
    }

//    /**
//     * Return an object that may be used to buffer {@link Statement}s for batch
//     * operations.
//     * <p>
//     * Note: This is used by the Sesame integration points (RIO parser and SAIL)
//     * to efficiently translate between the {@link Statement} and {@link Value}
//     * object models and the term identifiers and {@link SPO} objects used
//     * internally for operations on the statement indices.
//     * 
//     * @param capacity
//     *            The capacity of the buffer before it will overflow.
//     * 
//     * @return A {@link StatementBuffer}.
//     * 
//     * @see StatementBuffer
//     * 
//     * FIXME {@link StatementBuffer} should support arbitrary chunked batch
//     * operations on overflow, not just insert into the store.  We may need
//     * a pipe (writer, consumer) model for this.
//     */
//    public StatementBuffer getStatementBuffer(int capacity) {
//        
//        if (capacity == 0) {
//            
//            capacity = 100 * Bytes.kilobyte32;
//            
//        }
//        
//        return new StatementBuffer(this,capacity,true/*distinct*/);
//        
//    }
    
    /**
     * A service used to write on each of the statement indices in parallel.
     * 
     * @todo While this provides concurrency on the statement indices, it does
     *       not support concurrent operations that each want to write on the
     *       statement indices. That level of concurrency either requires a
     *       queue for operations to be executed on this service (each operation
     *       runs N tasks, one for each index) or a refactor to use the
     *       {@link ConcurrentJournal}.
     */
    public ExecutorService indexWriteService = Executors.newFixedThreadPool(3,
            DaemonThreadFactory.defaultThreadFactory());

    final public void generateSortKeys(RdfKeyBuilder keyBuilder,
            _Value[] terms, int numTerms) {
        
        for (int i = 0; i < numTerms; i++) {

            _Value term = terms[i];

            if (term.key == null) {

                term.key = keyBuilder.value2Key(term);

            }

        }

    }
    
    final public IIndex getStatementIndex(KeyOrder keyOrder) {

        switch (keyOrder) {
        case SPO:
            return getSPOIndex();
        case POS:
            return getPOSIndex();
        case OSP:
            return getOSPIndex();
        default:
            throw new IllegalArgumentException("Unknown: " + keyOrder);
        }

    }

    final public int getStatementCount() {
        
        return getSPOIndex().rangeCount(null,null);
        
    }
    
    final public int getTermCount() {
        
        return getTermIdIndex().rangeCount(null,null);
        
    }
    
    final public int getURICount() {
        
        byte[] fromKey = keyBuilder.keyBuilder.reset().append(
                RdfKeyBuilder.CODE_URI).getKey();

        byte[] toKey = keyBuilder.keyBuilder.reset().append(
                RdfKeyBuilder.CODE_LIT).getKey();
        
        return getTermIdIndex().rangeCount(fromKey,toKey);
        
    }
    
    final public int getLiteralCount() {
        
        byte[] fromKey = keyBuilder.keyBuilder.reset().append(
                RdfKeyBuilder.CODE_LIT).getKey();

        byte[] toKey = keyBuilder.keyBuilder.reset().append(
                RdfKeyBuilder.CODE_BND).getKey();
        
        return getTermIdIndex().rangeCount(fromKey,toKey);
        
    }
    
    final public int getBNodeCount() {
        
        byte[] fromKey = keyBuilder.keyBuilder.reset().append(
                RdfKeyBuilder.CODE_BND).getKey();

        byte[] toKey = keyBuilder.keyBuilder.reset().append(
                (byte) (RdfKeyBuilder.CODE_BND + 1)).getKey();
        
        return getTermIdIndex().rangeCount(fromKey,toKey);
        
    }

    /**
     * The capacity of the {@link StatementBuffer} used when reading RDF data.
     * The default (1M) is good for the {@link LocalTripleStore}.
     * 
     * @return The buffer capacity.
     */
    protected int getDataLoadBufferCapacity() {
        
        return 1000000;
        
    }
    
    final public LoadStats loadData(String resource, String baseURI,
            RDFFormat rdfFormat, boolean verifyData, boolean commit) throws IOException {

        final long begin = System.currentTimeMillis();
        
        LoadStats stats = new LoadStats();
        
        log.info( "loading: " + resource );

        /*
         * Note: distinct := true has substantially better performance.
         */
        
        IRioLoader loader = new PresortRioLoader(this, rdfFormat, verifyData,
                getDataLoadBufferCapacity(), true /* distinct */);

        loader.addRioLoaderListener( new RioLoaderListener() {
            
            public void processingNotification( RioLoaderEvent e ) {
                
                log.info
                    ( e.getStatementsProcessed() + 
                      " stmts added in " + 
                      ((double)e.getTimeElapsed()) / 1000d +
                      " secs, rate= " + 
                      e.getInsertRate() 
                      );
                
            }
            
        });
        
        /*
         * @todo change to use correct Parser method depending on Reader vs
         * InputStream (SAX Source).  Changing this means updating all of
         * the parser implementations, not just the PresortRioLoader.
         */
        
        InputStream rdfStream = getClass().getResourceAsStream(resource);

        if (rdfStream == null) {

            // If we do not find as a Resource then try the file system.
            rdfStream = new BufferedInputStream(new FileInputStream(resource));

        }

        Reader reader = new BufferedReader(new InputStreamReader(rdfStream));
        
        try {
            
            loader.loadRdf( reader, baseURI );
            
            long nstmts = loader.getStatementsAdded();
            
            stats.toldTriples += nstmts;
            
            stats.loadTime = System.currentTimeMillis() - begin;
            
            // commit the data.
            if(commit) {
                
                long beginCommit = System.currentTimeMillis();
                
                commit();

                stats.commitTime = System.currentTimeMillis() - beginCommit;

                log.info("commit: latency="+stats.commitTime+"ms");
                
            }
            
            stats.totalTime = System.currentTimeMillis() - begin;
            
            log.info( nstmts + 
                    " stmts added in " + 
                    ((double)loader.getInsertTime()) / 1000d +
                    " secs, rate= " + 
                    loader.getInsertRate()
                    );

            return stats;
            
        } catch ( Exception ex ) {
            
            throw new RuntimeException("While loading: "+resource, ex);
            
        } finally {
            
            reader.close();
            
        }
        
//        long elapsed = System.currentTimeMillis() - begin;
//
//        log
//                .info(total_stmts
//                        + " stmts added in "
//                        + ((double) elapsed)
//                        / 1000d
//                        + " secs, rate= "
//                        + ((long) (((double) total_stmts) / ((double) elapsed) * 1000d)));

    }

    final public int addStatements(_Statement[] stmts, int numStmts) {
        
        SPO[] tmp = new SPO[numStmts];
        
        for(int i=0; i<tmp.length; i++) {
            
            _Statement stmt = stmts[i];
            
            tmp[i] = new SPO(stmt.s.termId, stmt.p.termId, stmt.o.termId,
                    stmt.type);
            
        }
        
        return addStatements(tmp, numStmts);

    }
    
    final public void addStatement(Resource s, URI p, Value o) {

        /*
         * Note: This uses the batch API.
         */
        
        StatementBuffer buffer = new StatementBuffer(this,3,false);
        
        buffer.add(s, p, o);
        
        buffer.flush();
                
    }

    /**
     * Return true if the statement exists in the store (non-batch API).
     * <p>
     * Note: This method does not verify whether or not the statement is
     * explicit.
     * 
     * @param s
     *            The term identifier for the subject and not
     *            {@link ITripleStore#NULL}
     * @param p
     *            The term identifier for the predicate and not
     *            {@link ITripleStore#NULL}
     * @param o
     *            The term identifier for the object and not
     *            {@link ITripleStore#NULL}
     */
    final public boolean containsStatement(long s, long p, long o) {

        assert s != NULL;
        assert p != NULL;
        assert o != NULL;
        
        return getSPOIndex().contains(keyBuilder.statement2Key(s, p, o));
        
    }

    final public boolean containsStatement(Resource s, URI p, Value o) {

        s = (Resource) OptimizedValueFactory.INSTANCE.toNativeValue(s);
        p = (URI) OptimizedValueFactory.INSTANCE.toNativeValue(p);
        o = OptimizedValueFactory.INSTANCE.toNativeValue(o);

        /*
         * Resolve each term to its term identifier.
         * 
         * Note: If a value was specified and it is not in the terms index then
         * the statement can not exist in the KB and the code will return
         * [false] immediately.
         * 
         * @todo batch resolve the three terms.
         */

        final long _s = (s == null ? NULL : getTermId(s));

        if (_s == NULL && s != null)
            return false;

        final long _p = (p == null ? NULL : getTermId(p));

        if (_p == NULL && p != null)
            return false;

        final long _o = (o == null ? NULL : getTermId(o));

        if (_o == NULL && o != null)
            return false;
        
        /*
         * if all bound, then a slight optimization.
         */
        if (_s != NULL && _p != NULL && _o != NULL) {

            return getSPOIndex().contains(keyBuilder.statement2Key(_s, _p, _o));
            
        }
        
        /*
         * Choose the access path and test to see if any statements would be
         * visited for that triple pattern.
         */
        return getAccessPath(_s, _p, _o).iterator(1/* limit */, 1/* capacity */)
                .hasNext();
        
    }
    
    /**
     * FIXME define a means to obtain a fused view of the access path selected
     * by a triple pattern in this database and in another triple store, which
     * will typically be a {@link TempTripleStore}. This will be used to
     * support TM so that we can read from a fused view. This can be implemented
     * using a {@link ReadOnlyFusedView} or an {@link IsolatableFusedView}.
     * There will have to be a similar mechanism for the {@link ClientIndexView}.
     */
    final public IAccessPath getAccessPath(long s, long p, long o) {
        
        return new AccessPath(s,p,o);
        
    }
    
    final public IAccessPath getAccessPath(KeyOrder keyOrder) {

        /*
         * Note: An arbitrary non-NULL term identifier is used to select the
         * access path. This means that AccessPath.getTriplePattern() will
         * return the same arbitrary term identifier when the means is used to
         * obtain an IAccessPath.
         */
        
        switch (keyOrder) {
        case SPO:
            return new AccessPath(1,NULL,NULL);
        case POS:
            return new AccessPath(NULL,1,NULL);
        case OSP:
            return new AccessPath(NULL,NULL,1);
        default:
            throw new AssertionError();
        }
        
    }
    
    /**
     * Basic implementation resolves indices dynamically against the outer
     * class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class AccessPath implements IAccessPath {

        /** The triple pattern. */
        final long s, p, o;
        
        final KeyOrder keyOrder;
        
        final byte[] fromKey;
        
        final byte[] toKey;

        public long[] getTriplePattern() {
            
            return new long[]{s,p,o};
            
        }
        
        public IIndex getIndex() {
            
            return getStatementIndex( keyOrder );
            
        }

        public KeyOrder getKeyOrder() {

            return keyOrder;
            
        }
        
        public int rangeCount() {
            
            return getIndex().rangeCount(fromKey,toKey);
            
        }

        public IEntryIterator rangeQuery() {
            
            return getIndex().rangeIterator(fromKey, toKey);
            
        }

        /**
         * FIXME This currently sucks everything into an array. Get traversal
         * with concurrent modification working for {@link IEntryIterator} and
         * then modify this to use {@link SPOIterator}, which uses asynchronous
         * reads and buffers a reasonable number of statements in memory.
         * <p>
         * Note: the {@link Rule}s basically assume that they can traverse the
         * statement indices with concurrent modification of those indices (the
         * {@link SPOBuffer} incrementally flushes the results to the database,
         * which means that it is writing on the statement indices at the same
         * time as we are reading from those indices).
         * <p>
         * However, the underlying {@link BTree} does NOT support traversal with
         * concurrent modification.
         * <p>
         * There are several ways to accomplish this ranging from: (a) the data
         * service, which issues a series of range queries in order to
         * incrementally cover a range and which would support interleaving of
         * range query and update operations; (b) a specialization akin to
         * isolation in which writes are absorbed onto an isolated index while
         * reads traverse the primary index (or writes are absorbed onto the
         * live index while reads traverse a historical version, which does not
         * cause double buffering but means that your writes are not visible to
         * the reader until you commit); and (c) modifying the {@link BTree} to
         * support traversal with concurrent modification, which could be as
         * simple as restarting the iterator from the successor of the last
         * visited key if the tree has been modified.
         */
        public ISPOIterator iterator() {

            /*
             * This is an async incremental iterator that buffers some but not
             * necessarily all statements.
             */
//            return iterator(0/*limit*/, 0/*capacity*/);
            
            /*
             * This is a synchronous read that buffers all statements.
             */
            
            return new SPOArrayIterator(this);
            
        }

        /**
         * Note: Return an iterator that will use transparent read-ahead when no
         * limit is specified (limit is zero) or the limit is "small".
         * 
         * @see SPOIterator
         */
        public ISPOIterator iterator(int limit, int capacity) {

            boolean async = true;
            
            return new SPOIterator(this, limit, capacity, async);

        }
        
        /**
         * Chooses the best access path for the given triple pattern.
         * 
         * @param s
         *            The term identifier for the subject -or-
         *            {@link ITripleStore#NULL}.
         * @param p
         *            The term identifier for the predicate -or-
         *            {@link ITripleStore#NULL}.
         * @param o
         *            The term identifier for the object -or-
         *            {@link ITripleStore#NULL}.
         */
        public AccessPath(long s, long p, long o) {

            this.s = s;
            
            this.p = p;
            
            this.o = o;
            
            if (s != NULL && p != NULL && o != NULL) {

                keyOrder = KeyOrder.SPO;
                
                fromKey = keyBuilder.statement2Key(s, p, o);

                toKey = keyBuilder.statement2Key(s, p, o + 1);

            } else if (s != NULL && p != NULL) {

                keyOrder = KeyOrder.SPO;
                
                fromKey = keyBuilder.statement2Key(s, p, NULL);

                toKey = keyBuilder.statement2Key(s, p + 1, NULL);

            } else if (s != NULL && o != NULL) {

                keyOrder = KeyOrder.OSP;
                
                fromKey = keyBuilder.statement2Key(o, s, NULL);

                toKey = keyBuilder.statement2Key(o, s + 1, NULL);

            } else if (p != NULL && o != NULL) {

                keyOrder = KeyOrder.POS;
                
                fromKey = keyBuilder.statement2Key(p, o, NULL);

                toKey = keyBuilder.statement2Key(p, o + 1, NULL);

            } else if (s != NULL) {

                keyOrder = KeyOrder.SPO;
                
                fromKey = keyBuilder.statement2Key(s, NULL, NULL);

                toKey = keyBuilder.statement2Key(s + 1, NULL, NULL);

            } else if (p != NULL) {

                keyOrder = KeyOrder.POS;
                
                fromKey = keyBuilder.statement2Key(p, NULL, NULL);

                toKey = keyBuilder.statement2Key(p + 1, NULL, NULL);

            } else if (o != NULL) {

                keyOrder = KeyOrder.OSP;
                
                fromKey = keyBuilder.statement2Key(o, NULL, NULL);

                toKey = keyBuilder.statement2Key(o + 1, NULL, NULL);

            } else {

                keyOrder = KeyOrder.SPO;
                
                fromKey = toKey = null;

            }

        }

        /**
         * This materializes a set of {@link SPO}s at a time and then submits
         * tasks to parallel threads to remove those statements from each of the
         * statement indices. This continues until all statements selected by
         * the triple pattern have been removed.
         */
        public int removeAll() {

            /*
             * if all bound, then a slight optimization.
             */
            
            if (s != NULL && p != NULL && o != NULL) {

                byte[] key = keyBuilder.statement2Key(s, p, o);

                // @todo Do this in parallel threads with #indexWriteService.
                
                if (getSPOIndex().contains(key)) {

                    getSPOIndex().remove(key);
                    getPOSIndex().remove(keyBuilder.statement2Key(p, o, s));
                    getOSPIndex().remove(keyBuilder.statement2Key(o, s, p));
                    
                    return 1;
                    
                }
                
                return 0;
                
            }

            // asynchronous read-ahead iterator.
            ISPOIterator itr = iterator(0,0);
            
            int nremoved = 0;
            
            try {

                while(itr.hasNext()) {
                    
                    final SPO[] stmts = itr.nextChunk();
                    
                    // The #of statements that will be removed.
                    final int numStmts = stmts.length;
                    
                    final long begin = System.currentTimeMillis();

                    // The time to sort the data.
                    final AtomicLong sortTime = new AtomicLong(0);
                    
                    // The time to delete the statements from the indices.
                    final AtomicLong writeTime = new AtomicLong(0);
                    
                    /**
                     * Class writes on a statement index, removing the specified
                     * statements.
                     * 
                     * @author <a
                     *         href="mailto:thompsonbry@users.sourceforge.net">Bryan
                     *         Thompson</a>
                     * @version $Id$
                     */
                    class IndexWriter implements Callable<Long> {

                        final KeyOrder keyOrder;
                        final SPO[] a;

                        /*
                         * Private key builder for the SPO, POS, or OSP keys (one instance
                         * per thread).
                         */
                        final RdfKeyBuilder keyBuilder = new RdfKeyBuilder(
                                new KeyBuilder(3 * Bytes.SIZEOF_LONG));

                        IndexWriter(KeyOrder keyOrder, boolean clone) {
                            
                            this.keyOrder = keyOrder;

                            if(clone) {
                                
                                a = new SPO[numStmts];
                                
                                System.arraycopy(stmts, 0, a, 0, numStmts);
                                
                            } else {
                                
                                this.a = stmts;
                                
                            }
                            
                        }
                        
                        public Long call() throws Exception {

                            final long begin = System.currentTimeMillis();
                            
                            IIndex ndx = getIndex();

                            // Place statements in index order.
                            Arrays.sort(a, 0, numStmts, keyOrder.getSPOComparator());

                            final long beginWrite = System.currentTimeMillis();
                            
                            sortTime.addAndGet(beginWrite - begin);
                            
                            // remove statements from the index.
                            for (int i = 0; i < numStmts; i++) {

                                SPO spo = a[i];

                                ndx.remove(keyBuilder.statement2Key(keyOrder, spo));

                            }

                            final long endWrite = System.currentTimeMillis();
                            
                            writeTime.addAndGet(endWrite - beginWrite);
                            
                            return endWrite - begin;
                            
                        }
                        
                    }

                    List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(3);

                    /*
                     * @todo figure out which index is the most expensive and have it
                     * use the original data without cloning.
                     */

                    tasks.add(new IndexWriter(KeyOrder.SPO, false/* clone */));
                    tasks.add(new IndexWriter(KeyOrder.POS, true/* clone */));
                    tasks.add(new IndexWriter(KeyOrder.OSP, true/* clone */));

                    System.err.print("Removing " + numStmts + " statements...");

                    final List<Future<Long>> futures;
                    final long elapsed_SPO;
                    final long elapsed_POS;
                    final long elapsed_OSP;

                    try {

                        futures = indexWriteService.invokeAll(tasks);

                        elapsed_SPO = futures.get(0).get();
                        elapsed_POS = futures.get(1).get();
                        elapsed_OSP = futures.get(2).get();

                    } catch (InterruptedException ex) {

                        throw new RuntimeException(ex);

                    } catch (ExecutionException ex) {

                        throw new RuntimeException(ex);

                    }

                    long elapsed = System.currentTimeMillis() - begin;

                    System.err.println("in " + elapsed + "ms; sort=" + sortTime
                            + "ms, keyGen+delete=" + writeTime + "ms; spo="
                            + elapsed_SPO + "ms, pos=" + elapsed_POS + "ms, osp="
                            + elapsed_OSP + "ms");

                    // removed all statements in this chunk.
                    nremoved += numStmts;
                    
                }
                
            } finally {
                
                itr.close();
                
            }
            
            return nremoved;

        }

        /**
         * The implementation uses a key scan to find the first term identifer
         * for the given index. It then forms a fromKey that starts at the next
         * possible term identifier and does another scan, thereby obtaining the
         * 2nd distinct term identifier for that position on that index. This
         * process is repeated iteratively until the key scan no longer
         * identifies a match. This approach skips quickly over regions of the
         * index which have many statements for the same term and makes N+1
         * queries to identify N distinct terms. Note that there is no way to
         * pre-compute the #of distinct terms that will be identified short of
         * running the queries.
         * 
         * @todo This will need to be modified to return a chunked iterator that
         *       encapsulates the logic so that the distinct term scan may be
         *       applied when very large #s of terms would be visited.
         *       <p>
         *       If the indices are range partitioned and the iterator only
         *       guarentee "distinct" (and not also ordered) then those steps be
         *       parallelized. The only possibility for conflict is when the
         *       last distinct term identifier is read from one index before the
         *       right sibling index partition has reported its first distinct
         *       term identifier.
         *       <p>
         *       If the indices are range partitioned and distinct + ordered is
         *       required, then the operation can not be parallelized, or if it
         *       is parallelized then a merge sort must be done before returning
         *       the first result.
         *       <p>
         *       Likewise, if the indices are hash partitioned, then we can do
         *       parallel index scans and a merge sort but the caller will have
         *       to wait for the merge sort to complete before obtaining the 1st
         *       result.
         */
        public Iterator<Long> distinctTermScan() {

            ArrayList<Long> ids = new ArrayList<Long>(1000);
            
            byte[] fromKey = null;
            
            final byte[] toKey = null;
            
            IIndex ndx = getIndex();
            
            IEntryIterator itr = ndx.rangeIterator(fromKey, toKey);
            
            long[] tmp = new long[N];
            
            while(itr.hasNext()) {
                
                itr.next();
                
                // extract the term ids from the key. 
                RdfKeyBuilder.key2Statement(itr.getKey(), tmp); 

                final long id = tmp[0];
                
                // append tmp[0] to the output list.
                ids.add(id);

//                System.err.println(ids.size() + " : " + id + " : "
//                        + toString(id));
                
                // restart scan at the next possible term id.

                final long nextId = id + 1;
                
                fromKey = keyBuilder.statement2Key(nextId, NULL, NULL);
                
                // new iterator.
                itr = ndx.rangeIterator(fromKey, toKey);
                
            }
            
//            System.err.println("Distinct key scan: KeyOrder=" + keyOrder
//                    + ", #terms=" + ids.size());
            
            return ids.iterator();
            
        }
        
    }
    
    final public int removeStatements(Resource s, URI p, Value o) {

        /*
         * convert Value objects to internal identifiers.
         */
        long _s, _p, _o;

        _s = (s == null ? NULL : getTermId(s));
        _p = (p == null ? NULL : getTermId(p));
        _o = (o == null ? NULL : getTermId(o));

        /*
         * If a value was specified and it is not in the terms index then the
         * statement can not exist in the KB.
         */
        
        if (_s == NULL && s != null) {

            return 0;
            
        }
        
        if (_p == NULL && p != null) {
        
            return 0;
            
        }
        
        if (_o == NULL && o != null) {
            
            return 0;
            
        }
    
        return getAccessPath(_s,_p,_o).removeAll();
        
    }

    /*
     * statement externalization serialization stuff.
     */
    
    // namespace to prefix @todo integrate namespace stuff in the SAIL.
    private final Map<String, String> uriToPrefix = new HashMap<String, String>();
    
    /**
     * Defines a transient mapping from a URI to a namespace prefix that will be
     * used for that URI by {@link #toString()}.
     * 
     * @param namespace
     * 
     * @param prefix
     */
    final public void addNamespace(String namespace, String prefix) {
    
        uriToPrefix.put(namespace, prefix);

    }

    final public String toString( long s, long p, long o ) {
        
        return ("< " + toString(s) + ", " + toString(p) + ", " + toString(o) +" >");
        
    }

    final public String toString( long termId ) {

        if (termId == 0)
            return "0";

        _Value v = getTerm(termId);

        if (v == null)
            return "<NOT_FOUND#" + termId + ">";

        return (v instanceof URI ? abbrev((URI) v) : v.toString());
        
    }
//    private final String TERM_NOT_FOUND = "<NOT_FOUND>";
    
    /**
     * Substitutes in well know namespaces (rdf, rdfs, etc).
     */
    final private String abbrev( URI uri ) {
        
        String uriString = uri.getURI();
        
//        final int index = uriString.lastIndexOf('#');
//        
//        if(index==-1) return uriString;
//
//        final String namespace = uriString.substring(0, index);
        
        final String namespace = uri.getNamespace();
        
        final String prefix = uriToPrefix.get(namespace);
        
        if(prefix != null) {
            
            return prefix+":"+uri.getLocalName();
            
        } else return uriString;
        
    }

    final public void dumpStore() {
    
        dumpStore(true, true, true);
        
    }

    final public void dumpStore(boolean explicit, boolean inferred, boolean axioms) {

        final int nstmts = getStatementCount();
        
        IEntryIterator itr = getSPOIndex().rangeIterator(null, null);

        int i = 0;

        int nexplicit = 0;
        int ninferred = 0;
        int naxioms = 0;
        
        while (itr.hasNext()) {

            Object val = itr.next();
            
            SPO spo = new SPO(KeyOrder.SPO,itr.getKey(), val);

            switch (spo.type) {

            case Explicit:
                nexplicit++;
                if (!explicit)
                    continue;
                else
                    break;
            
            case Inferred:
                ninferred++;
                if (!inferred)
                    continue;
                else
                    break;
            
            case Axiom:
                naxioms++;
                if (!axioms)
                    continue;
                else
                    break;
            
            default:
                throw new AssertionError();
            
            }
            
            System.err.println("#" + (i+1) + "\t" + spo.toString(this));
            
            i++;
            
        }

        System.err.println("dumpStore: #statements=" + nstmts + ", #explicit="
                + nexplicit + ", #inferred=" + ninferred + ", #axioms="
                + naxioms);

    }
    
    /**
     * Iterator visits all terms in order by their assigned <strong>term
     * identifiers</strong> (efficient index scan, but the terms are not in
     * term order).
     * 
     * @see #termIdIndexScan()
     * 
     * @see #termIterator()
     */
    public Iterator<Value> idTermIndexScan() {

        final IIndex ndx = getIdTermIndex();

        // @todo this test does not work for the scale-out indices - see ClientIndexView.
        final boolean isolatableIndex = ndx instanceof IIsolatableIndex;

        return new Striterator(ndx.rangeIterator(null, null))
                .addFilter(new Resolver() {

            private static final long serialVersionUID = 1L;

            /**
             * @param val
             *            the serialized term.
             */
            protected Object resolve(Object val) {
                
                _Value term = (isolatableIndex ? _Value
                        .deserialize((byte[]) val) : (_Value) val);

                return term;
                
            }
            
        });

    }

    /**
     * Iterator visits all term identifiers in order by the <em>term</em> key
     * (efficient index scan).
     */
    public Iterator<Long> termIdIndexScan() {

        IIndex ndx = getTermIdIndex();

        // @todo this test does not work for the scale-out indices - see ClientIndexView.
        final boolean isolatableIndex = ndx instanceof IIsolatableIndex;

        return new Striterator(ndx.rangeIterator(null, null))
        .addFilter(new Resolver() {

            private static final long serialVersionUID = 1L;

            /**
             * Deserialize the term identifier (packed long integer).
             * 
             * @param val The serialized term identifier.
             */
            protected Object resolve(Object val) {

                final long id;
                
                try {

                    id = (isolatableIndex ? new DataInputBuffer((byte[]) val)
                            .unpackLong() : (Long) val);

                } catch (IOException ex) {

                    throw new RuntimeException(ex);

                }
                
                return id;
                
            }
            
        });
        
    }

    /**
     * Visits all terms in <strong>term key</strong> order (random index
     * operation).
     * <p>
     * Note: While this operation visits the terms in their index order it is
     * significantly less efficient than {@link #idTermIndexScan()}. This is
     * because the keys in the term:id index are formed using an un-reversable
     * technique such that it is not possible to re-materialize the term from
     * the key. Therefore visiting the terms in term order requires traversal of
     * the term:id index (so that you are in term order) plus term-by-term
     * resolution against the id:term index (to decode the term). Since the two
     * indices are not mutually ordered, that resolution will result in random
     * hits on the id:term index.
     */
    public Iterator<Value> termIterator() {

        // visit term identifiers in term order.
        Iterator<Long> itr = termIdIndexScan();

        // resolve term identifiers to terms.
        return new Striterator(itr).addFilter(new Resolver() {

            private static final long serialVersionUID = 1L;

            /**
             * @param val
             *            the term identifer (Long).
             */
            protected Object resolve(Object val) {

                // the term identifier.
                long termId = (Long) val;

                // resolve against the id:term index (random lookup).
                return getTerm(termId);

            }

        });

    }
    
    /**
     * Writes out some usage details on System.err.
     */
    final public void usage() {

        usage(name_termId, getTermIdIndex());
        usage(name_idTerm, getIdTermIndex());
        usage(name_spo, getSPOIndex());
        usage(name_pos, getPOSIndex());
        usage(name_osp, getOSPIndex());
        usage(name_just, getJustificationIndex());
         
    }

    /**
     * Writes out some usage information on the named index. More information is
     * available for local indices. Information for scale-out indices is both
     * less detailed and more approximate.
     * 
     * @param name
     *            The index name.
     * 
     * @param ndx
     *            The index.
     */
    final public void usage(String name,IIndex ndx) {
        
        if (ndx instanceof BTree) {

            BTree btree = (BTree) ndx;
            
            final int nentries = btree.getEntryCount();
            final int height = btree.getHeight();
            final int nleaves = btree.getLeafCount();
            final int nnodes = btree.getNodeCount();
            final int ndistinctOnQueue = btree.getNumDistinctOnQueue();
            final int queueCapacity = btree.getHardReferenceQueueCapacity();

            System.err.println(name + ": #entries=" + nentries + ", height="
                    + height + ", #nodes=" + nnodes + ", #leaves=" + nleaves
                    + ", #(nodes+leaves)=" + (nnodes + nleaves)
                    + ", #distinctOnQueue=" + ndistinctOnQueue
                    + ", queueCapacity=" + queueCapacity);
        } else {

            // Note: this is only an estimate if the index is a view.
            final int nentries = ndx.rangeCount(null, null);

            System.err.println(name+": #entries(est)="+nentries);
            
        }
        
    }

    /*
     * IRawTripleStore
     */

    /**
     * Copies the statements from <i>this</i> store into the specified store
     * using the <strong>same</strong> term identifiers (the lexicon is neither
     * copied to nor asserted on the target).
     * <p>
     * Note: This method MUST NOT be used unless it is known in advance that the
     * statements in <i>this</i> store use term identifiers that are consistent
     * with (term for term identical to) those in the destination store.
     * <p>
     * Note: The statements in <i>this</i> store are NOT removed.
     * 
     * @param src
     *            The temporary store (source).
     * 
     * @param dst
     *            The persistent database (destination).
     * 
     * @return The #of statements inserted into the main store (the count only
     *         reports those statements that were not already in the main
     *         store).
     * 
     * @todo write tests.
     */
    public int copyStatements(final AbstractTripleStore dst,
            final ISPOFilter filter) {

        if (dst == null)
            throw new IllegalArgumentException();
        if (dst==this)
            throw new IllegalArgumentException();
        
        // obtain a chunked iterator reading from any access path.
        ISPOIterator itr = getAccessPath(KeyOrder.SPO).iterator();
        
        // add statements to the target store.
        return dst.addStatements(itr, filter);

    }
    
    public int addStatements(SPO[] stmts, int numStmts ) {
       
        if( numStmts == 0 ) return 0;

        return addStatements( new SPOArrayIterator(stmts,numStmts), null /*filter*/);
    }
    
    public int addStatements(SPO[] stmts, int numStmts, ISPOFilter filter ) {
    
        if( numStmts == 0 ) return 0;

        return addStatements( new SPOArrayIterator(stmts,numStmts), filter);
        
    }
    
    public int addStatements(ISPOIterator itr, ISPOFilter filter) {

        if(!itr.hasNext()) return 0;
        
        final AtomicLong numWritten = new AtomicLong(0);

        /*
         * Note: We process the iterator a "chunk" at a time. If the iterator is
         * backed by an SPO[] then it will all be processed in one "chunk".
         */

        try {

            while (itr.hasNext()) {

                final SPO[] a = itr.nextChunk();

                final int numStmts = a.length;

                /*
                 * Note: The statements are inserted into each index in
                 * parallel. We clone the statement[] and sort and bulk load
                 * each index in parallel using a thread pool.
                 */

                long begin = System.currentTimeMillis();

                // time to sort the statements.
                final AtomicLong sortTime = new AtomicLong(0);

                // time to generate the keys and load the statements into the
                // indices.
                final AtomicLong insertTime = new AtomicLong(0);

                /**
                 * Writes on one of the statement indices.
                 * 
                 * @return The elapsed time for the operation.
                 * 
                 * @author <a
                 *         href="mailto:thompsonbry@users.sourceforge.net">Bryan
                 *         Thompson</a>
                 * @version $Id$
                 */
                class IndexWriter implements Callable<Long> {

                    private final SPO[] stmts;

                    private final Comparator<SPO> comparator;

                    private final IIndex ndx;

                    private final KeyOrder keyOrder;

                    private final ISPOFilter filter;

                    /*
                     * Private key builder for the SPO, POS, or OSP keys (one
                     * instance per thread).
                     */
                    private final RdfKeyBuilder keyBuilder = new RdfKeyBuilder(
                            new KeyBuilder(3 * Bytes.SIZEOF_LONG));

                    private final byte[][] keys;

                    // private final byte[][] vals;

                    /**
                     * Writes statements on a statement index (batch api).
                     * 
                     * @param clone
                     *            When true the statements are cloned.
                     *            <p>
                     *            Note:One of the {@link IndexWriter}s gets to
                     *            use the caller's array. The others MUST be
                     *            instructed to clone the caller's array so that
                     *            they can impose their distinct sort orders.
                     * @param keyOrder
                     *            Identifies the statement index on which to
                     *            write.
                     * @param filter
                     *            An optional filter.
                     */
                    IndexWriter(boolean clone, KeyOrder keyOrder,
                            ISPOFilter filter) {

                        if (clone) {

                            // copy the caller's data.
                            
                            this.stmts = new SPO[numStmts];

                            System.arraycopy(a, 0, this.stmts, 0, numStmts);

                        } else {

                            // use the callers reference.
                            
                            this.stmts = a;

                        }

                        this.comparator = keyOrder.getSPOComparator();

                        this.ndx = getStatementIndex(keyOrder);

                        this.keys = new byte[numStmts][];

                        // this.vals = new byte[numStmts][];

                        this.keyOrder = keyOrder;

                        this.filter = filter;

                    }

                    /**
                     * 
                     * Note: Changes here MUST also be made to the IndexWriter
                     * class for Sesame model Statements.
                     */
                    public Long call() throws Exception {

                        final long beginIndex = System.currentTimeMillis();

                        { // sort

                            long _begin = System.currentTimeMillis();

                            Arrays.sort(stmts, 0, numStmts, comparator);

                            sortTime.addAndGet(System.currentTimeMillis()
                                    - _begin);

                        }

                        { // load

                            long _begin = System.currentTimeMillis();

                            int numToAdd = 0;
                            
                            int writeCount = 0;

                            SPO last = null;

                            for (int i = 0; i < numStmts; i++) {

                                final SPO spo = stmts[i];

                                // skip statements that match the filter.
                                if (filter != null && filter.isMatch(spo))
                                    continue;

                                // skip duplicate records.
                                if (last != null && last.equals(spo))
                                    continue;

                                keys[numToAdd] = keyBuilder.statement2Key(
                                        keyOrder, spo);

                                last = spo;

                                numToAdd++;

                            }

                            if (false) {

                                /*
                                 * @todo allow client to send null for the
                                 * values when (a) they are inserting [null]
                                 * values under the keys; and (b) they do not
                                 * need the old values back.
                                 * 
                                 * FIXME This needs to be a custom batch
                                 * operation using a conditional insert so that
                                 * we do not write on the index when the data
                                 * would not be changed. Once this is working
                                 * again consider switching the embedded system
                                 * over to the same custom batch operation but
                                 * do some tests to figure out if it is
                                 * essentially the same cost for the embedded
                                 * database scenario.
                                 * 
                                 * @todo Also modify to write the StatementEnum
                                 * flags, which need to be upgraded to the max
                                 * of the old and new values when the statement
                                 * is pre-existing, e.g., a custom batch
                                 * operation.
                                 */

                                // BatchInsert op = new BatchInsert(numToAdd, keys, vals);
                                //
                                // ndx.insert(op);
                                
                            } else {

                                /*
                                 * This approach is fine for an embedded
                                 * database but it is no good for a distributed
                                 * system since it will make [n] RPCs.
                                 */

                                for (int i = 0; i < numToAdd; i++) {

                                    byte[] oldval = (byte[]) ndx
                                            .lookup(keys[i]);

                                    if (oldval == null) {

                                        // Statement is NOT pre-existing.

                                        ndx.insert(keys[i], stmts[i].type
                                                .serialize());

                                        writeCount++;
                                        
                                    } else {

                                        // old statement type.
                                        StatementEnum oldType = StatementEnum
                                                .deserialize(oldval);

                                        // proposed statement type.
                                        StatementEnum newType = stmts[i].type;

                                        // choose the max of the old and the
                                        // proposed.
                                        StatementEnum maxType = StatementEnum
                                                .max(oldType, newType);

                                        if (oldType != maxType) {

                                            /*
                                             * write on the index iff the type
                                             * was actually changed.
                                             */

                                            ndx.insert(keys[i], maxType
                                                    .serialize());
                                            
                                            writeCount++;

                                        }

                                    }

                                }

                            }

                            insertTime.addAndGet(System.currentTimeMillis()
                                    - _begin);

                            if (keyOrder == KeyOrder.SPO) {

                                /*
                                 * One task takes responsibility for reporting
                                 * the #of statements that were written on the
                                 * indices.
                                 */

                                numWritten.addAndGet(writeCount);

                            }

                        }

                        long elapsed = System.currentTimeMillis() - beginIndex;

                        return elapsed;

                    }

                }

                List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(3);

                /*
                 * @todo figure out which index is the most expensive and have
                 * it use the original data without cloning.
                 */

                tasks.add(new IndexWriter(false/* clone */, KeyOrder.SPO,
                        filter));

                tasks.add(new IndexWriter(true/* clone */, KeyOrder.POS,
                        filter));

                tasks.add(new IndexWriter(true/* clone */, KeyOrder.OSP,
                        filter));

                if(numStmts>1000) {

                    System.err.print("Writing " + numStmts + " statements...");
                    
                }

                final List<Future<Long>> futures;
                final long elapsed_SPO;
                final long elapsed_POS;
                final long elapsed_OSP;

                try {

                    futures = indexWriteService.invokeAll(tasks);

                    elapsed_SPO = futures.get(0).get();
                    elapsed_POS = futures.get(1).get();
                    elapsed_OSP = futures.get(2).get();

                } catch (InterruptedException ex) {

                    throw new RuntimeException(ex);

                } catch (ExecutionException ex) {

                    throw new RuntimeException(ex);

                }

                long elapsed = System.currentTimeMillis() - begin;

                if (numStmts > 1000) {
                
                    System.err.println("in " + elapsed + "ms; sort=" + sortTime
                        + "ms, keyGen+insert=" + insertTime + "ms; spo="
                        + elapsed_SPO + "ms, pos=" + elapsed_POS + "ms, osp="
                        + elapsed_OSP + "ms");
                    
                }

            }

        } finally {

            itr.close();

        }

        return (int) numWritten.get();

    }

    /**
     * Adds justifications to the store.
     * 
     * @param a
     *            The justifications.
     * @param n
     *            The #of elements in the array that are valid.
     * 
     * @todo Define a method to remove justifications that will be invoked by
     *       TM. Buffer and use bulk operations. Allow iterator as a source?
     * 
     * @todo refactor to {@link AbstractLocalTripleStore} and
     *       {@link ScaleOutTripleStore}
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
    public void addJustifications(Justification[] a, int n) {
        
        if (n == 0)
            return;

        final long begin = System.currentTimeMillis();
        
        IIndex ndx = getJustificationIndex();

        Arrays.sort(a, 0, n);
        
        /*
         * Note: This capacity estimate is based on 3 longs per SPO, one head,
         * and 2-3 SPOs in the tail. The capacity will be extended automatically
         * if necessary.
         */
        
        KeyBuilder keyBuilder = new KeyBuilder(3 * (1 + 3) * Bytes.SIZEOF_LONG);
        
        for (int i = 0; i < n; i++) {

            Justification jst = a[i];
            
            byte[] key = jst.getKey(keyBuilder);
            
            if (!ndx.contains(key)) {
                
                ndx.insert(key, null);
                
            }
            
        }
        
        final long elapsed = System.currentTimeMillis() - begin;
        
        log.info("Wrote "+n+" justifications in "+elapsed+" ms");
        
    }

    /**
     * Return true iff there is a grounded justification chain in the database
     * for the {@link SPO}.
     * <p>
     * Note: A grounded justification chain demonstrates the existence of a
     * proof for the {@link SPO}. During truth maintenance, if there is a
     * request to remove an explicit statement from the store AND a grounded
     * justification chain can be identified, then the statement will be
     * converted to an inference rather than being deleted from the store.
     * <p>
     * If justifications are being saved, then each entailed {@link SPO} will
     * have one or more justifications in the {@link #getJustificationIndex()}.
     * An SPO has a grounded justification chain IFF there exists a chain of
     * justifications that grounds out in explicit {@link SPO}s NOT including
     * itself.
     * 
     * @param spo
     *            An {@link StatementEnum#Explicit} SPO.
     * 
     * @return true if there is a grounded justification chain for that
     *         {@link SPO}.
     * 
     * @todo if an explicit statement being removed is an Axiom (test against
     *       the set of RDFS axioms) then it is always converted to an Axiom
     *       rather than searching for a grounded justification chain.
     * 
     * @todo if at all possible make this a set at a time operation.
     * 
     * @todo if we can prove that only grounded justifications are generated by
     *       the "fast" closure method then we do not need to chase the chain.
     *       In this case a statement is grounded if it there are any
     *       justification(s) in the index.
     */
    public boolean getGroundedJustification(SPO spo) {
        
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * The optional index on which {@link Justification}s are stored.
     */
    abstract public IIndex getJustificationIndex();
    
}
