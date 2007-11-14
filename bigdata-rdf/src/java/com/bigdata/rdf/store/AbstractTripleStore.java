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
 * Created on May 21, 2007
 */

package com.bigdata.rdf.store;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.sesame.sail.StatementIterator;
import org.openrdf.vocabulary.OWL;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;
import org.openrdf.vocabulary.XmlSchema;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.UnicodeKeyBuilder;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.isolation.IIsolatableIndex;
import com.bigdata.journal.ConcurrentJournal;
import com.bigdata.journal.Tx;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.InferenceEngine;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.inf.JustificationIterator;
import com.bigdata.rdf.inf.Rule;
import com.bigdata.rdf.inf.SPOAssertionBuffer;
import com.bigdata.rdf.model.OptimizedValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPOFilter;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOArrayIterator;
import com.bigdata.rdf.spo.SPOIterator;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * Abstract base class that implements logic for the {@link ITripleStore}
 * interface that is invariant across the choice of the backing store.
 * 
 * @todo Refactor to support transactions and concurrent load/query and test
 *       same.
 *       <p>
 *       Conflicts arise in the bigdata-RDF store when concurrent transactions
 *       attempt to define the same term. The problem arises because on index is
 *       used to map the term to an unique identifier and another to map the
 *       identifiers back to terms. Further, the statement indices use term
 *       identifiers directly in their keys. Therefore, resolving concurrent
 *       definition of the same term requires that we either do NOT isolate the
 *       writes on the term indices (which is probably an acceptable strategy)
 *       or that we let the application order the pass over the isolated indices
 *       and give the conflict resolver access to the {@link Tx} so that it can
 *       update the dependent indices if a conflict is discovered on the terms
 *       index.
 *       <p>
 *       The simplest approach appears to be NOT isolating the terms and ids
 *       indices. As long as the logic resides at the index, e.g., a lambda
 *       expression/method, to assign the identifier and create the entry in the
 *       ids index we can get buy with less isolation. If concurrent processes
 *       attempt to define the same term, then one or the other will wind up
 *       executing first (writes on indices are single threaded) and the result
 *       will be coherent as long as the write is committed before the ids are
 *       returned to the application. It simply does not matter which process
 *       defines the term since all that we care about is atomic, consistent,
 *       and durable. This is a case where group commit would work well (updates
 *       are blocked together on the server automatically to improve
 *       throughput).
 *       <p>
 *       Concurrent assertions of the same statement cause write-write
 *       conflicts, but they are trivially resolved -- we simply ignore the
 *       write-write conflict since both transactions agree on the statement
 *       data. Unlike the term indices, isolation is important for statements
 *       since we want to guarentee that a set of statements either is or is not
 *       asserted atomically. (With the terms index, we could care less as long
 *       as the indices are coherent.)
 *       <p>
 *       The only concern with the statement indices occurs when one transaction
 *       asserts a statement and a concurrent transaction deletes a statement. I
 *       need to go back and think this one through some more and figure out
 *       whether or not we need to abort a transaction in this case.
 * 
 * @todo bnodes do not need to be store in the terms or ids indices if we
 *       presume that an unknown identifier is a bnode. however, we still need
 *       to ensure that bnode identifiers are distinct or the same when and
 *       where appropriate, so we need to assign identifiers to bnodes in a
 *       restart-safe manner even if we "forget" the term-id mapping. (The
 *       possibility of an incomplete ids index during data load for the
 *       scale-out solution means that we must either read from a historical
 *       known consistent timestamp or record bnodes in the terms index.)
 * 
 * @todo the only added cost for a quad store is the additional statement
 *       indices. There are only three more statement indices in a quad store.
 *       Since statement indices are so cheap, it is probably worth implementing
 *       them now, even if only as a configuration option. (There may be reasons
 *       to maintain both versions.)
 * 
 * @todo verify read after commit (restart safe) for large data sets (multiple
 *       index partitions for scale-out and overflow for scale-out/scale-up).
 * 
 * @todo test re-load rate for a data set and verify that no new statements are
 *       added when re-loading a data set.
 * 
 * @todo possibly save frequently seen terms in each batch for the next batch in
 *       order to reduce unicode conversions.
 * 
 * @todo examine role for semi joins for a Sesame 2.x integration (quad store
 *       with real query operators). semi-joins (join indices) can be declared
 *       for various predicate combinations and then maintained. The
 *       declarations can be part of the scale-out index metadata. The logic
 *       that handles batch data load can also maintain the join indices. While
 *       triggers could be used for this purpose, there would need to be a means
 *       to aggregate and order the triggered events and then redistribute them
 *       against the partitions of the join indices. If the logic is in the
 *       client, then we need to make sure that newly declared join indices are
 *       fully populated (e.g., clients are notified to start building the join
 *       index and then we start the index build from existing data to remove
 *       any chance that the join index would be incomplete - the index would be
 *       ready as soon as the index build completes and client operations would
 *       be in a maintenance role).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractTripleStore implements ITripleStore, IRawTripleStore {

    /**
     * This is used to conditionally enable the logic to retract justifications
     * when the corresponding statements is retracted.
     */
    final private boolean justify;
    
    /**
     * Used to generate the compressed sort keys for the
     * {@link #getTermIdIndex()}.
     */
    final protected RdfKeyBuilder keyBuilder;
    
    /**
     * A copy of properties used to configure the {@link ITripleStore}.
     */
    final protected Properties properties;
    
    /**
     * The properties used to configure the configure the database wrapped up by
     * a new {@link Properties} object to prevent accidental modification by the
     * caller.
     */
    final public Properties getProperties() {
        
        /*
         * wrap them up so that people can not easily mess with the initial
         * properties.
         */

        return new Properties(properties);
        
    }
    
    /**
     * Configuration options.
     * 
     * @todo {@link AbstractTripleStore#readService} capacity.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends InferenceEngine.Options, com.bigdata.journal.Options {
        
        
    }
    
    /**
     * 
     * @param properties
     * 
     * @see Options.
     */    
    protected AbstractTripleStore(Properties properties) {
        
        // Copy the properties object.
        this.properties = (Properties)properties.clone();

        /*
         * Reads off the property for the inference engine that tells us whether
         * or not the justification index is being used. This is used to
         * conditionally enable the logic to retract justifications when the
         * corresponding statements is retracted.
         */

        this.justify = Boolean.parseBoolean(properties.getProperty(
                Options.JUSTIFY,
                Options.DEFAULT_JUSTIFY));

        // setup namespace mapping for serialization utility methods.
        addNamespace(RDF.NAMESPACE, "rdf");
        addNamespace(RDFS.NAMESPACE, "rdfs");
        addNamespace(OWL.NAMESPACE, "owl");
        addNamespace(XmlSchema.NAMESPACE, "xsd");

        keyBuilder = new RdfKeyBuilder(new UnicodeKeyBuilder(createCollator(),
                Bytes.kilobyte32));
        
    }
    
    /**
     * Close the client. If the client uses an embedded database, then close and
     * delete the embedded database as well. If the client is connected to a
     * remote database then only the connection is closed.
     * <p>
     * Note: This is mainly used by the test suites.
     * <p>
     * Note: The default implementation merely terminates some thread pools.
     */
    public void closeAndDelete() {

        shutdown();

    }

    /**
     * Note: The default implementation merely terminates some thread pools.
     */
    public void close() {

        shutdown();
        
    }

    /**
     * Terminates the {@link #readService} and the {@link #writeService}.
     */
    protected void shutdown() {
        
        writeService.shutdown();
        
        try {

            writeService.awaitTermination(2, TimeUnit.SECONDS);
            
        } catch(InterruptedException ex) {
            
            log.warn("Write service did not terminate within timeout.");
            
        }
        
        readService.shutdown();
        
        try {

            readService.awaitTermination(2, TimeUnit.SECONDS);
            
        } catch(InterruptedException ex) {
            
            log.warn("Read service did not terminate within timeout.");
            
        }
        
    }
    
    /**
     * True iff the backing store is stable (exists on disk somewhere and may be
     * closed and re-opened).
     * <p>
     * Note: This is mainly used by the test suites.
     */
    abstract public boolean isStable();

    /**
     * Return the shared {@link RdfKeyBuilder} instance for this client. The
     * object will be compatible with the Unicode preferences that are in effect
     * for the {@link ITripleStore}.
     * <p>
     * Note: This object is NOT thread-safe.
     * 
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
    
    /**
     * Executor service for read parallelism.
     * 
     * @todo use for parallel execution of map of new vs old+new over the terms
     *       of a rule.
     * 
     * @todo use for parallel execution of sub-queries.
     * 
     * @todo {@link SPOAssertionBuffer} must be thread-safe. {@link Rule}
     *       bindings must be per-thread.
     * 
     * @todo Note that rules that write entailments on the database statement
     *       MUST coordinate to avoid concurrent modification during traversal
     *       of the statement indices. The chunked iterators go a long way to
     *       addressing this.
     */
    final public ExecutorService readService = Executors
            .newCachedThreadPool(DaemonThreadFactory.defaultThreadFactory());

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
    public ExecutorService writeService = Executors.unconfigurableExecutorService(Executors.newFixedThreadPool(N,
            DaemonThreadFactory.defaultThreadFactory()));

    /**
     * Generate the sort keys for the terms.
     * 
     * @param keyBuilder
     *            The object used to generate the sort keys - <em>this is not
     *            safe for concurrent writers</em>
     * @param terms
     *            The terms whose sort keys will be generated.
     * @param numTerms
     *            The #of terms in that array.
     * 
     * @see #createCollator()
     * @see UnicodeKeyBuilder
     */
    final public void generateSortKeys(RdfKeyBuilder keyBuilder,
            _Value[] terms, int numTerms) {
        
        for (int i = 0; i < numTerms; i++) {

            _Value term = terms[i];

            if (term.key == null) {

                term.key = keyBuilder.value2Key(term);

            }

        }

    }

    /**
     * Batch insert of terms into the database.
     * 
     * @param terms
     *            An array whose elements [0:nterms-1] will be inserted.
     * @param numTerms
     *            The #of terms to insert.
     * @param haveKeys
     *            True if the terms already have their sort keys.
     * @param sorted
     *            True if the terms are already sorted by their sort keys (in
     *            the correct order for a batch insert).
     * 
     * @exception IllegalArgumentException
     *                if <code>!haveKeys && sorted</code>.
     */
    abstract public void insertTerms(_Value[] terms, int numTerms,
            boolean haveKeys, boolean sorted);

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

    /*
     * singletons.
     */
    
    private WeakReference<InferenceEngine> inferenceEngineRef = null;
    
    final public InferenceEngine getInferenceEngine() {
    
        synchronized(this) {
        
            InferenceEngine inf = inferenceEngineRef == null ? null
                    : inferenceEngineRef.get();
            
            if (inf == null) {
                
                inf = new InferenceEngine(this);
            
                inferenceEngineRef = new WeakReference<InferenceEngine>(inf);
                
            }
            
            return inf;
            
        }
        
    }
    
    private WeakReference<DataLoader> dataLoaderRef = null;

    final public DataLoader getDataLoader() {
        
        synchronized(this) {
        
            DataLoader dataLoader = dataLoaderRef == null ? null
                    : dataLoaderRef.get();
            
            if (dataLoader == null) {
                
                dataLoader = new DataLoader(this);
            
                dataLoaderRef = new WeakReference<DataLoader>(dataLoader);
                
            }
            
            return dataLoader;
            
        }
        
    }
    
    /*
     * Sesame integration.
     */

    final public void addStatement(Resource s, URI p, Value o) {

        /*
         * Note: This uses the batch API.
         */
        
        IStatementBuffer buffer = new StatementBuffer(this, 1);
        
        buffer.add(s, p, o);
        
        buffer.flush();
        
    }

    final public SPO getStatement(long s, long p, long o) {
        
        if (s == NULL || p == NULL || o == NULL) {

            throw new IllegalArgumentException();
            
        }

        // @todo thread-safety for the key builder.
        byte[] key = getKeyBuilder().statement2Key(s, p, o);

        byte[] val = (byte[]) getStatementIndex(KeyOrder.SPO).lookup(key);

        if (val == null) {

            return null;

        }

        // The statement is known to the database.

        StatementEnum type = StatementEnum.deserialize(val);

        return new SPO(s, p, o, type);

    }
    
    /**
     * Return true if the triple pattern matches any statement(s) in the store
     * (non-batch API).
     * <p>
     * Note: This method does not verify whether or not the statement is
     * explicit.
     * 
     * @param s
     * @param p
     * @param o
     */
    final public boolean hasStatement(long s, long p, long o) {

        return ! getAccessPath(s,p,o).isEmpty();
        
    }

    final public boolean hasStatement(Resource s, URI p, Value o) {

        IAccessPath accessPath = getAccessPath(s,p,o);
        
        if(accessPath instanceof EmptyAccessPath) {
            
            return false;
            
        }

        return ! accessPath.isEmpty();
        
    }
    
    final public int removeStatements(Resource s, URI p, Value o) {
        
        return getAccessPath(s,p,o).removeAll();
        
    }

    /**
     * Return the statement from the database matching the fully bound query.
     * 
     * @param s
     * @param p
     * @param o
     * @return
     */
    public StatementWithType getStatement(Resource s, URI p, Value o) {

        if(s == null || p == null || o == null) {
            
            throw new IllegalArgumentException();
            
        }
        
        StatementIterator itr = getStatements(s, p, o);

        try {

            if (!itr.hasNext()) {

                return null;

            }

            return (StatementWithType) itr.next();

        } finally {

            itr.close();

        }

    }
    
    public StatementIterator getStatements(Resource s, URI p, Value o) {

        return asStatementIterator( getAccessPath(s, p, o).iterator() );
        
    }

    /**
     * Converts an internal {@link _Value} to a Sesame {@link Value} object.
     * 
     * @param value
     *            Either an internal {@link _Value}, a Sesame {@link Value}
     *            object, or <code>null</code>.
     * 
     * @return A corresponding Sesame {@link Value} object -or-
     *         <code>null</code> iff <i>value</i> is <code>null</code>.
     */
    final public Value asValue(Value value) {
        
        return OptimizedValueFactory.INSTANCE.toSesameObject(value);
        
    }
    
    public Statement asStatement(SPO spo) {

        return new StatementWithType( //
                (Resource) OptimizedValueFactory.INSTANCE
                        .toSesameObject(getTerm(spo.s)),//
                (URI) OptimizedValueFactory.INSTANCE
                        .toSesameObject(getTerm(spo.p)), //
                (Value) OptimizedValueFactory.INSTANCE
                        .toSesameObject(getTerm(spo.o)), //
                spo.type//
        );
        
    }
    
    public StatementIterator asStatementIterator(ISPOIterator src) {
        
        return new SesameStatementIterator(this,src);
        
    }
    
    public IAccessPath getAccessPath(Resource s, URI p, Value o) {
        
        /*
         * convert other Value object types to our object types.
         */

        s = (Resource) OptimizedValueFactory.INSTANCE.toNativeValue(s);
        
        p = (URI) OptimizedValueFactory.INSTANCE.toNativeValue(p);
        
        o = OptimizedValueFactory.INSTANCE.toNativeValue(o);
        
        /*
         * Convert our object types to internal identifiers.
         * 
         * Note: If a value was specified and it is not in the terms index then the
         * statement can not exist in the KB.
         */
        final long _s = getTermId(s);

        if (_s == NULL && s != null) return new EmptyAccessPath();

        final long _p = getTermId(p);

        if (_p == NULL && p != null) return new EmptyAccessPath();

        final long _o = getTermId(o);

        if (_o == NULL && o != null) return new EmptyAccessPath();
        
        /*
         * Return the access path.
         */
        
        return getAccessPath(_s, _p, _o);

    }
    
    final public IAccessPath getAccessPath(long s, long p, long o) {
        
        return new AccessPath(KeyOrder.get(s,p,o),s,p,o);
        
    }
    
    final public IAccessPath getAccessPath(KeyOrder keyOrder) {
        
        return new AccessPath(keyOrder,NULL,NULL,NULL);
        
    }
    
    /**
     * An access path that is known to be empty. There is a single instance of
     * this class. Various methods will return that object if you request an
     * access path using the Sesame {@link Value} objects and one of the
     * {@link Value}s is not known to the database. In such cases we know that
     * nothing can be read from the database for the given triple pattern.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class EmptyAccessPath implements IAccessPath {
        
        /**
         * @throws UnsupportedOperationException
         */
        public long[] getTriplePattern() {
            
            throw new UnsupportedOperationException();
            
        }

//        public IIndex getStatementIndex() {
//            
//            return AbstractTripleStore.this.getStatementIndex(getKeyOrder());
//            
//        }

        public KeyOrder getKeyOrder() {
            
            // arbitrary.
            return KeyOrder.SPO;
            
        }

        /**
         * Always returns <code>true</code>.
         */
        public boolean isEmpty() {
            
            return true;
            
        }

        /**
         * Always returns ZERO(0).
         */
        public int rangeCount() {
            
            return 0;
            
        }

        /**
         * @throws UnsupportedOperationException
         */
        public IEntryIterator rangeQuery() {

            throw new UnsupportedOperationException();
            
        }

        public ISPOIterator iterator() {

            return new SPOArrayIterator(new SPO[]{},0);
            
        }

        public ISPOIterator iterator(ISPOFilter filter) {

            return new SPOArrayIterator(new SPO[]{},0);
            
        }

        public ISPOIterator iterator(int limit, int capacity) {

            return new SPOArrayIterator(new SPO[]{},0);
            
        }

        public ISPOIterator iterator(int limit, int capacity, ISPOFilter filter) {

            return new SPOArrayIterator(new SPO[]{},0);
            
        }

        public Iterator<Long> distinctTermScan() {
            
            return Arrays.asList(new Long[]{}).iterator();
            
        }

        public int removeAll() {

            return 0;
            
        }
        
        public int removeAll(ISPOFilter filter) {

            return 0;
            
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

        /** true iff the triple pattern is fully bound. */
        final boolean allBound;
             
        public long[] getTriplePattern() {
            
            return new long[]{s,p,o};
            
        }
        
        private IIndex getStatementIndex() {
            
            return AbstractTripleStore.this.getStatementIndex( keyOrder );
            
        }

        public KeyOrder getKeyOrder() {

            return keyOrder;
            
        }

        public boolean isEmpty() {

            /*
             * Note: empty iff the iterator can not visit anything.
             */
            
            return !iterator(1/* limit */, 1/* capacity */).hasNext();
            
        }
        
        public int rangeCount() {
            
            return getStatementIndex().rangeCount(fromKey,toKey);
            
        }

        public IEntryIterator rangeQuery() {
            
            return getStatementIndex().rangeIterator(fromKey, toKey);
            
        }

        public ISPOIterator iterator() {
            
            return iterator(null/*filter*/);
            
        }

        /**
         * FIXME This currently sucks everything into an array. Get traversal
         * with concurrent modification working for {@link IEntryIterator} and
         * then modify this to use {@link SPOIterator}, which uses asynchronous
         * reads and buffers a reasonable number of statements in memory.
         * <p>
         * Note: the {@link Rule}s basically assume that they can traverse the
         * statement indices with concurrent modification of those indices (the
         * {@link SPOAssertionBuffer} incrementally flushes the results to the database,
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
         * <p>
         * Updated position: There are two uses to support. One is remove() on
         * IEntryIterator with a single-thread accessing the BTree. This case is
         * easy enough since we only need to support "delete behind." It can be
         * handled with restarting the iterator if necessary to accomodate a
         * delete which causes a structural modification (leaves to merge).
         * <p>
         * The second use case is logically concurrent tasks that read and write
         * on the same unisolated index(s). This use case is already supported
         * by the concurrency control mechanisms in the concurrent journal.
         * However, the refactor should wait on a change to the concurrency
         * control to: (a) check point the indices at the each of each write so
         * that we can abort individual tasks without throwing away the work of
         * other tasks; and (b) add an "awaitCommit" property to the task so
         * that tasks may execute without either forcing a commit or awaiting a
         * commit. When false, "awaitCommit" does NOT mean that the application
         * can rely on the data to NOT be durable, it simply means that the
         * application does not require an immediate commit (ie, is willing to
         * discover that the data were not durable on restart). This would be
         * useful in an embedded application such as the RDF database which
         * performs a lot of write tasks on the unisolated indices before it has
         * completed an atomic operation from the application perspective. Note
         * that "awaitCommit" probably is only viable in an environment without
         * application level concurrency since partial writes would be visible
         * immediately and COULD be restart safe. However, an application such
         * as the {@link ScaleOutTripleStore} already manages concurrency at the
         * application level using a "consistent" data approach (but this issue
         * has not been solved for computing closure or truth maintenance on
         * statement removal.)
         */
        public ISPOIterator iterator(ISPOFilter filter) {

            if (allBound) {

                // Optimization for point test.

                return new SPOArrayIterator(AbstractTripleStore.this, this,
                        1/* limit */, filter);

            }

            /*
             * This is an async incremental iterator that buffers some but not
             * necessarily all statements.
             */
            // return iterator(0/*limit*/, 0/*capacity*/, filter);
            /*
             * This is a synchronous read that buffers all statements.
             */

            return new SPOArrayIterator(AbstractTripleStore.this, this,
                    0/* no limit */, filter);

        }

        /**
         * Note: Return an iterator that will use transparent read-ahead when no
         * limit is specified (limit is zero) or the limit is "small".
         * 
         * @see SPOIterator
         */
        public ISPOIterator iterator(int limit, int capacity) {

            return iterator(limit, capacity, null/*filter*/);
            
        }

        /**
         * Note: Return an iterator that will use transparent read-ahead when no
         * limit is specified (limit is zero) or the limit is "small".
         * 
         * @see SPOIterator
         */
        public ISPOIterator iterator(int limit, int capacity, ISPOFilter filter) {

            if (allBound) {

                // Optimization for point test.

                return new SPOArrayIterator(AbstractTripleStore.this, this,
                        1/* limit */, filter);

            }

            if (limit > 0 && limit < 100) {

                /*
                 * Use a light-weight synchronous fully buffered variant when
                 * the limit is small, especially when all that you are doing is
                 * an existence test (limit := 1).
                 */

                return new SPOArrayIterator(AbstractTripleStore.this, this,
                        limit, filter);

            }

            boolean async = true;

            return new SPOIterator(this, limit, capacity, async, filter);

        }
        
        /**
         * Chooses the best access path for the given triple pattern.
         * 
         * @param s
         *            The term identifier for the subject -or-
         *            {@link IRawTripleStore#NULL}.
         * @param p
         *            The term identifier for the predicate -or-
         *            {@link IRawTripleStore#NULL}.
         * @param o
         *            The term identifier for the object -or-
         *            {@link IRawTripleStore#NULL}.
         */
        AccessPath(final KeyOrder keyOrder, long s, long p, long o) {

            if (keyOrder == null)
                throw new IllegalArgumentException();
            
            this.keyOrder = keyOrder;
            
            this.s = s;
            
            this.p = p;
            
            this.o = o;
            
            this.allBound = (s != NULL && p != NULL & o != NULL);
            
            if (s != NULL && p != NULL && o != NULL) {
        
                assert keyOrder == KeyOrder.SPO;
                
                fromKey = keyBuilder.statement2Key(s, p, o);

                toKey = keyBuilder.statement2Key(s, p, o + 1);

            } else if (s != NULL && p != NULL) {

                assert keyOrder == KeyOrder.SPO;
                
                fromKey = keyBuilder.statement2Key(s, p, NULL);

                toKey = keyBuilder.statement2Key(s, p + 1, NULL);

            } else if (s != NULL && o != NULL) {

                assert keyOrder == KeyOrder.OSP;
                
                fromKey = keyBuilder.statement2Key(o, s, NULL);

                toKey = keyBuilder.statement2Key(o, s + 1, NULL);

            } else if (p != NULL && o != NULL) {

                assert keyOrder == KeyOrder.POS;
                
                fromKey = keyBuilder.statement2Key(p, o, NULL);

                toKey = keyBuilder.statement2Key(p, o + 1, NULL);

            } else if (s != NULL) {

                assert keyOrder == KeyOrder.SPO;
                
                fromKey = keyBuilder.statement2Key(s, NULL, NULL);

                toKey = keyBuilder.statement2Key(s + 1, NULL, NULL);

            } else if (p != NULL) {

                assert keyOrder == KeyOrder.POS;
                
                fromKey = keyBuilder.statement2Key(p, NULL, NULL);

                toKey = keyBuilder.statement2Key(p + 1, NULL, NULL);

            } else if (o != NULL) {

                assert keyOrder == KeyOrder.OSP;
                
                fromKey = keyBuilder.statement2Key(o, NULL, NULL);

                toKey = keyBuilder.statement2Key(o + 1, NULL, NULL);

            } else {

                /*
                 * Note: The KeyOrder does not matter when you are fully
                 * unbound.
                 */
                
                fromKey = toKey = null;

            }

        }

        /**
         * Representation of the state for the access path (key order, triple
         * pattern, and from/to keys).
         */
        public String toString() {
            
            return super.toString() + ": " + keyOrder + ", {" + s + "," + p
                    + "," + o + "}, fromKey=" + (fromKey==null?"n/a":Arrays.toString(fromKey))
                    + ", toKey=" + (toKey==null?"n/a":Arrays.toString(toKey));
            
        }
        
        /**
         * This materializes a set of {@link SPO}s at a time and then submits
         * tasks to parallel threads to remove those statements from each of the
         * statement indices. This continues until all statements selected by
         * the triple pattern have been removed.
         */
        public int removeAll() {
            
            return removeAll(null/*filter*/);
            
        }
        
        /**
         * This materializes a set of {@link SPO}s at a time and then submits
         * tasks to parallel threads to remove those statements from each of the
         * statement indices. This continues until all statements selected by
         * the triple pattern have been removed.
         */
        public int removeAll(ISPOFilter filter) {

            // @todo try with an asynchronous read-ahead iterator.
//            ISPOIterator itr = iterator(0,0);
            
            // synchronous fully buffered iterator.
            ISPOIterator itr = iterator(filter);
            
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
                                new KeyBuilder(N * Bytes.SIZEOF_LONG));

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
                            
                            IIndex ndx = AbstractTripleStore.this.getStatementIndex(keyOrder);

                            // Place statements in index order.
                            Arrays.sort(a, 0, numStmts, keyOrder.getComparator());

                            final long beginWrite = System.currentTimeMillis();
                            
                            sortTime.addAndGet(beginWrite - begin);
                            
                            // remove statements from the index.
                            for (int i = 0; i < numStmts; i++) {

                                SPO spo = a[i];

                                if(DEBUG) {
                                    
                                    /*
                                     * Note: the externalized terms will be NOT
                                     * FOUND when removing a statement from a
                                     * temp store since the term identifiers for
                                     * the temp store are generally only stored
                                     * in the database.
                                     */
                                    log.debug("Removing "
                                                    + spo.toString(AbstractTripleStore.this)
                                                    + " from " + keyOrder);
                                    
                                }
                                
                                byte[] key = keyBuilder.statement2Key(keyOrder, spo);

                                if(ndx.remove( key )==null) {
                                    
                                    throw new AssertionError(
                                            "Missing statement: keyOrder="
                                                    + keyOrder + ", spo=" + spo
                                                    + ", key=" + Arrays.toString(key));
                                    
                                }

                            }

                            final long endWrite = System.currentTimeMillis();
                            
                            writeTime.addAndGet(endWrite - beginWrite);
                            
                            return endWrite - begin;
                            
                        }
                        
                    }

                    /**
                     * Class writes on the justification index, removing all
                     * justifications for each statement that is being removed.
                     * <p>
                     * Note: There is only one index for justifications. The
                     * keys all use the SPO of the entailed statement as their
                     * prefix, so given a statement it is trivial to do a range
                     * scan for its justifications.
                     * 
                     * @todo if we supported remove() on the IEntryIterator then
                     *       we would not have to buffer the justifications that
                     *       we want to delete.
                     * 
                     * @author <a
                     *         href="mailto:thompsonbry@users.sourceforge.net">Bryan
                     *         Thompson</a>
                     * @version $Id$
                     */
                    class JustificationWriter implements Callable<Long> {

                        final SPO[] a;

                        /*
                         * Private key builder.
                         * 
                         * Note: This capacity estimate is based on N longs per SPO, one head,
                         * and 2-3 SPOs in the tail. The capacity will be extended automatically
                         * if necessary.
                         */
                        KeyBuilder keyBuilder = new KeyBuilder(N * (1 + 3) * Bytes.SIZEOF_LONG);

                        JustificationWriter(boolean clone) {
                            
                            if(clone) {
                                
                                a = new SPO[numStmts];
                                
                                System.arraycopy(stmts, 0, a, 0, numStmts);
                                
                            } else {
                                
                                this.a = stmts;
                                
                            }
                            
                        }

                        public Long call() throws Exception {
                            
                            final long begin = System.currentTimeMillis();
                            
                            IIndex ndx = AbstractTripleStore.this.getJustificationIndex();

                            /*
                             * Place statements in index order (SPO since all
                             * justifications begin with the SPO of the entailed
                             * statement.
                             */
                            Arrays.sort(a, 0, numStmts, KeyOrder.SPO.getComparator());

                            final long beginWrite = System.currentTimeMillis();
                            
                            sortTime.addAndGet(beginWrite - begin);

                            // remove statements from the index.
                            for (int i = 0; i < numStmts; i++) {

                                SPO spo = a[i];

                                // will visit justifications for that statement.
                                JustificationIterator itr = new JustificationIterator(
                                        AbstractTripleStore.this, spo);
                                
                                if(DEBUG) {
                                    
                                    log.debug("Removing "
                                                    + ndx.rangeCount(fromKey,toKey)
                                                    + " justifications for "
                                                    + spo.toString(AbstractTripleStore.this));
                                    
                                }

                                while(itr.hasNext()) {
                                    
                                    itr.next();
                                    
                                    itr.remove();
                                    
                                }

                            }

                            final long endWrite = System.currentTimeMillis();
                            
                            writeTime.addAndGet(endWrite - beginWrite);
                            
                            return endWrite - begin;

                        }
                        
                    }
                    
                    List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(3);

                    tasks.add(new IndexWriter(KeyOrder.SPO, false/* clone */));
                    tasks.add(new IndexWriter(KeyOrder.POS, true/* clone */));
                    tasks.add(new IndexWriter(KeyOrder.OSP, true/* clone */));
                    
                    if(justify) {

                        /*
                         * Also retract the justifications for the statements.
                         */
                        
                        tasks.add(new JustificationWriter(true/* clone */));
                        
                    }

                    final List<Future<Long>> futures;
                    final long elapsed_SPO;
                    final long elapsed_POS;
                    final long elapsed_OSP;
                    final long elapsed_JST;

                    try {

                        futures = writeService.invokeAll(tasks);

                        elapsed_SPO = futures.get(0).get();
                        elapsed_POS = futures.get(1).get();
                        elapsed_OSP = futures.get(2).get();
                        if(justify) {
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

                    if(numStmts>1000) {

                        log.info("Removed "+numStmts+" in " + elapsed + "ms; sort=" + sortTime
                            + "ms, keyGen+delete=" + writeTime + "ms; spo="
                            + elapsed_SPO + "ms, pos=" + elapsed_POS + "ms, osp="
                            + elapsed_OSP + "ms, jst="+elapsed_JST);
                        
                    }

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
            
            IIndex ndx = getStatementIndex();
            
            IEntryIterator itr = ndx.rangeIterator(fromKey, toKey);
            
            long[] tmp = new long[IRawTripleStore.N];
            
            while(itr.hasNext()) {
                
                itr.next();
                
                // extract the term ids from the key. 
                RdfKeyBuilder.key2Statement(itr.getKey(), tmp); 

                final long id = tmp[0];
                
                // append tmp[0] to the output list.
                ids.add(id);

//                log.debug(ids.size() + " : " + id + " : "+ toString(id));
                
                // restart scan at the next possible term id.

                final long nextId = id + 1;
                
                fromKey = keyBuilder.statement2Key(nextId, NULL, NULL);
                
                // new iterator.
                itr = ndx.rangeIterator(fromKey, toKey);
                
            }
            
//            log.debug("Distinct key scan: KeyOrder=" + keyOrder + ", #terms=" + ids.size());
            
            return ids.iterator();
            
        }
        
    }
    
    /*
     * statement externalization serialization stuff.
     */
    
    // namespace to prefix
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
    
    /**
     * Return an unmodifiable view of the mapping from namespaces to namespace
     * prefixes.
     * <p>
     * Note: this is NOT a persistent map. It is used by {@link #toString(long)}
     * when externalizing URIs.
     */
    final public Map<String,String> getNamespaces() {
        
        return Collections.unmodifiableMap(uriToPrefix);
        
    }

    final public String toString( long s, long p, long o ) {
        
        return ("< " + toString(s) + ", " + toString(p) + ", " + toString(o) +" >");
        
    }

    /**
     * Return true iff the term identifier is associated with a RDF Literal in
     * the database.
     * <p>
     * Note: This simply examines the low bit of the term identifier, which
     * marks whether or not the term identifier is a literal.
     * <p>
     * Note: Some entailments require the ability to filter based on whether or
     * not a term is a literal. For example, literals may not be entailed into
     * the subject position. This method makes it possible to determine whether
     * or not a term is a literal without materializing the term, thereby
     * allowing the entailments to be computed purely within the term identifier
     * space.
     * 
     * @param termId
     *            The term identifier.
     * 
     * @return <code>true</code> iff the term identifier is an RDF literal.
     */
    final public boolean isLiteral( long termId ) {
        
        return (termId & 0x01L) == 1L;
        
    }
    
    /**
     * Specialize the return to {@link _Value}. This keeps the
     * {@link ITripleStore} interface cleaner while imposes the actual semantics
     * on all implementation of this class.
     * 
     * @todo Add getTerm(s,p,o) or getTerm(SPO) returning _Value[] to batch each
     *       statement lookup so that we do less RPCs in a distributed system.
     *       <p>
     *       Note: Implementations of {@link #getTerm(long)} are already supposed
     *       to be caching recently resolved terms.
     */
    abstract public _Value getTerm(long id);

    final public String toString( long termId ) {

        if (termId == 0)
            return "0";

        _Value v = (_Value)getTerm(termId);

        if (v == null)
            return "<NOT_FOUND#" + termId + ">";

        String s = (v instanceof URI ? abbrev((URI) v) : v.toString());
        
        return s + ("("+termId+")");
        
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

    final public void predicateUsage() {
        
        predicateUsage(this);
        
    }

    /**
     * Dumps the #of statements using each predicate in the kb on
     * {@link System#err} (tab delimited, unordered).
     * 
     * @param resolveTerms
     *            Used to resolve term identifiers to terms (you can use this to
     *            dump a {@link TempTripleStore} that is using the term
     *            dictionary of the main database).
     */
    final public void predicateUsage(AbstractTripleStore resolveTerms) {

        // visit distinct term identifiers for the predicate position.
        Iterator<Long> itr = getAccessPath(KeyOrder.POS).distinctTermScan();
        
        while(itr.hasNext()) {
            
            long p = itr.next();
            
            int n = getAccessPath(NULL, p, NULL).rangeCount();
            
            System.err.println(n+"\t"+resolveTerms.toString(p));
            
        }
        
    }
    
    /**
     * Utility method dumps the statements in the store onto {@link System#err}
     * using the SPO index (subject order).
     */
    final public void dumpStore() {
    
        dumpStore(true, true, true);
        
    }

    final public void dumpStore(boolean explicit, boolean inferred, boolean axioms) {

        dumpStore(this,explicit,inferred,axioms);
        
    }

    /**
     * Dumps the store in a human readable format (not suitable for
     * interchange).
     * 
     * @param resolveTerms
     *            Used to resolve term identifiers to terms (you can use this to
     *            dump a {@link TempTripleStore} that is using the term
     *            dictionary of the main database).
     * @param explicit
     *            Show statements marked as explicit.
     * @param inferred
     *            Show statements marked inferred.
     * @param axioms
     *            Show statements marked as axioms.
     */
    final public void dumpStore(AbstractTripleStore resolveTerms,
            boolean explicit, boolean inferred, boolean axioms) {

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
            
            System.err.println("#" + (i+1) + "\t" + spo.toString(resolveTerms));
            
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

        final boolean isolatableIndex = ndx.isIsolatable();

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

        final boolean isolatableIndex = ndx.isIsolatable();

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
     * Writes out some usage information for the database on
     * {@link ITripleStore#log}.
     */
    public void usage() {

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

            log.info(name + ": #entries=" + nentries + ", height="
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
     */
    public int copyStatements(AbstractTripleStore dst, ISPOFilter filter) {

        if (dst == null)
            throw new IllegalArgumentException();

        if (dst == this)
            throw new IllegalArgumentException();
        
        // obtain a chunked iterator reading from any access path.
        ISPOIterator itr = getAccessPath(KeyOrder.SPO).iterator(filter);
        
        // add statements to the target store.
        return dst.addStatements(itr, null/*filter*/);

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
                            new KeyBuilder(N * Bytes.SIZEOF_LONG));

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

                        this.comparator = keyOrder.getComparator();

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

                                    SPO spo = stmts[i];
                                    
                                    byte[] oldval = (byte[]) ndx
                                            .lookup(keys[i]);

                                    if (oldval == null) {

                                        /*
                                         * Statement is NOT pre-existing.
                                         */
                                        
                                        ndx.insert(keys[i], spo.type
                                                .serialize());

                                        writeCount++;

                                    } else {

                                        /* 
                                         * Statement is pre-existing.
                                         */
                                        
                                        // old statement type.
                                        StatementEnum oldType = StatementEnum
                                                .deserialize(oldval);

                                        if (spo.override) {
                                            
                                            if (oldType != spo.type) {
                                                
                                                /*
                                                 * We are downgrading a
                                                 * statement from explicit to
                                                 * inferred during TM
                                                 */
                                                
                                                ndx.insert(keys[i], spo.type
                                                        .serialize());

                                                writeCount++;

                                            }
                                                                                
                                        } else {

                                            // proposed statement type.
                                            StatementEnum newType = spo.type;

                                            // choose the max of the old and the
                                            // proposed.
                                            StatementEnum maxType = StatementEnum
                                                    .max(oldType, newType);

                                            if (oldType != maxType) {

                                                /*
                                                 * write on the index iff the
                                                 * type was actually changed.
                                                 */

                                                ndx.insert(keys[i], maxType
                                                        .serialize());

                                                writeCount++;

                                            }

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

                    futures = writeService.invokeAll(tasks);

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
         * Note: This capacity estimate is based on N longs per SPO, one head,
         * and 2-3 SPOs in the tail. The capacity will be extended automatically
         * if necessary.
         */
        
        KeyBuilder keyBuilder = new KeyBuilder(N * (1 + 3) * Bytes.SIZEOF_LONG);
        
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

    /**
     * @todo It might be worth doing a more efficient method for bulk statement
     *       removal. This will wind up doing M * N operations. The N are
     *       parallelized, but the M are not.
     */
    public int removeStatements(ISPOIterator itr) {

        final long begin = System.currentTimeMillis();

        int n = 0;

        try {

            while (itr.hasNext()) {

                SPO[] chunk = itr.nextChunk();

                for (SPO spo : chunk) {

                    n += getAccessPath(spo.s, spo.p, spo.o).removeAll();

                }

            }

        } finally {

            itr.close();
            
        }

        final long elapsed = System.currentTimeMillis() - begin;

        log.info("Retracted " + n + " statements in " + elapsed + " ms");

        return n;

    }

}
