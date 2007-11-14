/*
 * Copyright SYSTAP, LLC 2006-2007.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Apr 12, 2007
 */

package com.bigdata.rdf.sail;

import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.Semaphore;

import org.CognitiveWeb.util.PropertyUtil;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sesame.sail.NamespaceIterator;
import org.openrdf.sesame.sail.RdfRepository;
import org.openrdf.sesame.sail.SailChangedEvent;
import org.openrdf.sesame.sail.SailChangedListener;
import org.openrdf.sesame.sail.SailInitializationException;
import org.openrdf.sesame.sail.SailInternalException;
import org.openrdf.sesame.sail.SailUpdateException;
import org.openrdf.sesame.sail.StatementIterator;
import org.openrdf.sesame.sail.query.PathExpression;
import org.openrdf.sesame.sail.query.Query;
import org.openrdf.sesame.sail.query.TriplePattern;
import org.openrdf.sesame.sail.query.Var;
import org.openrdf.sesame.sail.util.EmptyStatementIterator;
import org.openrdf.sesame.sail.util.SailChangedEventImpl;

import com.bigdata.rdf.inf.InferenceEngine;
import com.bigdata.rdf.inf.TMStatementBuffer;
import com.bigdata.rdf.inf.TMStatementBuffer.BufferEnum;
import com.bigdata.rdf.model.OptimizedValueFactory;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.InferredSPOFilter;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IAccessPath;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.rdf.store.StatementWithType;
import com.bigdata.rdf.store.AbstractTripleStore.EmptyAccessPath;

/**
 * A Sesame 1.x SAIL integration.
 * <p>
 * Note: Sesame 1.x coupled the control logic and data structures in such a way
 * that you could not write your own JOIN operators, which makes it very
 * difficult to optimize performance.
 * 
 * <h2>SAIL Transactions</h2>
 * 
 * The SAIL transaction model is extended to include an
 * {@link #abortTransaction()} operation. In order to ensure that the SAIL
 * remains available to other writers you MUST code a transaction as follows:
 * 
 * <pre>
 * repo.startTransaction();
 * 
 * try {
 * 
 *     // do work
 * 
 *     repo.commitTransaction();
 * 
 * } catch (Throwable t) {
 * 
 *     repo.abortTransaction();
 * 
 * }
 * </pre>
 * 
 * This SAIL enforces the constraint that there may be at most one writer. This
 * is done in {@link #startTransaction()}. If a 2nd thread attempts to start a
 * transaction while one is already started then that thread will block until
 * the SAIL becomes available again for writers.
 * 
 * <h2>Concurrent Query</h2>
 * 
 * While this class, by itself, is NOT safe for readers that run concurrently
 * with a writer, it provides a read-committed view that is safe for concurrent
 * readers and that is safe to use while a writer is concurrently running a
 * transaction. See {@link #asReadCommittedView()}.
 * <p>
 * In order to use concurrent query you basically have to decide whether an
 * operation is read-only or read-write. A read-only operation should be
 * directed to the {@link #asReadCommittedView()} while read-write operations
 * should obtain a lock on the mutable SAIL using {@link #startTransaction()}
 * before doing any other work. When approached in this manner, you will be able
 * to execute read operations (for example, high-level query) with full
 * concurrency and SAIL transactions will be serialized (at most one will run at
 * a time) but they will not block readers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataRdfRepository extends AbstractRdfRepository implements RdfRepository {

    /**
     * Logger.
     */
    public static final Logger log = Logger
            .getLogger(BigdataRdfRepository.class);

    /**
     * Additional parameters understood by the Sesame 1.x SAIL implementation.
     * 
     * @todo also extend Options for the scale-out triple store once defined.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends com.bigdata.rdf.store.LocalTripleStore.Options {

        /**
         * This optional boolean property may be used to specify whether or not
         * RDFS entailments are maintained by eager closure of the knowledge
         * base (the default is <code>false</code>, but that is overriden by
         * {@link BigdataRdfSchemaRepository}).
         */
        public static final String TRUTH_MAINTENANCE = "truthMaintenance"; 

        public static final String DEFAULT_TRUTH_MAINTENANCE = "false"; 

        /**
         * The property whose value is the name of the {@link ITripleStore} 
         * implementation that will be instantiated.  An {@link InferenceEngine} 
         * will be used to wrap that {@link ITripleStore}.
         */
        public static final String STORE_CLASS = "storeClass";
        
        public static final String DEFAULT_STORE_CLASS = LocalTripleStore.class.getName();

        /**
         * The capacity of the buffers used to absorb writes.
         * 
         * @todo name conflict?
         */
        public static final String BUFFER_CAPACITY = "bufferCapacity";

        public static final String DEFAULT_BUFFER_CAPACITY = "10000";

    }

    /**
     * The equivilent of a null identifier for an internal RDF Value.
     */
    protected static final long NULL = IRawTripleStore.NULL;
    
    final protected OptimizedValueFactory valueFactory = OptimizedValueFactory.INSTANCE;

    protected AbstractTripleStore database;
    
    /**
     * The inference engine if the SAIL is using one.
     * <p>
     * Note: Requesting this object will cause the axioms to be written onto the
     * database if they are not already present. If this is a read-only view and
     * the mutable view does not already have the axioms defined then this will
     * cause an exception to be thrown since the indices are not writable by the
     * read-only view.
     */
    public InferenceEngine getInferenceEngine() {
        
        if(_inf==null) {
        
            _inf = database.getInferenceEngine();
            
        }
        
        return _inf;
        
    }
    
    // hard reference cache.
    private InferenceEngine _inf = null;
    
    protected Properties properties;

    private int bufferCapacity = -1;
    
    /**
     * When true, the RDFS closure will be maintained.
     */
    private boolean truthMaintenance;
    
    /**
     * When true, the RDFS closure will be maintained by the <em>SAIL</em>
     * implementation (but not by methods that go around the SAIL).
     */
    public boolean getTruthMaintenance() {
        
        return truthMaintenance;
        
    }
    
    /**
     * Used to buffer statements that are being asserted so as to maximize the
     * opportunity for batch writes.  Truth maintenance (if enabled) will be
     * performed no later than the commit of the transaction.
     * <p>
     * Note: When non-empty, the buffer MUST be flushed (a) if it is not empty
     * and a transaction completes (otherwise writes will not be stored on the
     * database); or (b) if there is a read against the database during a
     * transaction (otherwise reads will not see the unflushed statements).
     */
    private IStatementBuffer assertBuffer;
    
    /**
     * Used to collect statements being retracted IFF truth maintenance is
     * enabled.
     */
    private TMStatementBuffer retractBuffer;
    
    /**
     * When true, a SAIL "transaction" is running.
     */
    private boolean transactionStarted;
    
    /**
     * The implementation object.
     */
    public AbstractTripleStore getDatabase() {
        
        return database;
        
    }
    
    /**
     * Create a repository.
     * 
     * @see #initialize(Map)
     */
    public BigdataRdfRepository() {
    }

    /**
     * Variant constructor used to wrap an exiting database.
     * <p>
     * Note: you MUST still invoke {@link #initialize(Map)}.
     * 
     * @param database
     *            The database.
     */
    public BigdataRdfRepository(AbstractTripleStore database) {

        this.database = database;
        
    }
    
    private WeakReference<BigdataRdfRepository> readCommittedRef;
    
    /**
     * A factory returning the singleton read-committed view of the database.
     * <p>
     * Note: This is only supported when the {@link Options#STORE_CLASS}
     * specifies {@link LocalTripleStore} or {@link ScaleOutTripleStore}.
     */
    public BigdataRdfRepository asReadCommittedView() {
        
        synchronized(this) {
        
            BigdataRdfRepository view = readCommittedRef == null ? null
                    : readCommittedRef.get();
            
            if(view == null) {
                
                if(database instanceof LocalTripleStore) {

                    /*
                     * Create the singleton using a read-committed view of the
                     * database.
                     */
                    
                    view = new BigdataRdfRepository(((LocalTripleStore)database).asReadCommittedView());
                    
                    readCommittedRef = new WeakReference<BigdataRdfRepository>(view);
                    
                } else if(database instanceof ScaleOutTripleStore) {
                    
                    /*
                     * The scale-out triple store already has read-committed
                     * semantics since it uses unisolated writes on the data
                     * service.  Therefore we just disable startTransaction()
                     * on the returned singleton.
                     */
                    
                    view = new BigdataRdfRepository(database) {
                      
                        public void startTransaction() {
                            
                            throw new UnsupportedOperationException("Read-only");
                            
                        }
                        
                    };
                                        
                    readCommittedRef = new WeakReference<BigdataRdfRepository>(view);
                    
                } else {
                    
                    throw new UnsupportedOperationException(Options.STORE_CLASS + "="
                            + database.getClass().getName());                    

                }
                
                try {

                    view.initialize(properties);
                    
                } catch (SailInitializationException e) {
                    
                    throw new RuntimeException(e);
                    
                }

            }
            
            return view; 
        
        }
        
    }
    
    /**
     * Initialize the repository.
     * <p>
     * Note: When using a constructor variant that accepts an existing
     * {@link AbstractTripleStore} the {@link InferenceEngine} will be
     * configured based on the properties used to configure that
     * {@link AbstractTripleStore} NOT those specified here.
     * 
     * @param configParams
     *            See {@link Options}.
     */
    public void initialize(Map configParams) throws SailInitializationException {

        properties = PropertyUtil.flatCopy(PropertyUtil.convert(configParams));

        String val;

        // truthMaintenance

        truthMaintenance = Boolean.parseBoolean(properties.getProperty(
                Options.TRUTH_MAINTENANCE, Options.DEFAULT_TRUTH_MAINTENANCE));

        log.info(Options.TRUTH_MAINTENANCE + "=" + truthMaintenance);
        
        if(database==null) {

            /*
             * Create/re-open the database.
             */
            
            final AbstractTripleStore database;
            
            val = properties.getProperty(Options.STORE_CLASS,Options.DEFAULT_STORE_CLASS);

            try {

                Class storeClass = Class.forName(val);

                if(!ITripleStore.class.isAssignableFrom(storeClass)) {
                    
                    throw new SailInitializationException("Must extend "
                            + ITripleStore.class.getName() + " : "
                            + storeClass.getName()); 
                    
                }
                
                Constructor ctor = storeClass.getConstructor(new Class[]{Properties.class});
                
                database = (AbstractTripleStore) ctor.newInstance(new Object[]{properties});

            } catch(SailInitializationException ex) {
                
                throw ex;
                
            } catch(Throwable t) {
                
                throw new SailInitializationException(t);
                
            }
            
            // the database.
            this.database = database;
            
//            // inference engine used to maintain RDF(S) closure.
//            this.inf = new InferenceEngine(PropertyUtil
//                    .convert(configParams), database);
            
        } else {
            
//            this.inf = database.getInferenceEngine();
            
        }

        bufferCapacity = Integer.parseInt(properties.getProperty(
                Options.BUFFER_CAPACITY, Options.DEFAULT_BUFFER_CAPACITY));
        
        if(truthMaintenance) {

            InferenceEngine inf = getInferenceEngine();
            
            assertBuffer = new TMStatementBuffer(inf, bufferCapacity,
                    BufferEnum.AssertionBuffer);

            retractBuffer = new TMStatementBuffer(inf, bufferCapacity,
                    BufferEnum.RetractionBuffer);
            
        } else {

            assertBuffer = new StatementBuffer(database, bufferCapacity);
            
            retractBuffer = null;  // Not used (deletes are immediate).
            
        }

    }
    
    public void shutDown() {

        /*
         * Note: This is an immediate shutdown.
         */
        
        database.close();
        
    }

    //
    // SailChangedListener support.
    //

    /**
     * Vector of transient {@link SailChangedListener}s registered with this
     * SAIL.
     */
    private Vector<SailChangedListener> m_listeners = null;
    private boolean m_stmtAdded = false;
    private boolean m_stmtRemoved = false;    

    public void addListener(SailChangedListener listener) {

        if( m_listeners == null ) {
            
            m_listeners = new Vector<SailChangedListener>();
            
            m_listeners.add( listener );
            
        } else {
            
            if( m_listeners.contains( listener ) ) {
                
                throw new IllegalStateException
                    ( "Already registered: listener="+listener
                      );
                
            }
            
            m_listeners.add( listener );
            
        }

    }

    public void removeListener(SailChangedListener listener) {

        if( m_listeners == null ) {
            
            throw new IllegalStateException
                ( "Not registered: listener="+listener
                  );
            
        }
        
        if( ! m_listeners.remove( listener ) ) {
            
            throw new IllegalStateException
                ( "Not registered: listener="+listener
                  );
            
        }

    }

    /**
     * Notifies {@link SailChangedListener}s if one or more statements have
     * been added to or removed from the repository using the SAIL methods:
     * <ul>
     * 
     * <li> {@link #addStatement(Resource, URI, Value)}
     * <li> {@link #removeStatements(Resource, URI, Value)}
     * <li> {@link #clearRepository()}
     * </ul>
     */
    synchronized protected void fireSailChangedEvents()
    {
        
        if( m_listeners == null ) return;
        
        if( ! m_stmtAdded && ! m_stmtRemoved ) return;

        SailChangedEvent e = new SailChangedEventImpl
            ( m_stmtAdded,
              m_stmtRemoved
              );
        
        SailChangedListener[] listeners = (SailChangedListener[]) 
            m_listeners.toArray( new SailChangedListener[]{} );

        for( int i=0; i<listeners.length; i++ ) {
            
            SailChangedListener l = listeners[ i ];
            
            l.sailChanged( e );
                
        }
        
    }

    public void addStatement(Resource s, URI  p, Value o)
            throws SailUpdateException {

        assertTransactionStarted();

        // flush any pending retractions first!
        
        if(retractBuffer!=null && !retractBuffer.isEmpty()) {
            
            retractBuffer.doClosure();
            
        }

        s = (Resource) valueFactory.toNativeValue(s);

        p = (URI) valueFactory.toNativeValue(p);

        o = (Value) valueFactory.toNativeValue(o);
        
        // buffer the assertion.
        
        assertBuffer.add(s, p, o);
        
    }

    public void changeNamespacePrefix(String namespace, String prefix)
            throws SailUpdateException {

        assertTransactionStarted();

        database.addNamespace(namespace,prefix);
        
    }

    public NamespaceIterator getNamespaces() {

        /*
         * Note: You do NOT need to flush the buffer since this does not read
         * statements.
         */

        return new MyNamespaceIterator(database.getNamespaces().entrySet().iterator());

    }

    private class MyNamespaceIterator implements NamespaceIterator {

        private final Iterator<Map.Entry<String/*namespace*/,String/*prefix*/>> src;
        
        private Map.Entry<String/*namespace*/,String/*prefix*/> current = null;
        
        public MyNamespaceIterator(Iterator<Map.Entry<String/*namespace*/,String/*prefix*/>> src) {
            
            assert src != null;
            
            this.src = src;
            
        }
        
        public boolean hasNext() {
            
            return src.hasNext();
            
        }

        public void next() {
         
            current = src.next();
            
        }

        public String getName() {
            
            if(current==null) throw new IllegalStateException();
            
            return current.getKey();
            
        }

        public String getPrefix() {

            if(current==null) throw new IllegalStateException();
            
            return current.getValue();
               
        }

        public void close() {
            
            // NOP.
            
        }
        
    }
    
    /**
     * Note: Since there is only one RdfRepository per persistence store, the
     * easiest way to achive this end is to delete the persistence store and
     * open/create a new one.
     */
    public void clearRepository() throws SailUpdateException {

        assertTransactionStarted();

        // discard any pending asserts.
        assertBuffer.clear();
        
        if(retractBuffer!=null) {
        
            // discard any pending retracts.
            retractBuffer.clear();
            
        }
        
        // clear the database.
        database.clear();
        
        m_stmtRemoved = true;
        
    }

    public int removeStatements(Resource s, URI p, Value o)
            throws SailUpdateException {
        
        assertTransactionStarted();
        
        // flush any pending assertions first!
        
        assertBuffer.flush();
        
        if(getTruthMaintenance()) {
        
            // do truth maintenance, writing on the database.
            
            ((TMStatementBuffer)assertBuffer).doClosure();
            
        }

        // #of explicit statements removed.
        final int n;

        if (getTruthMaintenance()) {

            /*
             * Since we are doing truth maintenance we need to copy the matching
             * "explicit" statements into a temporary store rather than deleting
             * them directly. This uses the internal API to copy the statements
             * to the temporary store without materializing them as Sesame
             * Statement objects.
             */
            
            /*
             * obtain a chunked iterator using the triple pattern that visits
             * only the explicit statements.
             */
            ISPOIterator itr = database.getAccessPath(s,p,o).iterator(ExplicitSPOFilter.INSTANCE);
            
            // copy explicit statements to retraction buffer.
            n = retractBuffer.getStatementStore().addStatements(itr,null/*filter*/);

        } else {

            /*
             * Since we are not doing truth maintenance, just remove the
             * statements from the database (synchronous, batch api, not
             * buffered).
             */
            
            n = database.removeStatements(s, p, o);

        }

        if (n > 0) {

            m_stmtRemoved = true;
            
        }
        
        return n;

    }

    private Semaphore semaphore = new Semaphore(1);
    
    public void startTransaction() {
        
        // this will block until the semaphore is available.
        try {
            semaphore.acquire();
        } catch(InterruptedException ex) {
            throw new SailInternalException(ex);
        }
        
//        if(transactionStarted) {
//            
//            throw new SailInternalException(
//                    "A transaction was already started.");
//
//        }
        
        transactionStarted = true;
        
        m_stmtAdded = false;

        m_stmtRemoved = false;

    }

    public boolean transactionStarted() {

        return transactionStarted;
        
    }

    /**
     * This method is MUST be invoked if an error arises during the execution of
     * a transaction such that you wish to discard the write set of that
     * transaction. The method clears the statement buffers and discards any
     * writes on the database since the last commit. You SHOULD write your code
     * like this:
     * 
     * <pre>
     *  repo.startTransaction();
     *  try {
     *     ...
     *     repo.commitTransaction();
     *  } catch(Throwable t) {
     *     repo.abortTransaction();
     *  }
     * </pre>
     * 
     * If you fail to either abort or commit a transaction then the SAIL will
     * become unavailable to other writers!
     * <p>
     * Note: This method is NOT part of the Sesame API.
     * <p>
     * Note: The semantics depend on the {@link Options#STORE_CLASS}.  See
     * {@link ITripleStore#abort()}.
     */
    public void abortTransaction() {

        if( ! transactionStarted ) {
            
            throw new SailInternalException
                ( "No transaction has been started."
                  );

        }
        
        // discard any pending asserts.
        assertBuffer.clear();
        
        if(retractBuffer!=null) {
        
            // discard any pending retracts.
            retractBuffer.clear();
            
        }
        
        // discard the write set.
        database.abort();
        
        transactionStarted = false;

        // release the semaphore allowing another tx to start.
        semaphore.release();
        
    }
    
    /**
     * Commit the write set.
     * <p>
     * Note: The semantics depend on the {@link Options#STORE_CLASS}.  See
     * {@link ITripleStore#commit()}.
     * 
     * @see #abortTransaction()
     */
    public void commitTransaction() {

        if( ! transactionStarted ) {
            
            throw new SailInternalException
                ( "No transaction has been started."
                  );

        }

        /*
         * Flush any pending writes.
         * 
         * Note: This must be done before you compute the closure so that the
         * pending writes will be read by the inference engine when it computes
         * the closure.
         */
        
        flushStatementBuffers();
        
        database.commit();
        
        fireSailChangedEvents();

        transactionStarted = false;
        
        // release the semaphore allowing another tx to start.
        semaphore.release();

    }

    /**
     * Flush any pending assertions or retractions to the database using
     * efficient batch operations. If {@link #getTruthMaintenance()} returns
     * <code>true</code> this method will also handle truth maintenance.
     * <p>
     * Note: This tests whether or not a transaction has been started. It MUST
     * be invoked within any method that will read on the database to ensure
     * that any pending writes have been flushed (otherwise the read operation
     * will not be able to see the pending writes). However, methods that assert
     * or retract statements MUST only flush the buffer on which they will NOT
     * write.  E.g., if you are going to retract statements, then first flush
     * the assertions buffer and visa versa.
     */
    protected void flushStatementBuffers() {

        if (transactionStarted) {

            if (assertBuffer != null && !assertBuffer.isEmpty()) {

                assertBuffer.flush();
                
                if(getTruthMaintenance()) {

                    // do TM, writing on the database.
                    ((TMStatementBuffer)assertBuffer).doClosure();
                    
                }

                m_stmtAdded = true;

            }

            if (retractBuffer != null && !retractBuffer.isEmpty()) {

                // do TM, writing on the database.
                retractBuffer.doClosure();
                
                m_stmtRemoved = true;

            }
            
        }
        
    }
    
    protected void assertTransactionStarted() {

        if (!transactionStarted) {

            throw new SailInternalException("No transaction has been started");

        }
        
    }
    
    /**
     * Returns an iterator that visits {@link StatementWithType} objects.
     */
    public StatementIterator getStatements(Resource s, URI p, Value o) {

        flushStatementBuffers();

        IAccessPath accessPath = database.getAccessPath(s, p, o);

        if(accessPath instanceof EmptyAccessPath) {
            
            /*
             * One of the Values was unknown so the access path will be empty.
             * 
             * Note: This is true even if we are doing some backchaining.
             */
            
            return new EmptyStatementIterator();
            
        }
        
        /*
         * Some valid access path.
         */
        
        final ISPOIterator src;
        
        if(getTruthMaintenance()) {

            /*
             * Obtain an iterator that will generate any missing entailments at
             * query time. The behavior of the iterator depends on how the
             * InferenceEngine was configured.
             */
            
            long[] ids = accessPath.getTriplePattern();

            src = getInferenceEngine().backchainIterator(//
                    ids[0], ids[1], ids[2] // the triple pattern.
                    );
            
        } else {

            /*
             * Otherwise NO entailments are permitted and we only return the
             * statements actually present in the database.
             */
            
            // Read straight from the database.
            src = accessPath.iterator();
            
        }
        
        return database.asStatementIterator(src);

    }
        
    public ValueFactory getValueFactory() {
        
        return valueFactory;
        
    }

    public boolean hasStatement(Resource s, URI p, Value o) {

        flushStatementBuffers();

        if( URIImpl.RDF_TYPE.equals(p) && URIImpl.RDFS_RESOURCE.equals(o) ) {
            
            if (database.getTermId(s) != NULL) {
                
                return true;
                
            }
            
        }
        
        return database.hasStatement(s, p, o);
        
    }

    /**
     * 
     */
    public Query optimizeQuery(Query query) {
        
        flushStatementBuffers();
        
        /*
         * This static method is provided by the Sesame framework and performs a
         * variety of default optimizations.
         */

//        QueryOptimizer.optimizeQuery(query);
        
        /*
         * This variant is based on the Sesame optimizer but it uses range
         * counts to order triple patterns based on their actual selectiveness
         * in the data at the time that the query is run.  This can be a big
         * win depending on the query.
         */
        optimizeQuery2(query);

        /*
         * Replace all Value objects stored in variables with the corresponding
         * _Value objects.
         */

        replaceValuesInQuery
            ( query
              );

        return query;
        
    }

    /**
     * Computes the closure of the triple store for RDF(S)+ entailments.
     * <p>
     * This computes the full forward closure of the store. This can be used if
     * you do NOT enable truth maintenance and choose instead to load up all of
     * your data first and then compute the closure of the database.
     * <p>
     * Note: If there are already entailments in the database AND you have
     * retracted statements since the last time the closure was computed then
     * you MUST delete all entailments from the database before re-computing the
     * closure.
     * <p>
     * Note: This method does NOT commit the database. See
     * {@link ITripleStore#commit()} and {@link #getDatabase()}.
     * 
     * @see #removeAllEntailments()
     */
    public void computeClosure() {
        
        assertTransactionStarted();
        
        flushStatementBuffers();
        
        getInferenceEngine().computeClosure(null/*focusStore*/);
                
    }
    
    /**
     * Removes all "inferred" statements from the database.
     * <p>
     * Note: This does NOT commit the database.
     */
    public void removeAllEntailments() {
        
        assertTransactionStarted();
        
        flushStatementBuffers();

        database.getAccessPath(NULL, NULL, NULL).removeAll(
                InferredSPOFilter.INSTANCE);
        
    }
    
    /**
     * Estimate the #of results for a triple pattern.
     * <p>
     * Note: This MAY over-estimate since deleted entries that have not been
     * purged will be counted if an index supports isolation.
     * 
     * @param pe
     * @param rangeCounts
     * 
     * @return The estimated range count or <code>-1</code> if this is not a
     *         {@link TriplePattern}.
     */
    int getRangeCount(PathExpression pe,Hashtable<PathExpression,Integer> rangeCounts) {
        
        Integer rangeCount = rangeCounts.get(pe);
        
        if(rangeCount!=null) {
            
            return rangeCount;
            
        }
        
        if(pe instanceof TriplePattern) {
            
            TriplePattern tp = (TriplePattern)pe;
            
            rangeCount = rangeCount(tp);

            log.info("rangeCount: " + rangeCount + " : " + pe);
            
            rangeCounts.put(pe,rangeCount);
            
            return rangeCount.intValue();
            
        } else {
            
            return -1;
            
        }
        
    }

    int rangeCount(TriplePattern tp) {
        
        /*
         * Extract "variables". If hasValue() is true, then the variable is
         * actually bound.
         */
        Var svar = tp.getSubjectVar();
        Var pvar = tp.getPredicateVar();
        Var ovar = tp.getObjectVar();
        
        /*
         * Extract binding for variable or null iff not bound.
         */
        Resource s = svar.hasValue()?(Resource)svar.getValue():null;
    
        URI p = pvar.hasValue()?(URI)pvar.getValue():null;
        
        Value o = ovar.hasValue()?ovar.getValue():null;
        
        /*
         * convert other Value object types to our object types.
         */
        if (s != null)
            s = (Resource) valueFactory.toNativeValue(s);

        if (p != null)
            p = (URI) valueFactory.toNativeValue(p);

        if (o != null)
            o = (Value) valueFactory.toNativeValue(o);
        
        /*
         * convert our object types to internal identifiers.
         */
        long _s, _p, _o;

        _s = (s == null ? NULL : database.getTermId(s));
        _p = (p == null ? NULL : database.getTermId(p));
        _o = (o == null ? NULL : database.getTermId(o));

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
        
        return database.getAccessPath(_s, _p, _o).rangeCount();

    }
   
}
