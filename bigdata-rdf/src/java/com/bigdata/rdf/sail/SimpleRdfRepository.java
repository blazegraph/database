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
 * Created on Apr 12, 2007
 */

package com.bigdata.rdf.sail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.CognitiveWeb.util.PropertyUtil;
import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.sesame.sail.NamespaceIterator;
import org.openrdf.sesame.sail.RdfRepository;
import org.openrdf.sesame.sail.SailChangedEvent;
import org.openrdf.sesame.sail.SailChangedListener;
import org.openrdf.sesame.sail.SailInitializationException;
import org.openrdf.sesame.sail.SailInternalException;
import org.openrdf.sesame.sail.SailUpdateException;
import org.openrdf.sesame.sail.StatementIterator;
import org.openrdf.sesame.sail.query.Query;
import org.openrdf.sesame.sail.query.QueryOptimizer;
import org.openrdf.sesame.sail.query.Var;
import org.openrdf.sesame.sail.util.EmptyStatementIterator;
import org.openrdf.sesame.sail.util.SailChangedEventImpl;
import org.openrdf.sesame.sail.util.SingleStatementIterator;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.rdf.KeyOrder;
import com.bigdata.rdf.TripleStore;
import com.bigdata.rdf.inf.InferenceEngine;
import com.bigdata.rdf.inf.SPO;
import com.bigdata.rdf.model.OptimizedValueFactory;
import com.bigdata.rdf.model.OptimizedValueFactory._Resource;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;

/**
 * A simple Sesame 1.x SAIL integration.
 * <p>
 * This is very much a trial balloon. Sesame 1.x coupled the control logic and
 * data structures in such a way that you could not write your own JOIN
 * operators, which makes it very difficult to optimize performance. A very
 * basic and untuned mechanisms is provided to compute the
 * {@link #fullForwardClosure()} of the triple store (there is no truth
 * maintenance and you must re-compute the entire closure if you add more data
 * to the store) - use of this method will let you run high level queries using
 * the SAIL with entailments. Only a simple transaction model is supported (no
 * transactional isolation). The {@link TripleStore} uses an embedded write-only
 * {@link Journal} as its store (it does not support distributed scale-out). The
 * namespace management methods and the methods to clear the repository have not
 * been implemented.
 * <p>
 * A custom integration is provided for directly loading data into the triple
 * store with throughput of 20,000+ triples per second - see
 * {@link #loadData(java.io.File, String, boolean)}.
 * <p>
 * <em>THIS CLASS IS NOT THREAD SAFE</em>
 * <p>
 * <em>THIS CLASS IS NOT SAFE FOR TRAVERSAL UNDER CONCURRENT MODIFICATION</em>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SimpleRdfRepository implements RdfRepository {

    /**
     * Logger.
     */
    public static final Logger log = Logger.getLogger
    ( SimpleRdfRepository.class
      );

    /**
     * The equivilent of a null identifier for an internal RDF Value.
     */
    protected static final long NULL = TripleStore.NULL;
    
    private OptimizedValueFactory valueFactory;
    private InferenceEngine tripleStore;
    private Properties properties;
    private boolean transactionStarted;
    
    /**
     * The implementation object.
     */
    public TripleStore getTripleStore() {
        
        return tripleStore;
        
    }
    
    /**
     * Create a repository.
     * 
     * @see #initialize(Map)
     */
    public SimpleRdfRepository() {
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
     * Notifies {@link SailChangeListener}s if one or more statements
     * have been added to or removed from the repository using the SAIL
     * methods: <ul>
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

        s = (Resource) valueFactory.toNativeValue(s);

        p = (URI) valueFactory.toNativeValue(p);

        o = (Value) valueFactory.toNativeValue(o);
        
        tripleStore.addStatement(s, p, o);
        
        m_stmtAdded = true;
        
    }

    public void changeNamespacePrefix(String arg0, String arg1)
            throws SailUpdateException {

        assertTransactionStarted();

        // TODO Auto-generated method stub

        throw new UnsupportedOperationException();

    }

    public NamespaceIterator getNamespaces() {
        
        // TODO Auto-generated method stub

        throw new UnsupportedOperationException();
    }

    /**
     * Note: This is not implemented. Since there is only one RdfRepository per
     * persistence store, the easiest way to achive this end is to delete the
     * persistence store and open/create a new one.
     */
    public void clearRepository() throws SailUpdateException {

        assertTransactionStarted();

        // TODO Auto-generated method stub
        
        m_stmtRemoved = true;
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Materializes the set of statements matching the triple pattern and then
     * deletes those statements from each index using an ordered batch delete
     * operation.
     */
    public int removeStatements(Resource s, URI p, Value o)
            throws SailUpdateException {
        
        assertTransactionStarted();
        
        /*
         * convert other Value object types to our object types.
         */
        if (s != null)
            s = (Resource) valueFactory.toNativeValue(s);

        if (p != null)
            p = (URI) valueFactory.toNativeValue(p);

        if (o != null)
            o = (Value) valueFactory.toNativeValue(o);
        
        int n = tripleStore.removeStatements(s, p, o);
        
        if (n > 0) {
            
            m_stmtRemoved = true;
            
        }
        
        return n;

    }

    public void startTransaction() {
        
        if(transactionStarted) {
            
            throw new SailInternalException(
                    "A transaction was already started.");

        }
        
        transactionStarted = true;
        
        m_stmtAdded = false;

        m_stmtRemoved = false;

    }

    public boolean transactionStarted() {

        return transactionStarted;
        
    }

    public void commitTransaction() {

        if( ! transactionStarted ) {
            
            throw new SailInternalException
                ( "No transaction has been started."
                  );

        }

        /*
         * @todo make closure optional? Is closure required by the Sesame test
         * suite?  Mark entailments in the KB using the btree entry value?
         */
        tripleStore.fullForwardClosure();
        
        tripleStore.commit();
        
        transactionStarted = false;
        
        fireSailChangedEvents();

    }

    protected void assertTransactionStarted() {

        if (!transactionStarted) {

            throw new SailInternalException("No transaction has been started");

        }
        
    }
    
    public StatementIterator getStatements(Resource s, URI p, Value o) {

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

        _s = (s == null ? NULL : tripleStore.getTermId(s));
        _p = (p == null ? NULL : tripleStore.getTermId(p));
        _o = (o == null ? NULL : tripleStore.getTermId(o));

        /*
         * If a value was specified and it is not in the terms index then the
         * statement can not exist in the KB.
         */
        if (_s == NULL && s != null) {

            return new EmptyStatementIterator();
            
        }
        
        if (_p == NULL && p != null) {
        
            return new EmptyStatementIterator();
            
        }
        
        if (_o == NULL && o != null) {
            
            return new EmptyStatementIterator();
            
        }
        
        /*
         * if all bound, then a slight optimization.
         */
        if (_s != NULL && _p != NULL && _o != NULL) {

            if( tripleStore.getSPOIndex().contains(tripleStore.keyBuilder.statement2Key(_s, _p, _o))) {
                
                return new SingleStatementIterator(s,p,o);
                
            } else {
                
                return new EmptyStatementIterator();
                
            }
            
        }
        
        /*
         * Choose the access path and encapsulate the resulting range iterator
         * as a sesame statement iterator.
         */
        return new MyStatementIterator(KeyOrder.getKeyOrder(_s, _p, _o),
                tripleStore.rangeQuery(_s, _p, _o));
        
    }
    
    /**
     * Wraps the raw iterator that traverses a statement index and exposes each
     * visited statement as a {@link Statement} object.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class MyStatementIterator implements StatementIterator {

        private final KeyOrder keyOrder;
        private final IEntryIterator src;
        
        public MyStatementIterator(KeyOrder keyOrder,IEntryIterator src) {
            
            if(keyOrder == null) throw new IllegalArgumentException();

            if(src == null) throw new IllegalArgumentException();
            
            this.keyOrder = keyOrder;
            
            this.src = src;
            
        }
        
        public boolean hasNext() {
            
            return src.hasNext();
            
        }

        public Statement next() {
            
            // visit the next entry.
            src.next();

            // unpacks the term identifiers from the key.
            SPO spo = new SPO(keyOrder, tripleStore.keyBuilder, src.getKey());
            
            // resolves the term identifiers to the terms.
            return new _Statement((_Resource) tripleStore.getTerm(spo.s),
                    (_URI) tripleStore.getTerm(spo.p), (_Value) tripleStore
                            .getTerm(spo.o));
            
        }
        
        public void close() {
            // NOP.
        }

    }

    public ValueFactory getValueFactory() {
        
        return valueFactory;
        
    }

    public boolean hasStatement(Resource s, URI p, Value o) {

        if (s != null)
            s = (Resource) valueFactory.toNativeValue(s);

        if (p != null)
            p = (URI) valueFactory.toNativeValue(p);

        if (o != null)
            o = (Value) valueFactory.toNativeValue(o);
        
        return tripleStore.containsStatement(s, p, o);
        
    }

    /**
     * @todo rewrite the join order based on selectivity.
     */
    public Query optimizeQuery(Query query) {
        
        /*
         * This static method is provided by the Sesame framework and performs a
         * variety of default optimizations.
         */

        QueryOptimizer.optimizeQuery(query);

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
     * Replace all {@link Value} objects stored in variables with the
     * corresponding {@link _Value} objects.
     * <p>
     * 
     * Note: This can cause unknown terms to be inserted into store.
     */
    private void replaceValuesInQuery(Query query) {

        final List varList = new ArrayList();

        query.getVariables(varList);

        for (int i = 0; i < varList.size(); i++) {

            final Var var = (Var) varList.get(i);

            if (var.hasValue()) {

                final Value value = var.getValue();

                if (value instanceof URI) {

                    // URI substitution.

                    String uriString = ((URI) value).getURI();

                    var.setValue(valueFactory.createURI(uriString));

                } else if (value instanceof BNode) {

                    // BNode substitution.

                    final String id = ((BNode) value).getID();

                    if (id == null) {

                        var.setValue(valueFactory.createBNode());

                    } else {

                        var.setValue(valueFactory.createBNode(id));

                    }

                } else if (value instanceof Literal) {

                    // Literal substitution.

                    Literal lit = (Literal) value;

                    final String lexicalForm = lit.getLabel();

                    final String language = lit.getLanguage();

                    if (language != null) {

                        lit = valueFactory.createLiteral(lexicalForm, language);

                    } else {

                        URI datatype = lit.getDatatype();

                        if (datatype != null) {

                            lit = valueFactory.createLiteral(lexicalForm,
                                    datatype);

                        } else {

                            lit = valueFactory.createLiteral(lexicalForm);

                        }

                    }

                    var.setValue(lit);

                } // if( literal )

            } // if( hasValue )

        } // next variable.

    }

    /**
     * @param configParams
     *            See {@link Options} for the persistence store options.
     *            <p>
     *            The optional boolean option "eagerClosure" may be used to turn
     *            on a simple inference engine that will compute the eager
     *            closure of data loaded into the store via either the SAIL
     *            transaction mechanism or the batch load mechanism. The
     *            selection of this option is NOT restart safe (it is not saved
     *            in the store).
     * 
     * @see #loadData(File, String, boolean)
     */
    public void initialize(Map configParams) throws SailInitializationException {

        properties = PropertyUtil.flatCopy(PropertyUtil.convert(configParams));

        valueFactory = new OptimizedValueFactory();
        
        try {

             tripleStore = new InferenceEngine(properties);
            
        } catch(IOException ex) {
            
            throw new SailInitializationException(ex);
            
        }
        
    }

    public void shutDown() {

        tripleStore.shutdown();
        
    }

    /**
     * Uses a fast batch loader to load the data into the store.
     * <p>
     * This method lies outside of the SAIL and does not rely on the SAIL
     * "transaction" mechanisms.
     * 
     * @param file
     *            The file containing the data.
     * @param baseURI
     *            The base URI (optional).
     * @param commit
     *            When true, the store will be committed after the data load
     *            (you can use false here if you will lots of small files in
     *            sequence).
     */
    public void loadData(File file, String baseURI, boolean commit)
            throws IOException {

        tripleStore.loadData(file, baseURI, commit);
        
    }
    
    /**
     * Computes the closure of the triple store for RDFS entailments. This
     * implementation computes the full forward closure of the store and then
     * commits the store. Since incremental closure and truth maintenance are
     * NOT supported, you should load all your data into store using
     * {@link #loadData(File, String, boolean)} and then invoke this method to
     * compute the RDFS entailments.
     * <p>
     * This method lies outside of the SAIL and does not rely on the SAIL
     * "transaction" mechanisms.
     */
    public void fullForwardClosure() {
        
        tripleStore.fullForwardClosure();
        
        tripleStore.commit();
                
    }
    
}
