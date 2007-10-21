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
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
import org.openrdf.sesame.sail.query.BooleanExpr;
import org.openrdf.sesame.sail.query.DirectSubClassOf;
import org.openrdf.sesame.sail.query.DirectSubPropertyOf;
import org.openrdf.sesame.sail.query.DirectType;
import org.openrdf.sesame.sail.query.GraphPattern;
import org.openrdf.sesame.sail.query.GraphPatternQuery;
import org.openrdf.sesame.sail.query.PathExpression;
import org.openrdf.sesame.sail.query.Query;
import org.openrdf.sesame.sail.query.SetOperator;
import org.openrdf.sesame.sail.query.TriplePattern;
import org.openrdf.sesame.sail.query.ValueCompare;
import org.openrdf.sesame.sail.query.ValueExpr;
import org.openrdf.sesame.sail.query.Var;
import org.openrdf.sesame.sail.util.EmptyStatementIterator;
import org.openrdf.sesame.sail.util.SailChangedEventImpl;
import org.openrdf.sesame.sail.util.SingleStatementIterator;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.rdf.inf.InferenceEngine;
import com.bigdata.rdf.model.OptimizedValueFactory;
import com.bigdata.rdf.model.OptimizedValueFactory._Resource;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;

/**
 * A Sesame 1.x SAIL integration.
 * <p>
 * Note: Sesame 1.x coupled the control logic and data structures in such a way
 * that you could not write your own JOIN operators, which makes it very
 * difficult to optimize performance.
 * <p>
 * Note: Only a simple transaction model is supported. There is no transactional
 * isolation. Queries run against the "live" indices and therefore CAN NOT be
 * used concurrently or concurrently with writes on the store.
 * <p>
 * A custom integration is provided for directly loading data into the triple
 * store with throughput of 20,000+ triples per second - see
 * {@link LocalTripleStore#loadData(File, String, org.openrdf.sesame.constants.RDFFormat, boolean, boolean)}.
 * This method lies outside of the SAIL and does NOT rely on the SAIL
 * "transaction" mechanisms. This method does NOT perform RDFS closure - you
 * must explicitly request that yourself, e.g., by specifying
 * <code>commit:=false</code> and then invoking {@link #fullForwardClosure()}.
 * <p>
 * <em>THIS CLASS IS NOT THREAD SAFE</em>
 * <p>
 * <em>THIS CLASS IS NOT SAFE FOR TRAVERSAL UNDER CONCURRENT MODIFICATION</em>
 * 
 * FIXME Queries could run concurrently against the last committed state of the
 * store. This would require an {@link RdfRepository} wrapper that was
 * initialized from the last commit record on the store and then was used to
 * execute queries. In turn, that should probably be built over a read-only
 * {@link ITripleStore} reading from the last commit record on the store at the
 * time that the view is created.
 * 
 * FIXME Support a better truth maintanence strategy
 * <p>
 * A very basic and untuned mechanism is provided to compute the
 * {@link #fullForwardClosure()} of the triple store (there is no truth
 * maintenance and you must re-compute the entire closure if you add more data
 * to the store) - use of this method will let you run high level queries using
 * the SAIL with entailments.
 * 
 * @todo The namespace management methods have not been implemented.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataRdfRepository implements RdfRepository {

    /**
     * Logger.
     */
    public static final Logger log = Logger.getLogger
    ( BigdataRdfRepository.class
      );

    /**
     * The equivilent of a null identifier for an internal RDF Value.
     */
    protected static final long NULL = ITripleStore.NULL;
    
    protected OptimizedValueFactory valueFactory;

    protected InferenceEngine inferenceEngine;
    
    protected AbstractTripleStore database;
    
    protected Properties properties;
    
    /**
     * When true, the RDFS closure will be maintained.
     */
    private boolean rdfsClosure;
    
    /**
     * When true, the RDFS closure will be maintained by the <em>SAIL</em>
     * implementation (but not by methods that go around the SAIL).
     */
    public boolean isRdfsClosure() {
        
        return rdfsClosure;
        
    }
    
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
        
        database.addStatement(s, p, o);
        
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
     * Note: Since there is only one RdfRepository per persistence store, the
     * easiest way to achive this end is to delete the persistence store and
     * open/create a new one.
     */
    public void clearRepository() throws SailUpdateException {

        assertTransactionStarted();

//        ((BTree)database.getTermIdIndex()).removeAll();
//        ((BTree)database.getIdTermIndex()).removeAll();
//        ((BTree)database.getSPOIndex()).removeAll();
//        ((BTree)database.getPOSIndex()).removeAll();
//        ((BTree)database.getOSPIndex()).removeAll();

        database.clear();
        
        m_stmtRemoved = true;
        
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
        
        int n = database.removeStatements(s, p, o);
        
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
         * Optionally compute the full forward closure over the store.
         * 
         * @todo In order to handle closure properly we should delete the
         * closure if any statements are removed from the store (the entailments
         * are not explicitly marked so this SAIL does not do this).
         */
        if (rdfsClosure) {

            inferenceEngine.fullForwardClosure();
            
        }
        
        database.commit();
        
        if(true) database.dumpStore();
        
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

        _s = (s == null ? NULL : database.getTermId(s));
        _p = (p == null ? NULL : database.getTermId(p));
        _o = (o == null ? NULL : database.getTermId(o));

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

            if (database.getSPOIndex().contains(
                    database.getKeyBuilder().statement2Key(_s, _p, _o))) {
                
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
                database.rangeQuery(_s, _p, _o));
        
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
        private final RdfKeyBuilder keyBuilder;
        
        public MyStatementIterator(KeyOrder keyOrder,IEntryIterator src) {
            
            if(keyOrder == null) throw new IllegalArgumentException();

            if(src == null) throw new IllegalArgumentException();
            
            this.keyOrder = keyOrder;
            
            this.src = src;

            this.keyBuilder = database.getKeyBuilder();
            
        }
        
        public boolean hasNext() {
            
            return src.hasNext();
            
        }

        public Statement next() {
            
            // visit the next entry.
            src.next();

            // unpacks the term identifiers from the key.
            SPO spo = new SPO(keyOrder, keyBuilder, src.getKey());
            
            // resolves the term identifiers to the terms.
            return new _Statement((_Resource) database.getTerm(spo.s),
                    (_URI) database.getTerm(spo.p), (_Value) database
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
        
        return database.containsStatement(s, p, o);
        
    }

    /**
     * 
     */
    public Query optimizeQuery(Query query) {
        
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
     * An attempt to get the Sesame query optimizer to choose the join
     * order based on the actual selectivity of the triple patterns.
     * 
     * @param qc
     */
    private void optimizeQuery2(Query qc) {
        if (qc instanceof GraphPatternQuery) {
            GraphPatternQuery gpQuery = (GraphPatternQuery)qc;
            _optimizeGraphPattern(gpQuery.getGraphPattern(), new HashSet());
        }
        else if (qc instanceof SetOperator) {
            SetOperator setOp = (SetOperator)qc;
            optimizeQuery( setOp.getLeftArg() );
            optimizeQuery( setOp.getRightArg() );
        }
    }

    private void _optimizeGraphPattern(GraphPattern graphPattern, Set boundVars) {
        // Optimize any optional child graph patterns:
        Iterator iter = graphPattern.getOptionals().iterator();

        if (iter.hasNext()) {
            // Build set of variables that are bound in this scope
            Set scopeVars = new HashSet(boundVars);
            graphPattern.getLocalVariables(scopeVars);

            // Optimize recursively
            while (iter.hasNext()) {
                GraphPattern optionalGP = (GraphPattern)iter.next();
                _optimizeGraphPattern(optionalGP, new HashSet(scopeVars));
            }
        }

        // Optimize the GraphPattern itself:
        _inlineVarAssignments(graphPattern);
        _orderExpressions(graphPattern, boundVars);
    }
    
    /**
     * Inlines as much of the "variable assignments" (comparison between a
     * variable and fixed value) that are found in the list of conjunctive
     * constraints as possible, and removes them from the query. Only variable
     * assignments for variables that are used in <tt>graphPattern</tt> itself
     * are processed. Inlining variable assignments for variables that are
     * (only) used in optional child graph patterns leads to incorrect query
     * evaluation.
     **/
    private void _inlineVarAssignments(GraphPattern graphPattern) {
        Set localVars = new HashSet();
        graphPattern.getLocalVariables(localVars);

        boolean constraintsModified = false;

        List conjunctiveConstraints =
                new ArrayList(graphPattern.getConjunctiveConstraints());

        Iterator iter = conjunctiveConstraints.iterator();

        while (iter.hasNext()) {
            BooleanExpr boolExpr = (BooleanExpr)iter.next();

            if (boolExpr instanceof ValueCompare) {
                ValueCompare valueCompare = (ValueCompare)boolExpr;

                if (valueCompare.getOperator() != ValueCompare.EQ) {
                    continue;
                }

                ValueExpr arg1 = valueCompare.getLeftArg();
                ValueExpr arg2 = valueCompare.getRightArg();

                Var varArg = null;
                Value value = null;

                if (arg1 instanceof Var && arg1.getValue() == null && // arg1 is an unassigned var 
                    arg2.getValue() != null) // arg2 has a value
                {
                    varArg = (Var)arg1;
                    value = arg2.getValue();
                }
                else if (arg2 instanceof Var && arg2.getValue() == null && // arg2 is an unassigned var
                    arg1.getValue() != null) // arg1 has a value
                {
                    varArg = (Var)arg2;
                    value = arg1.getValue();
                }

                if (varArg != null && localVars.contains(varArg)) {
                    // Inline this variable assignment
                    varArg.setValue(value);

                    // Remove the (now redundant) constraint
                    iter.remove();

                    constraintsModified = true;
                }
            }
        }

        if (constraintsModified) {
            graphPattern.setConstraints(conjunctiveConstraints);
        }
    }
    
    /**
     * Merges the boolean constraints and the path expressions in one single
     * list. The order of the path expressions is not changed, but the boolean
     * constraints are inserted between them. The separate boolean constraints
     * are moved to the start of the list as much as possible, under the
     * condition that all variables that are used in the constraint are
     * instantiated by the path expressions that are earlier in the list. An
     * example combined list might be:
     * <tt>[(A,B,C), A != foo:bar, (B,E,F), C != F, (F,G,H)]</tt>.
     **/
    private void _orderExpressions(GraphPattern graphPattern, Set boundVars) {
        List expressions = new ArrayList();
        List conjunctiveConstraints = new LinkedList(graphPattern.getConjunctiveConstraints());

        // First evaluate any constraints that don't depend on any variables:
        _addVerifiableConstraints(conjunctiveConstraints, boundVars, expressions);

        // Then evaluate all path expressions from graphPattern
        List pathExpressions = new LinkedList(graphPattern.getPathExpressions());
        Hashtable<PathExpression,Integer> rangeCounts = new Hashtable<PathExpression, Integer>();
        while (!pathExpressions.isEmpty()) {
            PathExpression pe = _getMostSpecificPathExpression(pathExpressions, boundVars, rangeCounts);

            pathExpressions.remove(pe);
            expressions.add(pe);

            pe.getVariables(boundVars);

            _addVerifiableConstraints(conjunctiveConstraints, boundVars, expressions);
        }

        // Finally, evaluate any optional child graph pattern lists
        List optionals = new LinkedList(graphPattern.getOptionals());
        while (!optionals.isEmpty()) {
            PathExpression pe = _getMostSpecificPathExpression(optionals, boundVars, rangeCounts);

            optionals.remove(pe);
            expressions.add(pe);

            pe.getVariables(boundVars);

            _addVerifiableConstraints(conjunctiveConstraints, boundVars, expressions);
        }

        // All constraints should be verifiable when all path expressions are
        // evaluated, but add any remaining constraints anyway
        expressions.addAll(conjunctiveConstraints);

        graphPattern.setExpressions(expressions);
    }

    /**
     * Gets the most specific path expression from <tt>pathExpressions</tt>
     * given that the variables in <tt>boundVars</tt> have already been assigned
     * values. The most specific path expressions is the path expression with
     * the least number of unbound variables.
     **/
    private PathExpression _getMostSpecificPathExpression(
            List pathExpressions, Set boundVars, Hashtable<PathExpression,Integer> rangeCounts)
    {
        int minVars = Integer.MAX_VALUE;
        int minRangeCount = Integer.MAX_VALUE;
        PathExpression result = null;
        ArrayList vars = new ArrayList();

        for (int i = 0; i < pathExpressions.size(); i++) {
            PathExpression pe = (PathExpression)pathExpressions.get(i);

            /*
             * The #of results for this PathException or -1 if not a
             * TriplePattern.
             * 
             * @todo if zero (0), then at least order it first.
             */
            int rangeCount = getRangeCount(pe,rangeCounts);
            
            // Get the variables that are used in this path expression
            vars.clear();
            pe.getVariables(vars);

            // Count unbound variables
            int varCount = 0;
            for (int j = 0; j < vars.size(); j++) {
                Var var = (Var)vars.get(j);

                if (!var.hasValue() && !boundVars.contains(var)) {
                    varCount++;
                }
            }

            // A bit of hack to make sure directType-, directSubClassOf- and
            // directSubPropertyOf patterns get sorted to the back because these
            // are potentially more expensive to evaluate.
            if (pe instanceof DirectType ||
                pe instanceof DirectSubClassOf ||
                pe instanceof DirectSubPropertyOf)
            {
                varCount++;
            }

            if (rangeCount != -1) {
                // rangeCount is known for this path expression.
                if (rangeCount < minRangeCount) {
                    // More specific path expression found
                    minRangeCount = rangeCount;
                    result = pe;
                }
            } else {
                // rangeCount is NOT known.
                if (varCount < minVars) {
                    // More specific path expression found
                    minVars = varCount;
                    result = pe;
                }
            }
        }

        return result;
    }

    /**
     * Estimate the #of results for a triple pattern.
     * 
     * @param pe
     * @param rangeCounts
     * 
     * @return The estimated range count or <code>-1</code> if this is not a
     *         {@link TriplePattern}.
     */
    private int getRangeCount(PathExpression pe,Hashtable<PathExpression,Integer> rangeCounts) {
        
        Integer rangeCount = rangeCounts.get(pe);
        
        if(rangeCount!=null) {
            
            return rangeCount;
            
        }
        
        if(pe instanceof TriplePattern) {
            
            TriplePattern tp = (TriplePattern)pe;
            
            rangeCount = rangeCount(tp);

            System.err.println("rangeCount: " + rangeCount + " : " + pe);
            
            rangeCounts.put(pe,rangeCount);
            
            return rangeCount.intValue();
            
        } else {
            
            return -1;
            
        }
        
    }

    private int rangeCount(TriplePattern tp) {
        
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
        
        return database.rangeCount(_s, _p, _o);

    }
    
    /**
     * Adds all verifiable constraints (constraint for which every variable has
     * been bound to a specific value) from <tt>conjunctiveConstraints</tt> to
     * <tt>expressions</tt>.
     *
     * @param conjunctiveConstraints A List of BooleanExpr objects.
     * @param boundVars A Set of Var objects that have been bound.
     * @param expressions The list to add the verifiable constraints to.
     **/
    private void _addVerifiableConstraints(
        List conjunctiveConstraints, Set boundVars, List expressions)
    {
        Iterator iter = conjunctiveConstraints.iterator();

        while (iter.hasNext()) {
            BooleanExpr constraint = (BooleanExpr)iter.next();

            Set constraintVars = new HashSet();
            constraint.getVariables(constraintVars);

            if (boundVars.containsAll(constraintVars)) {
                // constraint can be verified
                expressions.add(constraint);
                iter.remove();
            }
        }
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
     */
    public void initialize(Map configParams) throws SailInitializationException {

        properties = PropertyUtil.flatCopy(PropertyUtil.convert(configParams));

        String val;

        // rdfsClosure
        {
            val = properties.getProperty(Options.RDFS_CLOSURE);

            if (val != null) {

                rdfsClosure = Boolean.parseBoolean(val);

            } else {

                // No closure by default.
                rdfsClosure = false;

            }

            log.info(Options.RDFS_CLOSURE + "=" + rdfsClosure);
        }
        
        valueFactory = new OptimizedValueFactory();

        // storeClass
        {
            
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
            
            this.database = database;
            
            this.inferenceEngine = new InferenceEngine( database );
            
        }
        
    }
    
    public void shutDown() {

        // @todo use polite shutdown() and define on ITripleStore.
        database.close();
        
    }

    /**
     * Computes the closure of the triple store for RDFS entailments.
     * <p>
     * This computes the full forward closure of the store and then commits the
     * store. Since incremental closure and truth maintenance are NOT supported,
     * you should load all your data into store using
     * {@link LocalTripleStore#loadData(File, String, org.openrdf.sesame.constants.RDFFormat, boolean, boolean)}
     * and then invoke this method to compute the RDFS entailments.
     * <p>
     * This method lies outside of the SAIL and does not rely on the SAIL
     * "transaction" mechanisms.
     */
    public void fullForwardClosure() {
        
        inferenceEngine.fullForwardClosure();
        
        database.commit();
                
    }
    
}
