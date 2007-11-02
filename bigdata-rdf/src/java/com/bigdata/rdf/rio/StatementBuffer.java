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
 * Created on Jan 27, 2007
 */

package com.bigdata.rdf.rio;

import java.beans.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.NoSuccessorException;
import com.bigdata.rdf.model.OptimizedValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory.TermIdComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Resource;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.model.OptimizedValueFactory._ValueSortKeyComparator;
import com.bigdata.rdf.rio.MultiThreadedPresortRioLoader.ConsumerThread;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * A write buffer for absorbing the output of the RIO parser or other
 * {@link Statement} source and writing that output onto an
 * {@link AbstractTripleStore} using the batch API.
 * <p>
 * Note: there is a LOT of {@link Value} duplication in parsed RDF and we get a
 * significant reward for reducing {@link Value}s to only the distinct
 * {@link Value}s during processing. On the other hand, there is little
 * {@link Statement} duplication. Hence we pay an unnecessary overhead if we try
 * to make the statements distinct in the buffer.
 * <p>
 * Note: This also provides an explanation for why the {@link SPOBuffer} does
 * not do better when "distinct" is turned on - the "Value" objects in that case
 * are only represented by long integers and duplication in their values does
 * not impose a burden on either the heap or the index writers. In contrast, the
 * duplication of {@link Value}s in the {@link StatementBuffer} imposes a
 * burden on both the heap and the index writers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StatementBuffer implements IStatementBuffer {

    /**
     * Buffer for parsed RDF {@link Value}s.
     */
    protected final _Value[] values;
    
    /**
     * Buffer for parsed RDF {@link Statement}s.
     */
    protected final _Statement[] stmts;

    /**
     * #of valid entries in {@link #values}.
     */
    protected int numValues;
    
    /**
     * #of valid entries in {@link #stmts}.
     */
    protected int numStmts;

    /**
     * @todo consider tossing out these counters - they only add complexity to
     * the code in {@link #handleStatement(Resource, URI, Value, StatementEnum)}.
     */
    protected int numURIs, numLiterals, numBNodes;
    
    /**
     * Map used to filter out duplicate terms.  The use of this map provides
     * a ~40% performance gain.
     */
    final private Map<_Value, _Value> distinctTermMap;

    /**
     * Map used to filter out duplicate statements.
     * 
     * @deprecated this is not efficient and will be discarded.
     */
    final Map<_Statement,_Statement> distinctStmtMap;
    
    private final AbstractTripleStore statementStore;

    /**
     * The optional store into which statements will be inserted when non-<code>null</code>.
     */
    public final AbstractTripleStore getStatementStore() {
        
        return statementStore;
        
    }
    
    private final AbstractTripleStore database;
    
    /**
     * The database that will be used to resolve terms.  When {@link #getStatementStore()}
     * is <code>null</code>, statements will be written into this store as well.
     */
    public final AbstractTripleStore getDatabase() {
        
        return database;
        
    }
    
    /**
     * The bufferQueue capacity -or- <code>-1</code> if the {@link StatementBuffer}
     * object is signaling that no more buffers will be placed onto the
     * queue by the producer and that the consumer should therefore
     * terminate.
     */
    protected final int capacity;

    /**
     * When true only distinct terms are stored in the buffer (this is always
     * true since this condition always outperforms the alternative).
     */
    protected final boolean distinct = true;
    
    boolean haveKeys = false;
    
    boolean sorted = false;
    
    public boolean isEmpty() {
        
        return numStmts == 0;
        
    }
    
    public int size() {
        
        return numStmts;
        
    }

    /**
     * Create a buffer that maintains only the distinct {@link Value}s and {@link Statement}s.
     * 
     * @param database
     *            The database into which the terma and statements will be
     *            inserted.
     * @param capacity
     *            The maximum #of Statements, URIs, Literals, or BNodes that the
     *            buffer can hold. The minimum capacity is three (3) since that
     *            corresponds to a single triple where all terms are URIs.
     */
    public StatementBuffer(AbstractTripleStore database, int capacity) {
        
        this(null,database,capacity);
        
    }

    /**
     * Create a buffer.
     * 
     * @param statementStore
     *            The store into which the statements will be inserted
     *            (optional). When <code>null</code>, both statments and
     *            terms will be inserted into the <i>database</i>. This
     *            optional argument provides the ability to load statements into
     *            a temporary store while the terms are resolved against the
     *            main database. This facility is used during incremental
     *            load+close operations.
     * @param database
     *            The database. When <i>statementStore</i> is <code>null</code>,
     *            both terms and statements will be inserted into the
     *            <i>database</i>.
     * @param capacity
     *            The maximum #of Statements, URIs, Literals, or BNodes that the
     *            buffer can hold. The minimum capacity is three (3) since that
     *            corresponds to a single triple where all terms are URIs.
     */
    public StatementBuffer(AbstractTripleStore statementStore, AbstractTripleStore database, int capacity) {
            
        if (database == null)
            throw new IllegalArgumentException();

        if (capacity < 1)
            throw new IllegalArgumentException();

        this.statementStore = statementStore; // MAY be null.
        
        this.database = database;
        
        this.capacity = capacity;
        
        if( capacity == -1 ) {
            
            values = null;

            stmts = null;
            
            distinctTermMap = null;
            
            distinctStmtMap = null;
            
            return;
            
        }
        
        values = new _Value[capacity * IRawTripleStore.N];
        
        stmts = new _Statement[capacity];

        if (distinct) {

            /*
             * initialize capacity to N times the #of statements allowed. this
             * is the maximum #of distinct terms and would only be realized if
             * each statement used distinct values. in practice the #of distinct
             * terms will be much lower. however, also note that the map will be
             * resized at .75 of the capacity so we want to over-estimate the
             * maximum likely capacity by at least 25% to avoid re-building the
             * hash map.
             */
            
            distinctTermMap = new HashMap<_Value, _Value>(capacity * IRawTripleStore.N);

            distinctStmtMap = null;
//            distinctStmtMap = new HashMap<_Statement, _Statement>(capacity);
            
        } else {
            
            distinctTermMap = null;

            distinctStmtMap = null;
            
        }
        
    }
    
    /**
     * Resets the state of the buffer (any pending writes are discarded).
     */
    public void clear() {
        
        for(int i=0; i<numValues; i++) {
            
            values[i] = null;
            
        }

        for(int i=0; i<numStmts; i++) {
            
            stmts[i] = null;
            
        }
        
        numURIs = numLiterals = numBNodes = numStmts = numValues = 0;
        
        if(distinctTermMap!=null) {
            
            distinctTermMap.clear();
            
        }
        
        if(distinctStmtMap!=null) {
            
            distinctStmtMap.clear();
            
        }
    
        haveKeys = false;
        
        sorted = false;

    }
    
    /**
     * Generates the sort keys for the terms in the buffer and sets the
     * {@link #haveKeys} flag.
     * 
     * @param keyBuilder
     *            When one thread is used to parse, buffer, and generate
     *            keys and the other is used to insert the data into the
     *            store a <em>distinct instance</em> of the key builder
     *            object must be used by the main thread in order to avoid
     *            concurrent overwrites of the key buffer by the
     *            {@link ConsumerThread}, which is still responsible for
     *            generating statement keys.<br>
     *            Note further that the key builder MUST be provisioned in
     *            the same manner with respect to unicode support in order
     *            for keys to be comparable!
     */
    public void generateTermSortKeys(RdfKeyBuilder keyBuilder) {

        assert !haveKeys;

        final int numTerms = numURIs + numLiterals + numBNodes;
        
        assert numTerms == numValues;
        
        final long begin = System.currentTimeMillis();

        database.generateSortKeys(keyBuilder, values, numValues);

        haveKeys = true;
        
        System.err.println("generated "+numTerms+" term sort keys: "
                + (System.currentTimeMillis() - begin) + "ms");
     
    }

    /**
     * Sorts the terms by their pre-assigned sort keys and sets the
     * {@link #sorted} flag.
     * 
     * @see #generateSortKeys(), which must be invoked as a pre-condition.
     */
    public void sortTermsBySortKeys() {
        
//        assert haveKeys;
        assert ! sorted;
        assert values != null;
        
        final int numTerms = numURIs + numLiterals + numBNodes;
        
        assert numTerms == numValues;

        final long begin = System.currentTimeMillis();
        
        if (numValues > 0)
            Arrays.sort(values, 0, numValues, _ValueSortKeyComparator.INSTANCE);
        
        sorted = true;

        System.err.println("sorted "+numTerms+" terms: "
                + (System.currentTimeMillis() - begin) + "ms");
     
    }
    
    /**
     * Sorts the terms by their pre-assigned termIds.
     */
    public void sortTermsByTermIds() {
        
        final long begin = System.currentTimeMillis();

        if( numValues>0)
            Arrays.sort(values, 0, numValues, TermIdComparator.INSTANCE);

        sorted = true;

        System.err.println("sorted terms by ids: "
                + (System.currentTimeMillis() - begin) + "ms");
     
    }
    
    /**
     * Batch insert buffered data (terms and statements) into the store.
     */
    public void flush() {

        /*
         * insert terms (batch operation).
         */
        
        database.insertTerms(values, numValues, haveKeys, sorted);

        /*
         * insert statements (batch operation).
         */
        
        addStatements(stmts, numStmts);
        
        /* 
         * reset the state of the buffer.
         */

        clear();

    }
        
    /**
     * Adds the statements to each index (batch api, NO truth maintenance).
     * <p>
     * Pre-conditions: The term identifiers for each {@link _Statement} are
     * defined.
     * 
     * @param stmts
     *            An array of statements
     * 
     * @see #insertTerms(_Value[], int, boolean, boolean)
     * 
     * @return The #of statements written on the database.
     */
    final protected int addStatements(_Statement[] stmts, int numStmts) {
        
        SPO[] tmp = new SPO[numStmts];
        
        for(int i=0; i<tmp.length; i++) {
            
            _Statement stmt = stmts[i];
            
            tmp[i] = new SPO(stmt.s.termId, stmt.p.termId, stmt.o.termId,
                    stmt.type);
            
        }
        
        if(statementStore!=null) {

            // Writing statements on the secondary store.
            
            return statementStore.addStatements(tmp, numStmts);
            
        } else {
            
            // Write statements on the primary database.
            
            return database.addStatements(tmp, numStmts);
            
        }

    }

    /**
     * Returns true if the bufferQueue has less than three slots remaining for
     * any of the value arrays (URIs, Literals, or BNodes) or if there are
     * no slots remaining in the statements array. Under those conditions
     * adding another statement to the bufferQueue could cause an overflow.
     * 
     * @return True if the bufferQueue might overflow if another statement were
     *         added.
     */
    public boolean nearCapacity() {

        if(numStmts+1>capacity) return true;

        if(numValues+IRawTripleStore.N>values.length) return true;

        return false;
        
    }
    
    /**
     * Uniquify a term.
     * 
     * @param term
     *            A term.
     *            
     * @return Either the term or the pre-existing term in the buffer with the
     *         same data.
     */
    protected _Value getDistinctTerm(_Value term) {

        assert distinct == true;
        
        _Value existingTerm = distinctTermMap.get(term);
        
        if(existingTerm != null) {
            
            // return the pre-existing term.
            
            return existingTerm;
            
        } else {
            
            // put the new term in the map.
            
            if(distinctTermMap.put(term, term)!=null) {
                
                throw new AssertionError();
                
            }
            
            // return the new term.
            
            return term;
            
        }
        
    }
    
    /**
     * Uniquify a statement.
     * 
     * @param stmt
     * 
     * @return Either the statement or the pre-existing statement in the buffer
     *         with the same data.
     */
    protected _Statement getDistinctStatement(_Statement stmt) {

        assert distinctStmtMap != null;
        
        _Statement existingStmt = distinctStmtMap.get(stmt);
        
        if(existingStmt!= null) {
            
            // return the pre-existing statement.
            
            return existingStmt;
            
        } else {

            // put the new statement in the map.
            
            if(distinctStmtMap.put(stmt, stmt)!=null) {
                
                throw new AssertionError();
                
            }

            // return the new statement.
            return stmt;
            
        }
        
    }
    
    /**
     * Adds the values and the statement into the buffer.
     * 
     * @param s
     * @param p
     * @param o
     * @param type
     * 
     * @exception IndexOutOfBoundsException
     *                if the bufferQueue overflows.
     * 
     * @see #nearCapacity()
     */
    public void handleStatement( Resource s, URI p, Value o, StatementEnum type ) {

        s = (Resource) OptimizedValueFactory.INSTANCE.toNativeValue(s);
        p = (URI)      OptimizedValueFactory.INSTANCE.toNativeValue(p);
        o =            OptimizedValueFactory.INSTANCE.toNativeValue(o);
        
        boolean duplicateS = false;
        boolean duplicateP = false;
        boolean duplicateO = false;
        
        if (distinct) {
            {
                _Value tmp = getDistinctTerm((_Value) s);
//                if (tmp != s) {
                if(tmp.count>0){
                    duplicateS = true;
                }
                s = (Resource) tmp;
            }
            {
                _Value tmp = getDistinctTerm((_Value) p);
//                if (tmp != p) {
                if(tmp.count>0) {
                    duplicateP = true;
                }
                p = (URI) tmp;
            }
            {
                _Value tmp = getDistinctTerm((_Value) o);
//                if (tmp != o) {
                if(tmp.count>0) {
                    duplicateO = true;
                }
                o = (Value) tmp;
            }
        }

        if (!duplicateS) {

            values[numValues++] = (_Value)s;
            
            if (s instanceof _URI) {

                numURIs++;

            } else {

                numBNodes++;

            }
            
        }

        if (!duplicateP) {
            
            values[numValues++] = (_Value)p;

            numURIs++;
            
        }

        if (!duplicateO) {

            values[numValues++] = (_Value)o;

            if (o instanceof _URI) {

                numURIs++;

            } else if (o instanceof _BNode) {

                numBNodes++;

            } else {

                numLiterals++;

            }
            
        }

        _Statement stmt = new _Statement((_Resource) s, (_URI) p, (_Value) o,
                type);
        
        if (distinctStmtMap != null) {

            _Statement tmp = getDistinctStatement(stmt);

            if(tmp.count++ == 0){
           
                stmts[numStmts++] = tmp;
              
                // increment usage counts on terms IFF the statement is added to
                // the buffer.
                ((_Value)s).count++;
                ((_Value)p).count++;
                ((_Value)o).count++;

            }
          
        } else {

            stmts[numStmts++] = stmt;

            // increment usage counts on terms IFF the statement is added to
            // the buffer.
            ((_Value)s).count++;
            ((_Value)p).count++;
            ((_Value)o).count++;

        }

    }

    /**
     * Add an "explicit" statement to the buffer (flushes on overflow).
     * 
     * @param s
     * @param p
     * @param o
     */
    public void add(Resource s, URI p, Value o) {
        
        add(s, p, o, StatementEnum.Explicit);
        
    }
    
    /**
     * Add a statement to the buffer (flushes on overflow).
     * 
     * @param s
     * @param p
     * @param o
     * @param type
     */
    public void add(Resource s, URI p, Value o, StatementEnum type) {
        
        if (nearCapacity()) {

            // bulk insert the buffered data into the store.
            flush();

            // clear the buffer.
            clear();
            
        }
        
        // add to the buffer.
        handleStatement(s, p, o, type);

    }
    
    /**
     * Visits URIs, Literals, and BNodes marked as {@link _Value#known unknown}
     * in their current sorted order.
     * 
     * @deprecated Only used by the unit tests.
     */
    public static class UnknownTermIterator implements IEntryIterator {
        
        private final IStriterator src;
        private _Value current = null;
        
        public UnknownTermIterator(StatementBuffer buffer) {

            src = new Striterator(new TermIterator(buffer)).addFilter(new Filter(){

                private static final long serialVersionUID = 1L;

                protected boolean isValid(Object arg0) {
                    
                    _Value term = (_Value)arg0;
                    
                    return 
//                    term.duplicate == false &&
                    term.known == false;
                }
                
            }
            );
            
        }

        public byte[] getKey() {
            
            return current.key;
            
        }

        public Object getValue() {
            
            return current;
            
        }

        public boolean hasNext() {
            
            return src.hasNext();
            
        }

        public Object next() {
            
            current = (_Value) src.next();
            
            return current;
            
        }

        public void remove() {
            
            throw new UnsupportedOperationException();
            
        }
        
    }
    
    /**
     * Visits all URIs, Literals, and BNodes in their current order.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TermIterator implements IEntryIterator {

        private final IStriterator src;
        
        private _Value current = null;

        public TermIterator(StatementBuffer buffer) {

            src = new Striterator(new TermArrayIterator(buffer.values,
                    buffer.numValues));
            
        }

        public byte[] getKey() {

            if (current == null)
                throw new IllegalStateException();

            return current.key;
            
        }

        public Object getValue() {

            if (current == null)
                throw new IllegalStateException();

            return current;
            
        }

        public boolean hasNext() {
            
            return src.hasNext();
            
        }

        public Object next() {
            
            current = (_Value) src.next();
            
            return current;
            
        }

        public void remove() {
            
            throw new UnsupportedOperationException();
            
        }
        
    }

    /**
     * Visits the term identifier for all URIs, Literals, and BNodes in their
     * current order.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TermIdIterator implements IEntryIterator {

        private final IStriterator src;
        
        private int nvisited = 0;
        private _Value current = null;

        public TermIdIterator(StatementBuffer buffer) {
            
            src = new Striterator(new TermArrayIterator(buffer.values,
                    buffer.numValues));
            
        }

        public byte[] getKey() {

            if (current == null)
                throw new IllegalStateException();

            return current.key;
            
        }

        public Object getValue() {

            if (current == null)
                throw new IllegalStateException();

            return current.termId;
            
        }

        public boolean hasNext() {
            
            return src.hasNext();
            
        }

        public Object next() {
            
            try {

                current = (_Value) src.next();
                
                nvisited++;
                
            } catch(NoSuchElementException ex) {
                
                System.err.println("*** Iterator exhausted after: "+nvisited+" elements");
                
                throw ex;
                
            }
            
            assert current.termId != IRawTripleStore.NULL;
            
            return current.termId;
            
        }

        public void remove() {
            
            throw new UnsupportedOperationException();
            
        }
        
    }

    /**
     * Visits all terms in their current order.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class TermArrayIterator implements IEntryIterator {

        private int lastVisited = -1;
        private int index = 0;

        private final _Value[] terms;
        private final int nterms;
        
        public TermArrayIterator(_Value[] terms,int nterms) {
            
            this.terms = terms;
            
            this.nterms = nterms;

        }

        public byte[] getKey() {
            
            if( lastVisited == -1 ) {
                
                throw new IllegalStateException();
                
            }
            
            return terms[lastVisited].key;
            
        }

        public Object getValue() {
            
            if( lastVisited == -1 ) {
                
                throw new IllegalStateException();
                
            }
            
            return terms[lastVisited];
            
        }

        public boolean hasNext() {
            
            return index < nterms;
            
        }

        public Object next() {
            
            if (index >= nterms) {
                
                throw new NoSuccessorException();
                
            }

            lastVisited = index++;

            return terms[lastVisited];
            
        }

        public void remove() {
            
            throw new UnsupportedOperationException();
            
        }
        
    }

    /**
     * Visits all statements in their current order.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class StatementIterator implements IEntryIterator {

        private final KeyOrder keyOrder;
        private final RdfKeyBuilder keyBuilder;
        private final _Statement[] stmts;
        private final int numStmts;
        
        private int lastVisited = -1;
        private int index = 0;

        public StatementIterator(KeyOrder keyOrder,StatementBuffer buffer) {
            
            this(keyOrder, buffer.database.getKeyBuilder(), buffer.stmts,
                    buffer.numStmts);
            
        }
        
        public StatementIterator(KeyOrder keyOrder,RdfKeyBuilder keyBuilder,_Statement[] stmts,int numStmts) {
            
            this.keyOrder = keyOrder;
            
            this.keyBuilder = keyBuilder;
            
            this.stmts = stmts;
            
            this.numStmts = numStmts;
            
        }

        public byte[] getKey() {

            if (lastVisited == -1) {

                throw new IllegalStateException();

            }

            _Statement stmt = stmts[lastVisited];

            switch (keyOrder) {
            case SPO:
                return keyBuilder.statement2Key(stmt.s.termId,
                        stmt.p.termId, stmt.o.termId);
            case POS:
                return keyBuilder.statement2Key(stmt.p.termId,
                        stmt.o.termId, stmt.s.termId);
            case OSP:
                return keyBuilder.statement2Key(stmt.o.termId,
                        stmt.s.termId, stmt.p.termId);
            default:
                throw new UnsupportedOperationException();
            }
            
        }

        public Object getValue() {
            if( lastVisited == -1 ) {
                
                throw new IllegalStateException();
                
            }
            return stmts[lastVisited];
        }

        public boolean hasNext() {
            
            return index < numStmts;
            
        }

        public Object next() {
            
            if(!hasNext()) {
                
                throw new NoSuccessorException();
                
            }

            lastVisited = index++;

            return stmts[lastVisited];
            
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
        
    }

    /**
     * Visits statements in the buffer in their current sorted order that are
     * not found in the statement index as indicated by the
     * {@link _Statement#known} flag.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class UnknownStatementIterator implements IEntryIterator {

        private final KeyOrder keyOrder;
        private final RdfKeyBuilder keyBuilder;
        private final IStriterator src;
        private _Statement current;
        
        public UnknownStatementIterator(KeyOrder keyOrder, StatementBuffer buffer) {
        
            this.keyOrder = keyOrder;
            
            this.keyBuilder = buffer.database.getKeyBuilder();
            
            src = new Striterator(new StatementIterator(keyOrder,buffer))
                    .addFilter(new Filter() {

                        private static final long serialVersionUID = 1L;

                        protected boolean isValid(Object arg0) {

                            _Statement stmt = (_Statement) arg0;

                            return !stmt.known;

                        }

                    });
            
        }
        
        public byte[] getKey() {

            if (current == null) {

                throw new IllegalStateException();

            }

            _Statement stmt = current;

            switch (keyOrder) {
            case SPO:
                return keyBuilder.statement2Key(stmt.s.termId, stmt.p.termId,
                        stmt.o.termId);
            case POS:
                return keyBuilder.statement2Key(stmt.p.termId, stmt.o.termId,
                        stmt.s.termId);
            case OSP:
                return keyBuilder.statement2Key(stmt.o.termId, stmt.s.termId,
                        stmt.p.termId);
            default:
                throw new UnsupportedOperationException();
            }

        }

        public Object getValue() {

            if (current == null) {

                throw new IllegalStateException();

            }

            return current;
            
        }

        public boolean hasNext() {
            
            return src.hasNext();
            
        }

        public Object next() {

            current = (_Statement) src.next();
            
            return current;
            
        }

        public void remove() {
            
            throw new UnsupportedOperationException();
            
        }
        
    }

}
