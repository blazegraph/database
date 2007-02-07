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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.objndx.IEntryIterator;
import com.bigdata.objndx.NoSuccessorException;
import com.bigdata.rdf.KeyOrder;
import com.bigdata.rdf.RdfKeyBuilder;
import com.bigdata.rdf.TripleStore;
import com.bigdata.rdf.model.OptimizedValueFactory.TermIdComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._Resource;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.model.OptimizedValueFactory._ValueSortKeyComparator;
import com.bigdata.rdf.rio.MultiThreadedPresortRioLoader.ConsumerThread;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * A buffer for absorbing the output of the RIO parser.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Buffer {

    final _URI[] uris;
    final _Literal[] literals;
    final _BNode[] bnodes;
    final _Statement[] stmts;
    
    int numURIs, numLiterals, numBNodes;
    int numStmts;

    /**
     * Map used to filter out duplicate terms.  The use of this map provides
     * a ~40% performance gain.
     */
    final Map<_Value, _Value> distinctTermMap;

    /**
     * Map used to filter out duplicate statements. 
     */
    final Map<_Statement,_Statement> distinctStmtMap;
    
    protected final TripleStore store;
    
    /**
     * The bufferQueue capacity -or- <code>-1</code> if the {@link Buffer}
     * object is signaling that no more buffers will be placed onto the
     * queue by the producer and that the consumer should therefore
     * terminate.
     */
    protected final int capacity;

    /**
     * When true only distinct terms and statements are stored in the buffer.
     */
    protected final boolean distinct;
    
    boolean haveKeys = false;
    
    boolean sorted = false;
    
    /**
     * Create a buffer.
     * 
     * @param store
     *            The database into which the terma and statements will be
     *            inserted.
     * @param capacity
     *            The maximum #of Statements, URIs, Literals, or BNodes that the
     *            buffer can hold.
     * @param distinct
     *            When true only distinct terms and statements are stored in the
     *            buffer.
     */
    public Buffer(TripleStore store, int capacity, boolean distinct) {
    
        this.store = store;
        
        this.capacity = capacity;
    
        this.distinct = distinct;
        
        if( capacity == -1 ) {
            
            uris = null;
            literals = null;
            bnodes = null;
            stmts = null;
            distinctTermMap = null;
            distinctStmtMap = null;
            
            return;
            
        }
        
        uris = new _URI[ capacity ];
        
        literals = new _Literal[ capacity ];
        
        bnodes = new _BNode[ capacity ];

        stmts = new _Statement[ capacity ];

        if (distinct) {

            /*
             * initialize capacity to 3x the #of statements allowed. this is the
             * maximum #of distinct terms and would only be realized if each
             * statement used distinct values. in practice the #of distinct terms
             * will be much lower.  however, also note that the map will be resized
             * at .75 of the capacity so we want to over-estimate the maximum likely
             * capacity by at least 25% to avoid re-building the hash map.
             */
            distinctTermMap = new HashMap<_Value, _Value>(capacity * 3);

            distinctStmtMap = new HashMap<_Statement, _Statement>(capacity);
            
        } else {
            
            distinctTermMap = null;

            distinctStmtMap = null;
            
        }
        
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
        
        final long begin = System.currentTimeMillis();
        
        store.generateSortKeys(keyBuilder, uris, numURIs);
        
        store.generateSortKeys(keyBuilder, literals, numLiterals);
        
        store.generateSortKeys(keyBuilder, bnodes, numBNodes);
        
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
        assert uris != null;
        assert literals != null;
        assert bnodes != null;
        
        final int numTerms = numURIs + numLiterals + numBNodes;

        final long begin = System.currentTimeMillis();
        
        if (numURIs > 0)
            Arrays.sort(uris, 0, numURIs, _ValueSortKeyComparator.INSTANCE);

        if (numLiterals > 0)
            Arrays.sort(literals, 0, numLiterals, _ValueSortKeyComparator.INSTANCE);

        if( numBNodes>0)
            Arrays.sort(bnodes, 0, numBNodes, _ValueSortKeyComparator.INSTANCE);

        sorted = true;

        System.err.println("sorted "+numTerms+" terms: "
                + (System.currentTimeMillis() - begin) + "ms");
     
    }
    
    /**
     * Sorts the terms by their pre-assigned termIds.
     */
    public void sortTermsByTermIds() {
        
        final long begin = System.currentTimeMillis();
        
        if (numURIs > 0)
            Arrays.sort(uris, 0, numURIs, TermIdComparator.INSTANCE);

        if (numLiterals > 0)
            Arrays.sort(literals, 0, numLiterals, TermIdComparator.INSTANCE);

        if( numBNodes>0)
            Arrays.sort(bnodes, 0, numBNodes, TermIdComparator.INSTANCE);

        sorted = true;

        System.err.println("sorted terms by ids: "
                + (System.currentTimeMillis() - begin) + "ms");
     
    }
    
    /**
     * Batch insert buffered data (terms and statements) into the store.
     */
    public void insert() {

        /*
         * insert terms.
         */
        store.insertTerms(uris, numURIs, haveKeys, sorted);

        store.insertTerms(literals, numLiterals, haveKeys, sorted);

        store.insertTerms(bnodes, numBNodes, haveKeys, sorted);

        /*
         * insert statements.
         */
        store.addStatements(stmts, numStmts);

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
        
        if(numURIs+3>capacity) return true;

        if(numLiterals+3>capacity) return true;
        
        if(numBNodes+3>capacity) return true;
        
        if(numStmts+1>capacity) return true;
        
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

        assert distinct == true;
        
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
     * Adds the values and the statement into the bufferQueue.
     * <p>
     * Note: the RIO parser can report the same value on multiple invocations of
     * this handler method. I suspect that this issue goes back to the syntax of
     * RDF/XML which allows on to make multiple statements about the same
     * subject without repeating the lexical item for the subject. In a similar
     * fashion, some of the other RDF interchange languages may permit one to
     * make multiple statements about the same subject and predicate (I believe
     * that N3 does this). This is an issue for how we filter the parsed values
     * for duplicates.
     * 
     * @param s
     * @param p
     * @param o
     * 
     * @exception IndexOutOfBoundsException
     *                if the bufferQueue overflows.
     * 
     * @see #nearCapacity()
     */
    public void handleStatement( Resource s, URI p, Value o ) {

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

            if (s instanceof _URI) {

                uris[numURIs++] = (_URI) s;

            } else {

                bnodes[numBNodes++] = (_BNode) s;

            }
            
        }

        if (!duplicateP) {
            
            uris[numURIs++] = (_URI) p;
            
        }

        if (!duplicateO) {
            
            if (o instanceof _URI) {

                uris[numURIs++] = (_URI) o;

            } else if (o instanceof _BNode) {

                bnodes[numBNodes++] = (_BNode) o;

            } else {

                literals[numLiterals++] = (_Literal) o;

            }
            
        }

        _Statement stmt = new _Statement((_Resource) s, (_URI) p, (_Value) o);
        
        if(distinct) {

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
     * Visits URIs, Literals, and BNodes marked as {@link _Value#known unknown}
     * in their current sorted order.
     */
    public static class UnknownTermIterator implements IEntryIterator {
        
        private final IStriterator src;
        private _Value current = null;
        
        public UnknownTermIterator(Buffer buffer) {

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

        public TermIterator(Buffer buffer) {
            
            src = new Striterator(EmptyIterator.DEFAULT)
                .append(new TermClassIterator(buffer.uris, buffer.numURIs))
                .append(new TermClassIterator(buffer.literals, buffer.numLiterals))
                .append(new TermClassIterator(buffer.bnodes,buffer.numBNodes))
                ;
            
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
     * Visits the term identifier for all URIs, Literals, and BNodes in their current order.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TermIdIterator implements IEntryIterator {

        private final IStriterator src;
        private final TripleStore store;
        
        private int nvisited = 0;
        private _Value current = null;

        public TermIdIterator(Buffer buffer) {
            
            this.store = buffer.store;
            
            src = new Striterator(EmptyIterator.DEFAULT)
                .append(new TermClassIterator(buffer.uris, buffer.numURIs))
                .append(new TermClassIterator(buffer.literals, buffer.numLiterals))
                .append(new TermClassIterator(buffer.bnodes,buffer.numBNodes))
                ;
            
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

        /**
         * FIXME this assigns a term identifier if one is not found which breaks
         * down the iterator abstraction.
         */
        public Object next() {
            
            try {

                current = (_Value) src.next();
                
                nvisited++;
                
            } catch(NoSuchElementException ex) {
                
                System.err.println("*** Iterator exhausted after: "+nvisited+" elements");
                
                throw ex;
                
            }
            
            if(current.termId == 0L) {
                
                current.termId = store.getTermIdIndex().counter.nextId();
                
            }
            
            return current.termId;
            
        }

        public void remove() {
            
            throw new UnsupportedOperationException();
            
        }
        
    }

    /**
     * Visits all terms in some term class in their current order.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class TermClassIterator implements IEntryIterator {

        private int lastVisited = -1;
        private int index = 0;

        private final _Value[] terms;
        private final int nterms;
        
        public TermClassIterator(_Value[] terms,int nterms) {
            
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

        public StatementIterator(KeyOrder keyOrder,Buffer buffer) {
            
            this(keyOrder,buffer.store.keyBuilder,buffer.stmts,buffer.numStmts);
            
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
        
        public UnknownStatementIterator(KeyOrder keyOrder, Buffer buffer) {
        
            this.keyOrder = keyOrder;
            
            this.keyBuilder = buffer.store.keyBuilder;
            
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
