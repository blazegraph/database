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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.BytesUtil;
import com.bigdata.objndx.DefaultEvictionListener;
import com.bigdata.objndx.IEntryIterator;
import com.bigdata.objndx.IndexSegment;
import com.bigdata.objndx.IndexSegmentBuilder;
import com.bigdata.objndx.IndexSegmentFileStore;
import com.bigdata.objndx.NoSuccessorException;
import com.bigdata.objndx.PO;
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
class Buffer {

    _URI[] uris;
    _Literal[] literals;
    _BNode[] bnodes;
    _Statement[] stmts;
    
    int numURIs, numLiterals, numBNodes;
    int numStmts;

    /**
     * #of duplicate URIs, literals and bnodes as determined by
     * {@link #filterDuplicateTerms()}.
     */
    int numDupURIs = 0, numDupLiterals = 0, numDupBNodes = 0;

    /**
     * #of non-duplicate URIs, literals, and bnodes that have been determined to
     * already be in the terms index.
     */
    int numKnownURIs = 0, numKnownLiterals = 0, numKnownBNodes = 0;

    /**
     * #of duplicate statements.
     */
    int numDupStmts = 0;

    /**
     * #of already known statements.
     */
    int numKnownStmts = 0;
    
//  /**
//  * When true, each term in a type specific buffer will be placed into a
//  * cannonicalizing map so that we never have more than once instance for
//  * the same term in a buffer at a time.  When false those transient term
//  * maps are not used.
//  * 
//  * Note: this does not show much of an effect.
//  */
// final boolean useTermMaps = false;

    /*
     * @todo The use of these caches does not appear to improve performance.
     */
//    /**
//     * Hash of the string of the URI to the URI makes URIs unique within a
//     * bufferQueue full of URIs. 
//     */
//    Map<String, _URI> uriMap;
//    /**
//     * FIXME this does not properly handle literals unless the source is a
//     * Literal with optional language code and datatype uri fields. It will
//     * falsely conflate a plain literal with a language code literal having
//     * the same text.
//     */
//    Map<String, _Literal> literalMap;
//    /**
//     * @todo bnodes are always distinct, right?  Or at least often enough
//     * that we don't want to do this.  Also, the bnode id can be encoded 
//     * using a non-unicode conversion since we are generating them ourselves
//     * for the most part.  what makes most sense is probably to pre-convert
//     * a String ID given for a bnode to a byte[] and cache only such bnodes
//     * in this map and all other bnodes should be assigned a byte[] key 
//     * directly instead of an id, e.g., by an efficient conversion of a UUID
//     * to a byte[].
//     */
//    Map<String, _BNode> bnodeMap;

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

        if (distinct)
            throw new UnsupportedOperationException(
                    "distinct is not supported.");
        
        if( capacity == -1 ) return;
        
        uris = new _URI[ capacity ];
        
        literals = new _Literal[ capacity ];
        
        bnodes = new _BNode[ capacity ];

//        if (useTermMaps) {
//
//            uriMap = new HashMap<String, _URI>(capacity);
//            
//            literalMap = new HashMap<String, _Literal>(capacity);
//            
//            bnodeMap = new HashMap<String, _BNode>(capacity);
//            
//        }

        stmts = new _Statement[ capacity ];
        
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
    public void generateSortKeys(RdfKeyBuilder keyBuilder) {

        assert !haveKeys;

        final long begin = System.currentTimeMillis();
        
        store.generateSortKeys(keyBuilder, uris, numURIs);
        
        store.generateSortKeys(keyBuilder, literals, numLiterals);
        
        store.generateSortKeys(keyBuilder, bnodes, numBNodes);
        
        haveKeys = true;
        
        System.err.println("generated term sort keys: "
                + (System.currentTimeMillis() - begin) + "ms");
     
    }

    /**
     * Sorts the terms by their pre-assigned sort keys and sets the
     * {@link #sorted} flag.
     * 
     * @see #generateSortKeys(), which must be invoked as a pre-condition.
     */
    public void sortTerms() {
        
        assert haveKeys;
        assert ! sorted;
        assert uris != null;
        assert literals != null;
        assert bnodes != null;
        
        final long begin = System.currentTimeMillis();
        
        if (numURIs > 0)
            Arrays.sort(uris, 0, numURIs, _ValueSortKeyComparator.INSTANCE);

        if (numLiterals > 0)
            Arrays.sort(literals, 0, numLiterals, _ValueSortKeyComparator.INSTANCE);

        if( numBNodes>0)
            Arrays.sort(bnodes, 0, numBNodes, _ValueSortKeyComparator.INSTANCE);

        sorted = true;

        System.err.println("sorted terms: "
                + (System.currentTimeMillis() - begin) + "ms");
     
    }
    
    /**
     * Sorts the terms by their pre-assigned termIds.
     */
    public void sortTermIds() {
        
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
     * Scans the sorted term buffers and flags terms that are duplicates of the
     * immediately proceeding term in the buffer by setting
     * {@link _Value#duplicate} to <code>true</code>.  The flagged duplicates
     * are NOT removed from the buffers since that would require a compacting
     * operation.
     */
    public void filterDuplicateTerms() {
        
        assert haveKeys;
        assert sorted;
        assert uris != null;
        assert literals != null;
        assert bnodes != null;

        final long begin = System.currentTimeMillis();
        
        numDupURIs = filterDuplicateTerms(uris, numURIs);
        
        numDupLiterals = filterDuplicateTerms(uris, numLiterals);
        
        numDupBNodes = filterDuplicateTerms(uris, numBNodes);

        int numDuplicates = numDupURIs + numDupLiterals + numDupBNodes;
        
        System.err.println("filtered "+numDuplicates+" duplicate terms: "
                + (System.currentTimeMillis() - begin) + "ms");
        
    }

    /**
     * Flag duplicate terms within the <i>terms</i> array.
     * 
     * @param terms
     *            The terms, which MUST be sorted on {@link _Value#key}.
     * @param numTerms
     *            The #of terms in that array.
     *            
     * @return The #of duplicate terms in that array.
     */
    private int filterDuplicateTerms(_Value[] terms, int numTerms) {

        int numDuplicates = 0;
        
        if(numTerms>0){

            byte[] lastKey = terms[0].key; 
            
            for (int i = 1; i < numTerms; i++) {

                byte[] thisKey = terms[i].key;
                
                if( BytesUtil.bytesEqual(lastKey, thisKey) ) {
                    
                    terms[i].duplicate = true;
                    
                    numDuplicates++;
                    
                } else {
                    
                    lastKey = thisKey;
                    
                }
                
            }
            
        }

        return numDuplicates;
        
    }

    /**
     * Mark duplicate statements by setting {@link _Statement#duplicate}.
     * <p>
     * Pre-condition - the statements must be sorted by one of the access paths
     * (SPO, POS, OSP). It does not matter which sort order is used, but
     * duplicate detection requires sorted data.
     * 
     * @return The #of duplicate statements flagged.
     */
    public int filterDuplicateStatements() {

        int numDuplicates = 0;
        
        if(numStmts>0){

            final long begin = System.currentTimeMillis();
            
            _Statement lastStmt = stmts[0]; 
            
            for (int i = 1; i < numStmts; i++) {

                _Statement thisStmt = stmts[i];
                
                if( lastStmt.equals(thisStmt) ) {
                    
                    thisStmt.duplicate = true;
                    
                    numDuplicates++;
                    
                } else {
                    
                    lastStmt = thisStmt;
                    
                }
                
            }
            
            System.err.println("filtered " + numDuplicates
                    + " duplicate statements: "
                    + (System.currentTimeMillis() - begin) + "ms");

        }

        this.numDupStmts = numDuplicates;
        
        return numDuplicates;

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
     * Adds the values and the statement into the bufferQueue.
     * 
     * @param s
     * @param p
     * @param o
     * 
     * @exception IndexOutOfBoundsException if the bufferQueue overflows.
     * 
     * @see #nearCapacity()
     */
    public void handleStatement( Resource s, URI p, Value o ) {

        if ( s instanceof _URI ) {
            
            uris[numURIs++] = (_URI) s;
            
        } else {
            
            bnodes[numBNodes++] = (_BNode) s;
            
        }
        
        uris[numURIs++] = (_URI) p;
        
        if ( o instanceof _URI ) {
            
            uris[numURIs++] = (_URI) o;
            
        } else if ( o instanceof _BNode ) {
            
            bnodes[numBNodes++] = (_BNode) o;
            
        } else {
            
            literals[numLiterals++] = (_Literal) o;
            
        }
        
        stmts[numStmts++] = new _Statement
            ( (_Resource) s,
              (_URI) p,
              (_Value) o
              );
        
    }

    /**
     * Visits all non-duplicate, non-known URIs, Literals, and BNodes in their
     * current order.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    class TermIterator implements IEntryIterator {

        private _Value current = null;
        
        /**
         * Visits non-duplicate, non-known terms in total sorted order (URIs
         * before literals before BNodes).
         * 
         * @todo This also assigned termIds when none is found for a term,
         *       however it might be best to move that behavior into a different
         *       processing stage in {@link BulkRioLoader#bulkLoad(Buffer)}.
         */
        private final Striterator src;
        
        public TermIterator() {

            assert haveKeys;
            assert sorted;
            
            src = new Striterator(EmptyIterator.DEFAULT);

            src.append(new TermClassIterator(uris,numURIs));

            src.append(new TermClassIterator(literals,numLiterals));
            
            src.append(new TermClassIterator(bnodes,numBNodes));

            src.addFilter(new Filter() {

                private static final long serialVersionUID = 1L;

                protected boolean isValid(Object arg0) {
                    
                    _Value term = (_Value)arg0;

                    return !term.duplicate && !term.known;
                    
                }
                
            });
            
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
            
            if(current.termId == 0L) {
                
                // Note: this assigns a term identifier!
                current.termId = store.ndx_termId.nextId();
                
            }
            
            return current;
            
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
            
            if(!hasNext()) {
                
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
    class StatementIterator implements IEntryIterator {

        private KeyOrder keyOrder;
        private int lastVisited = -1;
        private int index = 0;
        
        public StatementIterator(KeyOrder keyOrder) {
            
            this.keyOrder = keyOrder;
            
        }

        public byte[] getKey() {

            if (lastVisited == -1) {

                throw new IllegalStateException();

            }

            _Statement stmt = stmts[lastVisited];

            switch (keyOrder) {
            case SPO:
                return store.keyBuilder.statement2Key(stmt.s.termId,
                        stmt.p.termId, stmt.o.termId);
            case POS:
                return store.keyBuilder.statement2Key(stmt.p.termId,
                        stmt.o.termId, stmt.s.termId);
            case OSP:
                return store.keyBuilder.statement2Key(stmt.o.termId,
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

    class UnknownAndDistinctStatementIterator implements IEntryIterator {

        private final KeyOrder keyOrder;
        private final IStriterator src;
        private _Statement current;
        
        public UnknownAndDistinctStatementIterator(KeyOrder keyOrder) {
        
            this.keyOrder = keyOrder;
            
            src = new Striterator(new StatementIterator(keyOrder))
                    .addFilter(new Filter() {

                        private static final long serialVersionUID = 1L;

                        protected boolean isValid(Object arg0) {

                            _Statement stmt = (_Statement) arg0;

                            return !stmt.duplicate && !stmt.known;

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
                return store.keyBuilder.statement2Key(stmt.s.termId,
                        stmt.p.termId, stmt.o.termId);
            case POS:
                return store.keyBuilder.statement2Key(stmt.p.termId,
                        stmt.o.termId, stmt.s.termId);
            case OSP:
                return store.keyBuilder.statement2Key(stmt.o.termId,
                        stmt.s.termId, stmt.p.termId);
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

    /**
     * Bulk loads the mapping from term to term identifier for pre-sorted,
     * non-duplicate, non-known terms into a new {@link IndexSegment}.
     * 
     * @return The new {@link IndexSegment}.
     */
    public IndexSegment bulkLoadTermIndex(int branchingFactor,File outFile) throws IOException {

        int numTerms = numURIs + numLiterals + numBNodes;
        
        numTerms -= numDupURIs + numDupLiterals + numDupBNodes;

        numTerms -= numKnownURIs + numKnownLiterals + numKnownBNodes;
        
        System.err.println("Building terms index segment: numTerms="+numTerms);
        
        final long begin = System.currentTimeMillis();
        
        IndexSegmentBuilder builder = new IndexSegmentBuilder(outFile, null,
                numTerms, new TermIterator(),
                branchingFactor,
                com.bigdata.rdf.TermIndex.ValueSerializer.INSTANCE,
                IndexSegmentBuilder.DEFAULT_ERROR_RATE);

        IndexSegment seg = new IndexSegment(new IndexSegmentFileStore(
                builder.outFile), new HardReferenceQueue<PO>(
                new DefaultEvictionListener(),
                BTree.DEFAULT_HARD_REF_QUEUE_CAPACITY,
                BTree.DEFAULT_HARD_REF_QUEUE_SCAN), 
                com.bigdata.rdf.TermIndex.ValueSerializer.INSTANCE);

        final long elapsed = System.currentTimeMillis() - begin;

        System.err.println("Built terms index segment: numTerms="+numTerms+", elapsed="+elapsed);

        return seg;
        
    }
    
    /**
     * Bulk loads the reverse mapping for pre-sorted, non-duplicate, non-known
     * terms into a new {@link IndexSegment}.
     * 
     * @return The new {@link IndexSegment}.
     */
    public IndexSegment bulkLoadTermIdentifiersIndex(int branchingFactor, File outFile) throws IOException {

        int numTerms = numURIs + numLiterals + numBNodes;
        
        numTerms -= numDupURIs + numDupLiterals + numDupBNodes;

        numTerms -= numKnownURIs + numKnownLiterals + numKnownBNodes;

        System.err.println("Building ids index segment: numTerms="+numTerms);
        
        final long begin = System.currentTimeMillis();

        IndexSegmentBuilder builder = new IndexSegmentBuilder(outFile, null,
                numTerms, new TermIterator(),
                branchingFactor,
                com.bigdata.rdf.ReverseIndex.ValueSerializer.INSTANCE,
                IndexSegmentBuilder.DEFAULT_ERROR_RATE);

        IndexSegment seg = new IndexSegment(new IndexSegmentFileStore(
                builder.outFile), new HardReferenceQueue<PO>(
                new DefaultEvictionListener(),
                BTree.DEFAULT_HARD_REF_QUEUE_CAPACITY,
                BTree.DEFAULT_HARD_REF_QUEUE_SCAN),
                com.bigdata.rdf.ReverseIndex.ValueSerializer.INSTANCE);

        final long elapsed = System.currentTimeMillis() - begin;

        System.err.println("Built ids index segment: numTerms="+numTerms+", elapsed="+elapsed);

        return seg;
        
    }

    /**
     * Bulk loads the reverse mapping for pre-sorted, non-duplicate, non-known
     * statements into a new {@link IndexSegment}.
     * 
     * @return The new {@link IndexSegment}.
     */
    public IndexSegment bulkLoadStatementIndex(KeyOrder keyOrder, int branchingFactor, File outFile) throws IOException {

        int numStmts = this.numStmts - numDupStmts - numKnownStmts;
        
        System.err.println("Building statement index segment: numStmts="+numStmts);
        
        final long begin = System.currentTimeMillis();

        IndexSegmentBuilder builder = new IndexSegmentBuilder(outFile, null,
                numStmts, new UnknownAndDistinctStatementIterator(keyOrder),
                branchingFactor,
                com.bigdata.rdf.StatementIndex.ValueSerializer.INSTANCE,
                IndexSegmentBuilder.DEFAULT_ERROR_RATE);

        IndexSegment seg = new IndexSegment(new IndexSegmentFileStore(
                builder.outFile), new HardReferenceQueue<PO>(
                new DefaultEvictionListener(),
                BTree.DEFAULT_HARD_REF_QUEUE_CAPACITY,
                BTree.DEFAULT_HARD_REF_QUEUE_SCAN),
                com.bigdata.rdf.StatementIndex.ValueSerializer.INSTANCE
                );
        
        final long elapsed = System.currentTimeMillis() - begin;

        System.err.println("Built " + keyOrder
                + " statement index segment: numStmts=" + numStmts
                + ", elapsed=" + elapsed);

        return seg;
        
    }

}
