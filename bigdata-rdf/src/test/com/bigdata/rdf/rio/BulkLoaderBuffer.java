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
 * Created on Jan 29, 2007
 */

package com.bigdata.rdf.rio;

import it.unimi.dsi.mg4j.util.BloomFilter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.NoSuccessorException;
import com.bigdata.btree.NodeSerializer;
import com.bigdata.btree.RecordCompressor;
import com.bigdata.rdf.model.OptimizedValueFactory.OSPComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.POSComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.SPOComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.rio.BulkRioLoader.Indices;
import com.bigdata.rdf.serializers.RdfValueSerializer;
import com.bigdata.rdf.serializers.StatementSerializer;
import com.bigdata.rdf.serializers.TermIdSerializer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * Implementation specialized to support bulk index load operations (not ready
 * for use).
 * 
 * @todo refactor to support a map/reduce style bulk index generation using hash
 *       partitioned indices. map tasks are triple sources, e.g., reading
 *       rdf/xml or reading documents from which they extract rdf/xml. reduce
 *       tasks generate the indices corresponding to the hash partitions. each
 *       index (term:id, id:term, and statement indices) is generated as a set
 *       of index segments.
 * 
 * @todo this needs to be rewritten to support scale-out indices. Also, the bulk
 *       loader should probably attempt to directly generate index segments
 *       rather than proceeding through journal overflow operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BulkRioLoader
 */
public class BulkLoaderBuffer extends StatementBuffer {

    /**
     * #of URIs, literals, and bnodes that have been determined to already be in
     * the terms index.
     */
    int numKnownURIs = 0, numKnownLiterals = 0, numKnownBNodes = 0;

    /**
     * #of already known statements.
     */
    int numKnownStmts = 0;
    
    /**
     * @todo experiment with and without checksum computation.
     */
    boolean useChecksum = false;
    
    /**
     * @todo experiment with and without record compression on the different
     *       indices - I have seen some EOFs that relate to a too small
     *       compression buffer in the {@link NodeSerializer} - the fix for that
     *       is to grow that buffer as required.
     * 
     * @todo look for faster implementations of the ZIP algorithm.
     * @todo try hamming codes for the keys.
     * @todo try dictionaries for the values.
     * @todo try prefix isolation for the ids index values (the terms).
     */
//    RecordCompressor recordCompressor = new RecordCompressor();
    RecordCompressor recordCompressor = null;

    /**
     * The error rate for the bloom filter or zero to disable.
     * <p>
     * Note: The bloom filter is QUITE EXPENSIVE to generate and is not required
     * for inference or common queries and it is therefore disabled at this
     * time. If you need to enable this, you should look into the versions of
     * the {@link BloomFilter} derived by the heritrix project since they put
     * some effort into optimizing performance. Note that all keys are variable
     * length byte[]s, so that is the only hash method that needs to be
     * implemented by the bloom filter class.
     */
//    double errorRate = IndexSegmentBuilder.DEFAULT_ERROR_RATE;
    double errorRate = 0d;
    
    private final AbstractTripleStore database;
    
    /**
     * @param database
     * @param capacity
     */
    public BulkLoaderBuffer(AbstractTripleStore database, int capacity) {
        
        super(database, capacity);
        
        this.database = database;
        
    }

    /**
     * Bulk loads the mapping from term to term identifier for pre-sorted,
     * unknown terms into a new {@link IndexSegment} written on <i>outFile</i>.
     */
    public void bulkLoadTermIndex(int branchingFactor,File outFile) throws IOException {

        // #of terms identified by the parser.
        int numTerms = numURIs + numLiterals + numBNodes;
        
        // #of terms that are already known to the database.
        int numKnownTerms = numKnownURIs + numKnownLiterals + numKnownBNodes;
        
        // #of terms to be loaded into the index.
        numTerms = numTerms - numKnownTerms;
        
        log.info("Building terms index segment: numTerms="+numTerms);
        
        final long begin = System.currentTimeMillis();
        
        new IndexSegmentBuilder(outFile, null, numTerms, new TermIdIterator(
                this), branchingFactor, TermIdSerializer.INSTANCE, useChecksum,
                recordCompressor, errorRate, database.getTermIdIndex()
                        .getIndexUUID());

        final long elapsed = System.currentTimeMillis() - begin;

        log.info("Built terms index segment: numTerms="+numTerms+", elapsed="+elapsed);
        
    }

    /**
     * Bulk loads the reverse mapping for pre-sorted, non-duplicate, non-known
     * terms into a new {@link IndexSegment} written on <i>outFile</i>.
     * 
     * @todo only load into the reverse index if the term was unknown when we
     *       loaded the forward index (this has already been optimized on the
     *       batch and non-batch apis).
     */
    public void bulkLoadTermIdentifiersIndex(int branchingFactor, File outFile) throws IOException {

        int numTerms = numURIs + numLiterals + numBNodes;

        numTerms -= numKnownURIs + numKnownLiterals + numKnownBNodes;

        log.info("Building ids index segment: numTerms="+numTerms);
        
        final long begin = System.currentTimeMillis();

        new IndexSegmentBuilder(outFile, null, numTerms,
                new TermIterator(this), branchingFactor,
                RdfValueSerializer.INSTANCE, useChecksum, recordCompressor,
                errorRate, database.getIdTermIndex().getIndexUUID());

        final long elapsed = System.currentTimeMillis() - begin;

        log.info("Built ids index segment: numTerms="+numTerms+", elapsed="+elapsed);
        
    }

    /**
     * Bulk loads the reverse mapping for pre-sorted, non-duplicate, non-known
     * statements into a new {@link IndexSegment} written on <i>outFile</i>.
     */
    public void bulkLoadStatementIndex(KeyOrder keyOrder, int branchingFactor, File outFile) throws IOException {

        int numStmts = this.numStmts - numKnownStmts;
        
        log.info("Building statement index segment: numStmts="+numStmts);
        
        final long begin = System.currentTimeMillis();
        
        final IIndex ndx;
        switch(keyOrder) {
        case SPO:
            ndx = database.getSPOIndex();
            break;
        case POS:
            ndx = database.getPOSIndex();
            break;
        case OSP:
            ndx = database.getOSPIndex();
            break;
        default:
            throw new AssertionError("Unknown keyOrder=" + keyOrder);
        }
        
        new IndexSegmentBuilder(outFile, null, numStmts,
                new UnknownStatementIterator(keyOrder, this), branchingFactor,
                StatementSerializer.INSTANCE, useChecksum, recordCompressor,
                errorRate, ndx.getIndexUUID());
        
        final long elapsed = System.currentTimeMillis() - begin;

        log.info("Built " + keyOrder
                + " statement index segment: numStmts=" + numStmts
                + ", elapsed=" + elapsed);
        
    }        
    
    /**
     * Bulk load the buffered data into {@link IndexSegment}s.
     * <p>
     * The {@link ITripleStore} maintain one index to map term:id, one index to
     * map id:term, and one index per access path for the statements. For each
     * of these indices we now generate an {@link IndexSegment}.
     * <p>
     * 
     * @param buffer
     * 
     * FIXME try buffering nodes in memory to remove disk IO from the picture
     * during the index segment builds.  
     * 
     * @todo try a high precision bloom filter for the terms and spo indices as
     *       a fast means to detect whether a non-duplicate term or statement
     *       has been seen already during the load. if the bloom filter reports
     *       false, then we definately have NOT seen that term/statement. if the
     *       bloom filter reports true, then we have to test the actual index(s)
     *       and see whether or not the term or statement already exists.  the
     *       bloom filter could either be per-index or a bloom filter could be
     *       created for the entire bulk load.
     * 
     * @todo an alternative to testing for known terms or statements is to form
     *       a consistent view. note however that allowing inconsistencies in
     *       the term:id mapping will require us to update (delete + insert)
     *       statements formed with term identifiers that are deemed
     *       inconsistent.
     */
    public void bulkLoad(int batchId, int branchingFactor,Indices indices)
        throws IOException
    {
        
        // generate sort keys.
        generateTermSortKeys(database.getKeyBuilder());
        
        // sort each type of term (URI, Literal, BNode).
        sortTermsBySortKeys();
        
        /*
         * FIXME resolve terms against bloom filter, journal or pre-existing
         * index segments. if found, set termId on the term and set flag to
         * indicate that it is already known (known:=true), counting #of terms
         * to insert (numTerms - numKnown). if not found, then assign a termId.
         * 
         * FIXME assigned termIds MUST be disjoint from those already assigned
         * or those that might be assigned concurrently.
         * 
         * FIXME verify that bloom filters are enabled and in use for index
         * segments.
         */
        if(database.getTermIdIndex().rangeCount(null,null)>0 || ! indices.terms.isEmpty()) {
            
            /*
             * read against the terms index on the journal and each segment of
             * the terms index that has already been built.
             * 
             * @todo eventually read against a historical state for the terms
             * index.
             */
            throw new UnsupportedOperationException();
            
        }
        
        /*
         * Insert non-duplicate, non-known terms into term index using a single
         * index segment build for all term types (URIs, Literals, and BNodes).
         * 
         * Note: The terms are already in sorted order by their assigned sort
         * keys.
         */
        {
            
            File outFile = getNextOutFile("terms",batchId);
            
            bulkLoadTermIndex(branchingFactor,outFile);
            
            indices.terms.add(outFile);

        }
        
        /*
         * Bulk load the reverse termId:term index with the unknown terms. 
         */
        {
            
            sortTermsByTermIds(); // first sort by termIds.
            
            File outFile = getNextOutFile("ids",batchId);
            
            bulkLoadTermIdentifiersIndex(branchingFactor,outFile);
         
            indices.ids.add(outFile);

        }
        
        /*
         * For each statement access path, generate the corresponding key and
         * load an IndexSegment for that access path.
         */

        {

            // sort on SPO index first.
            Arrays.sort(stmts, 0, numStmts, SPOComparator.INSTANCE);

            // FIXME Mark known statements by testing the bloom filter and/or
            // indices.

            if (database.getStatementCount() > 0
                    || !indices.spo.isEmpty()) {

                throw new UnsupportedOperationException();

            }

            File outFile = getNextOutFile("spo", batchId);
            
            // Note: already sorted on SPO key.
            bulkLoadStatementIndex(KeyOrder.SPO, branchingFactor,outFile);

            indices.spo.add(outFile);
            
        }

        {

            File outFile = getNextOutFile("pos",batchId);
            
            Arrays.sort(stmts, 0, numStmts, POSComparator.INSTANCE);
        
            bulkLoadStatementIndex(KeyOrder.POS,branchingFactor,outFile);
         
            indices.pos.add(outFile);

        }

        {

            File outFile = getNextOutFile("ops",batchId);
            
            Arrays.sort(stmts, 0, numStmts, OSPComparator.INSTANCE);
            
            bulkLoadStatementIndex(KeyOrder.OSP,branchingFactor,outFile);
         
            indices.osp.add(outFile);
            
        }
    
    }

    /**
     * Return a unique file name that will be used to create an index segment of
     * the same name.
     * 
     * @param name
     *            The index name (terms, ids, spo, pos, ops).
     * 
     * @param batchId
     *            The index batch identifier.
     * 
     * @return The unique file name.
     */
    protected File getNextOutFile(String name, int batchId) throws IOException {

        return File.createTempFile(name + "-" + batchId, ".seg", new File("."));
        
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
            
            this.keyBuilder = buffer.getDatabase().getKeyBuilder();
            
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
                
                log.error("*** Iterator exhausted after: "+nvisited+" elements", ex);
                
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
            
            this(keyOrder, buffer.getDatabase().getKeyBuilder(), buffer.stmts,
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

}
