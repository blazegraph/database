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
 * Created on Jan 29, 2007
 */

package com.bigdata.rdf.rio;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import com.bigdata.objndx.BytesUtil;
import com.bigdata.objndx.IndexSegment;
import com.bigdata.objndx.IndexSegmentBuilder;
import com.bigdata.rdf.KeyOrder;
import com.bigdata.rdf.TripleStore;
import com.bigdata.rdf.model.OptimizedValueFactory.OSPComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.POSComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.SPOComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.rio.BulkRioLoader.Indices;

/**
 * Implementation specialized to support bulk index load operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BulkRioLoader
 */
public class BulkLoaderBuffer extends Buffer {

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
    
    /**
     * @param store
     * @param capacity
     * @param distinct
     */
    public BulkLoaderBuffer(TripleStore store, int capacity, boolean distinct) {
        super(store, capacity, distinct);
    }

    /**
     * Bulk loads the mapping from term to term identifier for pre-sorted,
     * non-duplicate, non-known terms into a new {@link IndexSegment} written on
     * <i>outFile</i>.
     */
    public void bulkLoadTermIndex(int branchingFactor,File outFile) throws IOException {

        // #of terms identified by the parser.
        int numTerms = numURIs + numLiterals + numBNodes;
        
        // #of terms that are duplicates of one another.
        int numDupTerms = numDupURIs + numDupLiterals + numDupBNodes;

        // #of terms that are already known to the database.
        int numKnownTerms = numKnownURIs + numKnownLiterals + numKnownBNodes;
        
        // #of terms to be loaded into the index.
        numTerms = numTerms - numDupTerms - numKnownTerms;
        
        System.err.println("Building terms index segment: numTerms="+numTerms);
        
        final long begin = System.currentTimeMillis();
        
        new IndexSegmentBuilder(outFile, null, numTerms,
                new UnknownAndDistinctTermIterator(this), branchingFactor,
                com.bigdata.rdf.TermIndex.ValueSerializer.INSTANCE,
                IndexSegmentBuilder.DEFAULT_ERROR_RATE);

        if(numDupURIs>0)
            assignTermIdsToDuplicateTerms(uris, numURIs);
            
        if(numDupLiterals>0)
            assignTermIdsToDuplicateTerms(literals, numLiterals);
            
        if(numDupBNodes>0)
            assignTermIdsToDuplicateTerms(bnodes, numBNodes);
            
        final long elapsed = System.currentTimeMillis() - begin;

        System.err.println("Built terms index segment: numTerms="+numTerms+", elapsed="+elapsed);
        

    }

    /**
     * Assign termIds to the duplicate terms. This step is required since a
     * {@link _Statement} may otherwise be linked to a term that was flagged as
     * a duplicate and hence did not have a termId assigned by
     * {@link #bulkLoadTermIndex(int, File)}.
     */
    protected void assignTermIdsToDuplicateTerms(_Value[] terms, int nterms) {
       
        int nassigned = 0;
        
        for( int i=1; i<nterms; i++) {
            
            if(terms[i].duplicate) {
                
                assert terms[i-1].termId != 0l;

                terms[i].termId = terms[i-1].termId;
                
                nassigned++;
                
            }
            
        }
        
        if(nassigned != 0) {
            
            System.err.println("Assigned term identifiers to "+nassigned+" duplicate terms");
            
        }
        
    }
    
    /**
     * Bulk loads the reverse mapping for pre-sorted, non-duplicate, non-known
     * terms into a new {@link IndexSegment} written on <i>outFile</i>.
     */
    public void bulkLoadTermIdentifiersIndex(int branchingFactor, File outFile) throws IOException {

        int numTerms = numURIs + numLiterals + numBNodes;
        
        numTerms -= numDupURIs + numDupLiterals + numDupBNodes;

        numTerms -= numKnownURIs + numKnownLiterals + numKnownBNodes;

        System.err.println("Building ids index segment: numTerms="+numTerms);
        
        final long begin = System.currentTimeMillis();

        new IndexSegmentBuilder(outFile, null,
                numTerms, new UnknownAndDistinctTermIterator(this),
                branchingFactor,
                com.bigdata.rdf.ReverseIndex.ValueSerializer.INSTANCE,
                IndexSegmentBuilder.DEFAULT_ERROR_RATE);

        final long elapsed = System.currentTimeMillis() - begin;

        System.err.println("Built ids index segment: numTerms="+numTerms+", elapsed="+elapsed);
        
    }

    /**
     * Bulk loads the reverse mapping for pre-sorted, non-duplicate, non-known
     * statements into a new {@link IndexSegment} written on <i>outFile</i>.
     */
    public void bulkLoadStatementIndex(KeyOrder keyOrder, int branchingFactor, File outFile) throws IOException {

        int numStmts = this.numStmts - numDupStmts - numKnownStmts;
        
        System.err.println("Building statement index segment: numStmts="+numStmts);
        
        final long begin = System.currentTimeMillis();
        
        new IndexSegmentBuilder(outFile, null, numStmts,
                new UnknownAndDistinctStatementIterator(keyOrder,this),
                branchingFactor,
                com.bigdata.rdf.StatementIndex.ValueSerializer.INSTANCE,
                IndexSegmentBuilder.DEFAULT_ERROR_RATE);
        
        final long elapsed = System.currentTimeMillis() - begin;

        System.err.println("Built " + keyOrder
                + " statement index segment: numStmts=" + numStmts
                + ", elapsed=" + elapsed);
        
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
        assert numDupURIs == 0;
        assert numDupLiterals == 0;
        assert numDupBNodes == 0;
        
        final int numTerms = numURIs + numLiterals + numBNodes;
        
        final long begin = System.currentTimeMillis();
        
        numDupURIs = filterDuplicateTerms(uris, numURIs);
        
        numDupLiterals = filterDuplicateTerms(literals, numLiterals);
        
        numDupBNodes = filterDuplicateTerms(bnodes, numBNodes);

        final int numDuplicates = numDupURIs + numDupLiterals + numDupBNodes;
        
        System.err.println("filtered out " + numDuplicates
                + " duplicates from " + numTerms + " terms leaving "
                + (numTerms - numDuplicates) + " terms: "
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
     */
    public void filterDuplicateStatements() {

        assert numDupStmts == 0;
        
        if(numStmts>0){

            final long begin = System.currentTimeMillis();
            
            _Statement lastStmt = stmts[0]; 
            
            for (int i = 1; i < numStmts; i++) {

                _Statement thisStmt = stmts[i];
                
                if( lastStmt.equals(thisStmt) ) {
                    
                    thisStmt.duplicate = true;
                    
                    numDupStmts++;
                    
                } else {
                    
                    lastStmt = thisStmt;
                    
                }
                
            }
            
            System.err.println("filtered out " + numDupStmts
                    + " duplicate statements from " + numStmts + " leaving "
                    + (numStmts - numDupStmts) + " statements: "
                    + (System.currentTimeMillis() - begin) + "ms");

        }

    }
    
    /**
     * Bulk load the buffered data into {@link IndexSegment}s.
     * <p>
     * The {@link TripleStore} maintain one index to map term:id, one index to
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
     *       and see whether or not the term or statement already exists.
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
        generateTermSortKeys(store.keyBuilder);
        
        // sort each type of term (URI, Literal, BNode).
        sortTermsBySortKeys();
        
        /*
         * Mark duplicate terms (term.duplicate := true), counting the #of
         * duplicate terms.
         */
        filterDuplicateTerms();
        
        /*
         * FIXME resolve terms against bloom filter, journal or pre-existing
         * index segments. if found, set termId on the term and set flag to
         * indicate that it is already known (known:=true), counting #of terms
         * to insert (numTerms - numDuplicates - numKnown). if not found, then
         * assign a termId - assigned termIds MUST be disjoint from those
         * already assigned or those that might be assigned concurrently.
         */
        if(store.ndx_termId.getEntryCount()>0 || ! indices.terms.isEmpty()) {
            
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

            // Mark duplicate statements.
            filterDuplicateStatements();

            // FIXME Mark known statements by testing the bloom filter and/or
            // indices.

            if (store.ndx_spo.getEntryCount() > 0
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

        return File.createTempFile(name + "-" + batchId, "seg");
        
    }

}
