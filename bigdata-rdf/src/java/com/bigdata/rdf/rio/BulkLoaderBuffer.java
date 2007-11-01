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

import it.unimi.dsi.mg4j.util.BloomFilter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.NodeSerializer;
import com.bigdata.btree.RecordCompressor;
import com.bigdata.rdf.model.OptimizedValueFactory.OSPComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.POSComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.SPOComparator;
import com.bigdata.rdf.rio.BulkRioLoader.Indices;
import com.bigdata.rdf.serializers.RdfValueSerializer;
import com.bigdata.rdf.serializers.StatementSerializer;
import com.bigdata.rdf.serializers.TermIdSerializer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.util.KeyOrder;

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
        
        System.err.println("Building terms index segment: numTerms="+numTerms);
        
        final long begin = System.currentTimeMillis();
        
        new IndexSegmentBuilder(outFile, null, numTerms, new TermIdIterator(
                this), branchingFactor, TermIdSerializer.INSTANCE, useChecksum,
                recordCompressor, errorRate, database.getTermIdIndex()
                        .getIndexUUID());

        final long elapsed = System.currentTimeMillis() - begin;

        System.err.println("Built terms index segment: numTerms="+numTerms+", elapsed="+elapsed);
        

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

        System.err.println("Building ids index segment: numTerms="+numTerms);
        
        final long begin = System.currentTimeMillis();

        new IndexSegmentBuilder(outFile, null, numTerms,
                new TermIterator(this), branchingFactor,
                RdfValueSerializer.INSTANCE, useChecksum, recordCompressor,
                errorRate, database.getIdTermIndex().getIndexUUID());

        final long elapsed = System.currentTimeMillis() - begin;

        System.err.println("Built ids index segment: numTerms="+numTerms+", elapsed="+elapsed);
        
    }

    /**
     * Bulk loads the reverse mapping for pre-sorted, non-duplicate, non-known
     * statements into a new {@link IndexSegment} written on <i>outFile</i>.
     */
    public void bulkLoadStatementIndex(KeyOrder keyOrder, int branchingFactor, File outFile) throws IOException {

        int numStmts = this.numStmts - numKnownStmts;
        
        System.err.println("Building statement index segment: numStmts="+numStmts);
        
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

        System.err.println("Built " + keyOrder
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

}
