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
package com.bigdata.rdf.rio;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.Parser;
import org.openrdf.rio.StatementHandler;
import org.openrdf.rio.rdfxml.RdfXmlParser;

import com.bigdata.objndx.IndexSegment;
import com.bigdata.rdf.KeyOrder;
import com.bigdata.rdf.TripleStore;
import com.bigdata.rdf.model.OptimizedValueFactory;
import com.bigdata.rdf.model.OptimizedValueFactory.OSPComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.POSComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.SPOComparator;

/**
 * Bulk loader statement handler for the RIO RDF Parser that collects distinct
 * values and statements into batches and bulk loads those batches into
 * {@link IndexSegment}s.
 * 
 * @todo we have to resolve terms against a fused view of the existing btree and
 *       or index segments in order to avoid inconsistent assignments of term
 *       ids to terms in different batches. this is an alternative to using hash
 *       maps. the same problem exists for the statement indices (but we can
 *       test for uniqueness on just one of the statement indices). bloom
 *       filters could help out quite a bit here since they could report whether
 *       we have seen a term or a statement anywhere during a parse and
 *       therefore whether or not we need to check any of the index segments.
 * 
 * @todo handle partitioning of indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BulkRioLoader implements IRioLoader, StatementHandler
{

    /**
     * The default buffer size.
     * <p>
     * Note: I am seeing a 1000 tps performance boost at 1M vs 100k for this
     * value.
     */
    static final int DEFAULT_BUFFER_SIZE = 1000000;
    
    /**
     * The default branching factor used for the generated {@link IndexSegment}s.
     * 
     * @todo review good values for this for each of the index types (terms,
     *       ids, and statements).  also review good branching factors for the
     *       mutable indices used for non-bulk operations. 
     */
    static final int branchingFactor = 4096;
    
    /**
     * Terms and statements are inserted into this store.
     */
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

    long stmtsAdded;
    
    long insertTime;
    
    long insertStart;
    
    Vector<RioLoaderListener> listeners;
    
    /**
     * Used to buffer RDF {@link Value}s and {@link Statement}s emitted by
     * the RDF parser.
     */
    Buffer buffer;
    
    public BulkRioLoader( TripleStore store ) {
    
        this(store, DEFAULT_BUFFER_SIZE, false );
        
    }
    
    public BulkRioLoader(TripleStore store, int capacity, boolean distinct) {

        assert store != null;
        
        assert capacity > 0;

        this.store = store;
        
        this.capacity = capacity;
        
        this.distinct = distinct;
        
        this.buffer = new Buffer(store, capacity, distinct );
        
    }
    
    public long getStatementsAdded() {
        
        return stmtsAdded;
        
    }
    
    public long getInsertTime() {
        
        return insertTime;
        
    }
    
    public long getTotalTime() {
        
        return insertTime;
        
    }
    
    public long getInsertRate() {
        
        return (long) 
            ( ((double)stmtsAdded) / ((double)getTotalTime()) * 1000d );
        
    }
    
    public void addRioLoaderListener( RioLoaderListener l ) {
        
        if ( listeners == null ) {
            
            listeners = new Vector<RioLoaderListener>();
            
        }
        
        listeners.add( l );
        
    }
    
    public void removeRioLoaderListener( RioLoaderListener l ) {
        
        listeners.remove( l );
        
    }
    
    protected void notifyListeners() {
        
        RioLoaderEvent e = new RioLoaderEvent
            ( stmtsAdded,
              System.currentTimeMillis() - insertStart
              );
        
        for ( Iterator<RioLoaderListener> it = listeners.iterator(); 
              it.hasNext(); ) {
            
            it.next().processingNotification( e );
            
        }
        
    }
    
    /**
     * We need to collect two (three including bnode) term arrays and one
     * statement array.  These should be buffers of a settable size.
     * <p>
     * Once the term buffers are full (or the data is exhausted), the term 
     * arrays should be sorted and batch inserted into the TripleStore.
     * <p>
     * As each term is inserted, its id should be noted in the Value object,
     * so that the statement array is sortable based on term id.
     * <p>
     * Once the statement buffer is full (or the data is exhausted), the 
     * statement array should be sorted and batch inserted into the
     * TripleStore.  Also the term buffers should be flushed first.
     * 
     * @param reader
     *                  the RDF/XML source
     */

    public void loadRdfXml( Reader reader ) throws Exception {
        
        OptimizedValueFactory valueFac = new OptimizedValueFactory();
        
        Parser parser = new RdfXmlParser( valueFac );
        
        parser.setVerifyData( false );
        
        parser.setStatementHandler( this );
        
        // Note: reset to that rates reflect load times not clock times.
        insertStart = System.currentTimeMillis();
        insertTime = 0; // clear.
        
        // Note: reset so that rates are correct for each source loaded.
        stmtsAdded = 0;
        
        // Allocate the initial buffer for parsed data.
        if(buffer != null) {
            
            buffer = new Buffer(store,capacity,distinct);
            
        }

        try {

            // Parse the data.
            parser.parse(reader, "");

            // bulk load insert the buffered data into the store.
            if(buffer!=null) {
                
                bulkLoad(buffer);
                
            }

        } catch (RuntimeException ex) {

            log.error("While parsing: " + ex, ex);

            throw ex;
            
        } finally {

            // clear the old buffer reference.
            buffer = null;

        }

        store.commit();

        insertTime += System.currentTimeMillis() - insertStart;
        
    }
    
    public void handleStatement( Resource s, URI p, Value o ) {

        /* 
         * When the buffer could not accept three of any type of value plus 
         * one more statement then it is considered to be near capacity and
         * is flushed to prevent overflow. 
         */
        if(buffer.nearCapacity()) {

            // bulk insert the buffered data into the store.
            try {
                
                bulkLoad(buffer);
                
            } catch(IOException ex) {

                throw new RuntimeException(ex);
                
            }

            // allocate a new buffer.
            buffer = new Buffer(store,capacity,distinct);
            
            // fall through.
            
        }
        
        // add the terms and statement to the buffer.
        buffer.handleStatement(s,p,o);
        
        stmtsAdded++;
        
        if ( stmtsAdded % 100000 == 0 ) {
            
            notifyListeners();
            
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
    protected void bulkLoad(Buffer buffer) throws IOException {
        
        // generate sort keys.
        buffer.generateSortKeys(store.keyBuilder);
        
        // sort each type of term (URI, Literal, BNode).
        buffer.sortTerms();
        
        /*
         * Mark duplicate terms (term.duplicate := true), counting the #of
         * duplicate terms.
         * 
         * FIXME I am not seeing the expected #of distinct terms for wordnet
         * when compared with the other load methods.
         */
        buffer.filterDuplicateTerms();
        
        /*
         * FIXME resolve terms against bloom filter, journal or pre-existing
         * index segments. if found, set termId on the term and set flag to
         * indicate that it is already known (known:=true), counting #of terms
         * to insert (numTerms - numDuplicates - numKnown). if not found, then
         * assign a termId - assigned termIds MUST be disjoint from those
         * already assigned or those that might be assigned concurrently.
         */
        if(store.ndx_termId.getEntryCount()>0 || ! indices_termId.isEmpty()) {
            
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
        buffer.bulkLoadTermIndex(branchingFactor,getNextOutFile("terms"));
        
        /*
         * Bulk load the reverse termId:term index with the unknown terms. 
         */
        buffer.sortTermIds(); // first sort by termIds.
        buffer.bulkLoadTermIdentifiersIndex(branchingFactor,getNextOutFile("ids"));
        
        /*
         * For each statement access path, generate the corresponding key and
         * load an IndexSegment for that access path.
         */

        // sort on SPO index first.
        Arrays.sort(buffer.stmts, 0, buffer.numStmts, SPOComparator.INSTANCE);

        // Mark duplicate statements.
        buffer.filterDuplicateStatements();
        
        // FIXME Mark known statements by testing the bloom filter and/or indices.
        
        if(store.ndx_spo.getEntryCount()>0 || ! indices_spo.isEmpty()) {

            throw new UnsupportedOperationException();
            
        }

        // Note: already sorted on SPO key.
        buffer.bulkLoadStatementIndex(KeyOrder.SPO,branchingFactor,getNextOutFile("spo"));
        
        Arrays.sort(buffer.stmts, 0, buffer.numStmts, POSComparator.INSTANCE);
        buffer.bulkLoadStatementIndex(KeyOrder.POS,branchingFactor,getNextOutFile("pos"));

        Arrays.sort(buffer.stmts, 0, buffer.numStmts, OSPComparator.INSTANCE);
        buffer.bulkLoadStatementIndex(KeyOrder.OSP,branchingFactor,getNextOutFile("ops"));
        
        // End of this batch.
        batchId++;
        
    }
    
    /*
     * The index segments generated during the load.
     */
    List<IndexSegment> indices_termId = new LinkedList<IndexSegment>();
    List<IndexSegment> indices_idTerm= new LinkedList<IndexSegment>();
    List<IndexSegment> indices_spo = new LinkedList<IndexSegment>();
    List<IndexSegment> indices_pos = new LinkedList<IndexSegment>();
    List<IndexSegment> indices_ops = new LinkedList<IndexSegment>();

    /**
     * Used to assign ordered names to each index segment.
     */
    private int batchId = 0;

    /**
     * Return a unique file name that will be used to create an index segment of
     * the same name.
     * 
     * @param name
     *            The index name (terms, ids, spo, pos, ops).
     * 
     * @return The unique file name.
     */
    protected File getNextOutFile(String name) throws IOException {

        return File.createTempFile(name + "-" + batchId, "seg");
        
    }
    
}
