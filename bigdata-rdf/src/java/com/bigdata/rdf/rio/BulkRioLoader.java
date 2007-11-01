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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.StatementHandler;

import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentFileStore;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.scaleup.MasterJournal;

/**
 * Bulk loader statement handler for the RIO RDF Parser that collects distinct
 * values and statements into batches and bulk loads those batches into
 * {@link IndexSegment}s.
 * 
 * @todo mark generated files for deletion in the test suite.
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
public class BulkRioLoader extends BasicRioLoader implements IRioLoader, StatementHandler
{

    /**
     * The default branching factor used for the generated {@link IndexSegment}s.
     * 
     * @todo review good values for this for each of the index types (terms,
     *       ids, and statements).  also review good branching factors for the
     *       mutable indices used for non-bulk operations. 
     */
    static final int branchingFactor = 4*Bytes.kilobyte32;
    
    /**
     * Terms and statements are inserted into this store.
     */
    protected final AbstractTripleStore store;
    
    /**
     * The buffer capacity -or- <code>-1</code> if the {@link StatementBuffer}
     * object is signaling that no more buffers will be placed onto the queue by
     * the producer and that the consumer should therefore terminate.
     */
    protected final int capacity;

    /**
     * Used to assign ordered names to each index segment.
     */
    private int batchId = 0;

    /**
     * The set of {@link IndexSegment}s generated for each index during the
     * batch load.
     */
    protected Indices indices = new Indices();
    
    /**
     * Used to buffer RDF {@link Value}s and {@link Statement}s emitted by
     * the RDF parser.
     */
    BulkLoaderBuffer buffer;
    
    public BulkRioLoader(AbstractTripleStore store, int capacity) {

        assert store != null;
        
        assert capacity > 0;

        this.store = store;
        
        this.capacity = capacity;
        
        this.buffer = new BulkLoaderBuffer(store, capacity);
        
    }
    
    /** Allocate the initial buffer for parsed data. */
    protected void before() {
        
        if(buffer == null) {
            
            buffer = new BulkLoaderBuffer(store,capacity);
            
        }

    }

    /**
     * Flush any data still in the buffer.
     */
    protected void success() {

        if(buffer!=null) {
            
            // Bulk load the buffered data into {@link IndexSegment}s.

            try {

                buffer.bulkLoad(batchId++,branchingFactor,indices);
                
            } catch(IOException ex) {
                
                throw new RuntimeException(ex);
                
            }
            
        }

    }

    /** clear the old buffer reference. */
    protected void cleanUp() {

        buffer = null;

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
                
                // Bulk load the buffered data into {@link IndexSegment}s.
                buffer.bulkLoad(batchId++,branchingFactor,indices);
                
            } catch(IOException ex) {

                throw new RuntimeException(ex);
                
            }

            // allocate a new buffer.
            buffer = new BulkLoaderBuffer(store,capacity);
            
            // fall through.
            
        }
        
        // add the terms and statement to the buffer.
        buffer.handleStatement(s,p,o,StatementEnum.Explicit);
        
        stmtsAdded++;
        
        if ( stmtsAdded % 100000 == 0 ) {
            
            notifyListeners();
            
        }
        
    }

    /**
     * A collection of indices each of which has at least one index segments
     * generated during the load.
     * 
     * @todo store File or String and if String then the absolute or the
     *       relative filename?
     * 
     * @todo and hash lookup of the index segments iff open (isolate as
     *       getTermIndices:Iterator<IndexSegment>?)
     * 
     * @deprecated Replace this with the use of a {@link MasterJournal}.
     */
    public static class Indices {
    
        List<File> terms = new ArrayList<File>();

        List<File> ids = new ArrayList<File>();

        List<File> spo = new ArrayList<File>();

        List<File> pos = new ArrayList<File>();

        List<File> osp = new ArrayList<File>();

        public Indices() {
            
        }

        /**
         * Map of the currently open {@link IndexSegment}s.
         * 
         * @todo could be a weak value hash map with an LRU to retain segments
         *       that are getting use.
         */
        Map<File,IndexSegment> indices = new HashMap<File,IndexSegment>();

        /**
         * @todo these variants could be combined if we saved more of the metadata
         * in the index
         */
        protected IndexSegment getTermIndex(File file) {

            IndexSegment seg = indices.get(file);

            if (seg == null) {

                seg = new IndexSegmentFileStore(file).load();
                // com.bigdata.rdf.serializers.TermIdSerializer.INSTANCE)
            }
            
            return seg;
            
        }


        protected IndexSegment getIdIndex(File file) {

            IndexSegment seg = indices.get(file);

            if (seg == null) {

                seg = new IndexSegmentFileStore(file).load();
//                        com.bigdata.rdf.serializers.RdfValueSerializer.INSTANCE);

            }
            
            return seg;
            
        }

        protected IndexSegment getStatementIndex(File file) {

            IndexSegment seg = indices.get(file);

            if (seg == null) {

                seg = new IndexSegmentFileStore(file).load();
//                com.bigdata.rdf.serializers.StatementSerializer.INSTANCE);

            }
            
            return seg;
            
        }

    }

}
