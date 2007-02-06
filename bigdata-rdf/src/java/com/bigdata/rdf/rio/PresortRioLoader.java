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

import java.io.Reader;
import java.util.Iterator;
import java.util.Vector;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.Parser;
import org.openrdf.rio.StatementHandler;
import org.openrdf.rio.rdfxml.RdfXmlParser;

import com.bigdata.rdf.TripleStore;
import com.bigdata.rdf.model.OptimizedValueFactory;

/**
 * Statement handler for the RIO RDF Parser that collects values and statements
 * in batches and inserts ordered batches into the {@link TripleStore}.  The
 * default batch size is large enough to absorb many ontologies in a single
 * batch.
 * 
 * @todo try optimization using async IO to write data buffered on the journal
 *       to disk.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PresortRioLoader implements IRioLoader, StatementHandler
{

    /**
     * The default buffer size.
     * <p>
     * Note: I am seeing a 1000 tps performance boost at 1M vs 100k for this
     * value.
     */
    static final int DEFAULT_BUFFER_SIZE = 1000000;
    
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
    
    public PresortRioLoader( TripleStore store ) {
    
        this(store, DEFAULT_BUFFER_SIZE, true );
        
    }
    
    public PresortRioLoader(TripleStore store, int capacity, boolean distinct) {

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
     * statement array. These should be buffers of a settable size.
     * <p>
     * Once the term buffers are full (or the data is exhausted), the term
     * arrays should be sorted and batch inserted into the TripleStore.
     * <p>
     * As each term is inserted, its id should be noted in the Value object, so
     * that the statement array is sortable based on term id.
     * <p>
     * Once the statement buffer is full (or the data is exhausted), the
     * statement array should be sorted and batch inserted into the TripleStore.
     * Also the term buffers should be flushed first.
     * 
     * @param reader
     *            the RDF/XML source
     * @param baseURI
     *            The baseURI or "" if none is known.
     */

    public void loadRdfXml( Reader reader, String baseURI ) throws Exception {
        
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
        if(buffer == null) {
            
            buffer = new Buffer(store,capacity,distinct);
            
        }

        try {

            // Parse the data.
            parser.parse(reader, baseURI);

            // bulk insert the buffered data into the store.
            if(buffer != null) {
                
                buffer.insert();
                
            }

        } catch (RuntimeException ex) {

            log.error("While parsing: " + ex, ex);

            throw ex;
            
        } finally {

            // clear the old buffer reference.
            buffer = null;

        }

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
            buffer.insert();

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
    
}
