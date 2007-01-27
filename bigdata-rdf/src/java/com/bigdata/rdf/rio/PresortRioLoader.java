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
 * Statement handled for the RIO RDF Parser that collects values and statements
 * in batches and inserts ordered batches into the {@link TripleStore}.
 * 
 * @todo setup a producer-consumer queue. that creates a more or less even
 *       division of labor so as to maximize load rate on a platform with at
 *       least 2 cpus. The producer runs rio fills in buffers, generates keys
 *       for terms, and sorts terms by their keys and then places the buffer
 *       onto the queue. The consumer accepts a buffer with pre-generated term
 *       keys and terms in sorted order by those keys and (a) inserts the terms
 *       into the database; and (b) generates the statement keys for each of the
 *       statement indices, ordered the statement for each index in turn, and
 *       bulk inserts the statements into each index in turn. <br>
 *       pre-generate keys before handing over the buffer to the consumer. <br>
 *       pre-sort terms by their keys before handing over the buffer to the
 *       consumer. <br>
 *       if an LRU cache is to be used, then allow it to cross buffer boundaries
 *       always returning the same value. such values will have a non-null key
 *       so their key will only be generated once. see if i can leverage the
 *       existing cweb LRU classes for this. <br>
 *       sort does not remove duplicate. therefore if we do not use some sort of
 *       cache in the term factory then it may be worth testing if terms in a
 *       sequence are the same term.
 * 
 * @todo compare a bulk insert for each batch with a "perfect" index build and a
 *       compacting merge (i.e., a bulk load that runs outside of the
 *       transaction mechanisms). small documents can clearly fit inside of a
 *       single buffer and batch insert. Up to N buffers could be collected
 *       before deciding whether the document is moderate in size vs very large.
 *       Very large documents can combine N buffers into an index segment and
 *       collect a set of index segments. the commit would merge those index
 *       segments with the database.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PresortRioLoader implements IRioLoader, StatementHandler
{

    /**
     * Note: I am seeing a 1000 tps performance boost at 1M vs 100k for this
     * value.
     */
    final int BUFFER_SIZE = 1000000;
    
    TripleStore store;
    
    long stmtsAdded;
    
    long insertTime;
    
    long insertStart;
    
    Vector<RioLoaderListener> listeners;
    
//    /**
//     * When true, each term in a type specific buffer will be placed into a
//     * cannonicalizing map so that we never have more than once instance for
//     * the same term in a buffer at a time.  When false those transient term
//     * maps are not used.
//     * 
//     * Note: this does not show much of an effect.
//     */
//    final boolean useTermMaps = false;

    /**
     * Used to buffer RDF {@link Value}s and {@link Statement}s emitted by
     * the RDF parser.
     */
    Buffer buffer;
    
    public PresortRioLoader( TripleStore store ) {
    
        this.store = store;

        this.buffer = new Buffer(store, BUFFER_SIZE);
        
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
        buffer = new Buffer(store,BUFFER_SIZE);

        // Parse the data.
        parser.parse( reader, "" );
        
        // bulk insert the buffered data into the store.
        buffer.insert();

        // clear the old buffer reference.
        buffer = null;

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
            buffer.insert();

            // allocate a new buffer.
            buffer = new Buffer(store,BUFFER_SIZE);
            
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
