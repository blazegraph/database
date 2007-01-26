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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.Parser;
import org.openrdf.rio.StatementHandler;
import org.openrdf.rio.rdfxml.RdfXmlParser;

import com.bigdata.rdf.TripleStore;
import com.bigdata.rdf.rio.OptimizedValueFactory._BNode;
import com.bigdata.rdf.rio.OptimizedValueFactory._Literal;
import com.bigdata.rdf.rio.OptimizedValueFactory._Resource;
import com.bigdata.rdf.rio.OptimizedValueFactory._Statement;
import com.bigdata.rdf.rio.OptimizedValueFactory._URI;
import com.bigdata.rdf.rio.OptimizedValueFactory._Value;

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
public class PresortRioLoader implements StatementHandler
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

    final int queueCapacity = 1;
    
    final BlockingQueue<Buffer> queue = new ArrayBlockingQueue<Buffer>(queueCapacity); 
    
    static class Buffer {
    
        _URI[] uris;
        _Literal[] literals;
        _BNode[] bnodes;
        _Statement[] stmts;
        
        int numURIs, numLiterals, numBNodes;
        int numStmts;

        /*
         * @todo The use of these caches does not appear to improve performance.
         */
//        /**
//         * Hash of the string of the URI to the URI makes URIs unique within a
//         * buffer full of URIs. 
//         */
//        Map<String, _URI> uriMap;
//        /**
//         * FIXME this does not properly handle literals unless the source is a
//         * Literal with optional language code and datatype uri fields. It will
//         * falsely conflate a plain literal with a language code literal having
//         * the same text.
//         */
//        Map<String, _Literal> literalMap;
//        /**
//         * @todo bnodes are always distinct, right?  Or at least often enough
//         * that we don't want to do this.  Also, the bnode id can be encoded 
//         * using a non-unicode conversion since we are generating them ourselves
//         * for the most part.  what makes most sense is probably to pre-convert
//         * a String ID given for a bnode to a byte[] and cache only such bnodes
//         * in this map and all other bnodes should be assigned a byte[] key 
//         * directly instead of an id, e.g., by an efficient conversion of a UUID
//         * to a byte[].
//         */
//        Map<String, _BNode> bnodeMap;

        protected final TripleStore store;
        
        /**
         * The buffer capacity -or- <code>-1</code> if the {@link Buffer}
         * object is signaling that no more buffers will be placed onto the
         * queue by the producer and that the consumer should therefore
         * terminate.
         */
        protected final int capacity;
 
        public Buffer(TripleStore store, int capacity) {
        
            this.store = store;
            
            this.capacity = capacity;
        
            if( capacity == -1 ) return;
            
            uris = new _URI[ capacity ];
            
            literals = new _Literal[ capacity ];
            
            bnodes = new _BNode[ capacity ];

//            if (useTermMaps) {
//
//                uriMap = new HashMap<String, _URI>(capacity);
//                
//                literalMap = new HashMap<String, _Literal>(capacity);
//                
//                bnodeMap = new HashMap<String, _BNode>(capacity);
//                
//            }

            stmts = new _Statement[ capacity ];
            
        }
        
        /**
         * Bulk insert buffered data (terms and statements) into the store.
         */
        public void insert() {

            store.insertTerms(uris, numURIs);

            store.insertTerms(literals, numLiterals);
            
            store.insertTerms(bnodes, numBNodes);
            
            store.addStatements(stmts, numStmts);

        }
        
        /**
         * Returns true if the buffer has less than three slots remaining for
         * any of the value arrays (URIs, Literals, or BNodes) or if there are
         * no slots remaining in the statements array. Under those conditions
         * adding another statement to the buffer could cause an overflow.
         * 
         * @return True if the buffer might overflow if another statement were
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
         * Adds the values and the statement into the buffer.
         * 
         * @param s
         * @param p
         * @param o
         * 
         * @exception IndexOutOfBoundsException if the buffer overflows.
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
        
    }
    
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
