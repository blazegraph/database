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

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.RdfKeyBuilder;
import com.bigdata.rdf.TripleStore;
import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._Resource;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.model.OptimizedValueFactory._ValueSortKeyComparator;
import com.bigdata.rdf.rio.MultiThreadedPresortRioLoader.ConsumerThread;

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

    boolean haveKeys = false;
    
    boolean sorted = false;
    
    public Buffer(TripleStore store, int capacity) {
    
        this.store = store;
        
        this.capacity = capacity;
    
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
        
        store.generateSortKeys(keyBuilder, uris, numURIs);
        
        store.generateSortKeys(keyBuilder, literals, numLiterals);
        
        store.generateSortKeys(keyBuilder, bnodes, numBNodes);
        
        haveKeys = true;
        
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
        
        if (numURIs > 0)
            Arrays.sort(uris, 0, numURIs, _ValueSortKeyComparator.INSTANCE);

        if (numLiterals > 0)
            Arrays.sort(literals, 0, numLiterals, _ValueSortKeyComparator.INSTANCE);

        if( numBNodes>0)
            Arrays.sort(bnodes, 0, numBNodes, _ValueSortKeyComparator.INSTANCE);

        sorted = true;

    }
    
    /**
     * Bulk insert buffered data (terms and statements) into the store.
     */
    public void insert() {

        store.insertTerms(uris, numURIs, haveKeys, sorted);

        store.insertTerms(literals, numLiterals, haveKeys, sorted);

        store.insertTerms(bnodes, numBNodes, haveKeys, sorted);
        
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
    
}
