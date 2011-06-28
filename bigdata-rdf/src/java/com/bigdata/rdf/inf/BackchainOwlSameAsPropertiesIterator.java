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
 * Created on March 11, 2008
 */
package com.bigdata.rdf.inf;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.Logger;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * Provides backward chaining for property collection and reverse property
 * collection on owl:sameAs for all access paths.
 * <p>
 * Note:
 * 
 * @see BackchainOwlSameAsPropertiesSPOIterator
 * @see BackchainOwlSameAsPropertiesSPIterator
 * @see BackchainOwlSameAsPropertiesPOIterator
 * @see BackchainOwlSameAsPropertiesPIterator
 * @see InferenceEngine
 * @see InferenceEngine.Options
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id: BackchainOwlSameAsPropertiesIterator.java,v 1.2 2008/03/20
 *          04:05:51 mrpersonick Exp $
 */
public class BackchainOwlSameAsPropertiesIterator implements IChunkedOrderedIterator<ISPO> {
    
    /**
     * Enables record of stack traces for callers and verification that the
     * iterator is closed.
     */
    private static final boolean RECORD_STACK_TRACES = false;
    
    private static final Logger log =
            Logger.getLogger(BackchainOwlSameAsPropertiesIterator.class);

    private boolean closed = false;
    
    private String stack;

    @Override
    protected void finalize() throws Throwable {
        
        super.finalize();
        
        if (!closed) {
            
            log
                    .error("DANGER! Someone needs to close this iterator correctly.\n"
                            + (stack != null ? stack : ""));
            
        }
        
    }

    private IChunkedOrderedIterator<ISPO> delegate;

    /**
     * Create an iterator that will visit all statements in the source iterator
     * and also backchain any entailments that would have resulted from
     * owl:sameAs {2,3}.
     * 
     * @param src
     *            The source iterator. {@link #nextChunk()} will sort statements
     *            into the {@link IKeyOrder} reported by this iterator (as long
     *            as the {@link IKeyOrder} is non-<code>null</code>).
     * @param s
     *            The subject of the triple pattern.
     * @param p
     *            The predicate of the triple pattern.
     * @param o
     *            The object of the triple pattern.
     * @param db
     *            The database from which we will read the distinct subject
     *            identifiers (iff this is an all unbound triple pattern).
     * @param sameAs
     *            The term identifier that corresponds to owl:sameAs for the
     *            database.
     */
    public BackchainOwlSameAsPropertiesIterator(
            IChunkedOrderedIterator<ISPO> src, IV s, IV p, IV o,
            AbstractTripleStore db, final IV sameAs) {

        if (s != null && o != null) {
            
            this.delegate =
                    new BackchainOwlSameAsPropertiesSPOIterator(
                            src, s, p, o, db, sameAs);
            
        } else if (s != null && o == null) {
            
            this.delegate =
                    new BackchainOwlSameAsPropertiesSPIterator(
                            src, s, p, db, sameAs);
            
        } else if (s == null && o != null) {
            
            this.delegate =
                    new BackchainOwlSameAsPropertiesPOIterator(
                            src, p, o, db, sameAs);
            
        } else if (s == null && o == null) {
            
            this.delegate =
                    new BackchainOwlSameAsPropertiesPIterator(
                            src, p, db, sameAs);
            
        } else throw new AssertionError();
        
        if (RECORD_STACK_TRACES) {
         
            StringWriter sw = new StringWriter();
            
            new Exception("Stack trace").printStackTrace(new PrintWriter(sw));
            
            stack = sw.toString();
            
        }
        
    }

    public IKeyOrder<ISPO> getKeyOrder() {
        
        return delegate.getKeyOrder();
        
    }

    public boolean hasNext() {
        
        return delegate.hasNext();
        
    }

    public ISPO next() {
        
        return delegate.next();
        
    }

    public ISPO[] nextChunk() {
        
        return delegate.nextChunk();
        
    }

    public ISPO[] nextChunk(IKeyOrder<ISPO> keyOrder) {
        
        return delegate.nextChunk(keyOrder);
        
    }

    public void close() {

        delegate.close();
        
        closed = true;
        
    }

    public void remove() {
        
        delegate.remove();
        
    }

}
