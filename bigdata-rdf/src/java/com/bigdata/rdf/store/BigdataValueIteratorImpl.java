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
 * Created on May 23, 2008
 */

package com.bigdata.rdf.store;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.striterator.IChunkedIterator;

/**
 * Wraps an iterator that visits term identifiers and exposes each visited term
 * identifier as a {@link BigdataValue} (batch API).
 * 
 * @todo The resolution of term identifiers to terms should happen during
 *       asynchronous read-ahead for even better performance.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataValueIteratorImpl implements BigdataValueIterator {

    final protected static Logger log = Logger.getLogger(BigdataValueIteratorImpl.class);

    /**
     * The database whose lexicon will be used to resolve term identifiers to
     * terms.
     */
    private final AbstractTripleStore db;
    
    /**
     * The source iterator.
     */
    private final IChunkedIterator<Long> src;
    
    /**
     * The index of the last entry returned in the current {@link #chunk} and
     * <code>-1</code> until the first entry is returned.
     */
    private int lastIndex = -1;
    
    /**
     * The current chunk from the source iterator and initially <code>null</code>.
     */
    private Long[] chunk = null;

    /**
     * The map that will be used to resolve term identifiers to terms for the
     * current {@link #chunk} and initially <code>null</code>.
     */
    private Map<Long, BigdataValue> terms = null;

    /**
     * 
     * @param db
     *            Used to resolve term identifiers to {@link Value} objects.
     * @param src
     *            The source iterator.
     */
    public BigdataValueIteratorImpl(final AbstractTripleStore db,
            final IChunkedIterator<Long> src) {

        if (db == null)
            throw new IllegalArgumentException();

        if (src == null)
            throw new IllegalArgumentException();

        this.db = db;
        
        this.src = src;

    }
    
    public boolean hasNext() {

        if (lastIndex != -1 && lastIndex + 1 < chunk.length) {
            
            return true;
            
        }
        
        log.debug("Testing source iterator.");
        
        return src.hasNext();
        
    }

    public BigdataValue next() {

        if (!hasNext())
            throw new NoSuchElementException();

        if (lastIndex == -1 || lastIndex + 1 == chunk.length) {

            log.info("Fetching next chunk");
            
            // fetch the next chunk of SPOs.
            chunk = src.nextChunk();

            if(log.isInfoEnabled())
            log.info("Fetched chunk: size="+chunk.length);

            /*
             * Create a collection of the distinct term identifiers used in this
             * chunk.
             */

            final Collection<Long> ids = new HashSet<Long>(chunk.length);

            for (Long id : chunk) {

                ids.add(id);

            }

            if(log.isInfoEnabled())
            log.info("Resolving "+ids.size()+" term identifiers");
            
            // batch resolve term identifiers to terms.
            terms = db.getLexiconRelation().getTerms(ids);

            // reset the index.
            lastIndex = 0;
            
        } else {
            
            // index of the next term identifier in this chunk.
            lastIndex++;
            
        }

        if(log.isDebugEnabled()) {
            
            log.debug("lastIndex="+lastIndex+", chunk.length="+chunk.length);
            
        }
        
        // the current term identifier.
        final Long id = chunk[lastIndex];

        if (log.isDebugEnabled())
            log.debug("termId=" + id);
                
        /*
         * Resolve term identifiers to terms using the map populated when we
         * fetched the current chunk.
         */
        final BigdataValue val = terms.get(id);
        
        if (val == null) {

            throw new RuntimeException("No value for term identifier: id=" + id);

        }
        
        if(log.isDebugEnabled()) {
            
            log.debug("termId=" + id + ", value=" + val);

        }

        return val;
        
    }

    /**
     * @throws UnsupportedOperationException
     * 
     * @todo this could be implemented if we saved the last term identifier
     *       visited.
     */
    public void remove() {
        
        throw new UnsupportedOperationException();
        
    }
    
    public void close() {
    
        log.info("");
        
        src.close();

        chunk = null;
        
        terms = null;
        
    }

}
