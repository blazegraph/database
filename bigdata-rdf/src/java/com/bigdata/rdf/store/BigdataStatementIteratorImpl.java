/*

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
package com.bigdata.rdf.store;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Wraps the raw iterator that traverses a statement index and exposes each
 * visited statement as a {@link BigdataStatement} (batch API).
 * 
 * @todo The resolution of term identifiers to terms should happen during
 *       asynchronous read-ahead for even better performance (less latency).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BigdataSolutionResolverator
 */
public class BigdataStatementIteratorImpl implements BigdataStatementIterator {

    final protected static Logger log = Logger.getLogger(BigdataStatementIteratorImpl.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();
    
    /**
     * The database whose lexicon will be used to resolve term identifiers to
     * terms.
     */
    private final AbstractTripleStore db;
    
    /**
     * The source iterator (will be closed when this iterator is closed).
     */
    private final IChunkedOrderedIterator<ISPO> src;
    
    /**
     * The index of the last entry returned in the current {@link #chunk} and
     * <code>-1</code> until the first entry is returned.
     */
    private int lastIndex = -1;
    
    /**
     * The current chunk from the source iterator and initially <code>null</code>.
     */
    private ISPO[] chunk = null;

    /**
     * The map that will be used to resolve term identifiers to terms for the
     * current {@link #chunk} and initially <code>null</code>.
     */
    private Map<Long, BigdataValue> terms = null;
    
    final private BigdataValueFactory valueFactory;
    
    /**
     * 
     * @param db
     *            Used to resolve term identifiers to {@link Value} objects.
     * @param src
     *            The source iterator (will be closed when this iterator is
     *            closed).
     */
    public BigdataStatementIteratorImpl(AbstractTripleStore db,
            IChunkedOrderedIterator<ISPO> src) {

        if (db == null)
            throw new IllegalArgumentException();

        if (src == null)
            throw new IllegalArgumentException();

        this.db = db;
        
        this.src = src;

        this.valueFactory = db.getValueFactory();
        
    }
    
    public boolean hasNext() {

        if (lastIndex != -1 && lastIndex + 1 < chunk.length) {
            
            return true;
            
        }
        
        if(DEBUG) {
            
            log.debug("Testing source iterator.");
            
        }
        
        return src.hasNext();
        
    }

    public BigdataStatement next() {

        if (!hasNext())
            throw new NoSuchElementException();

        if (lastIndex == -1 || lastIndex + 1 == chunk.length) {

            if (INFO)
                log.info("Fetching next chunk");
            
            // fetch the next chunk of SPOs.
            chunk = src.nextChunk();

            if (log.isInfoEnabled())
                log.info("Fetched chunk: size=" + chunk.length);

            /*
             * Create a collection of the distinct term identifiers used in this
             * chunk.
             */

            final Collection<Long> ids = new HashSet<Long>(chunk.length * 4);

            for (ISPO spo : chunk) {

                ids.add(spo.s());

                ids.add(spo.p());

                ids.add(spo.o());

                if (spo.hasStatementIdentifier()) {

                    ids.add(spo.getStatementIdentifier());

                }

            }

            if (log.isInfoEnabled())
                log.info("Resolving " + ids.size() + " term identifiers");
            
            // batch resolve term identifiers to terms.
            terms = db.getLexiconRelation().getTerms(ids);

            // reset the index.
            lastIndex = 0;
            
        } else {
            
            // index of the next SPO in this chunk.
            lastIndex++;
            
        }

        if(DEBUG) {
            
            log.debug("lastIndex="+lastIndex+", chunk.length="+chunk.length);
            
        }
        
        // the current SPO
        final ISPO spo = chunk[lastIndex];

        if(DEBUG) {
            
            log.debug("spo="+spo);
            
        }
                
        /*
         * Resolve term identifiers to terms using the map populated when we
         * fetched the current chunk.
         */
        final BigdataResource s = (BigdataResource) terms.get(spo.s());
        final BigdataURI p = (BigdataURI) terms.get(spo.p());
        final BigdataValue o = terms.get(spo.o());
        final BigdataResource c = (spo.hasStatementIdentifier() ? (BigdataResource) terms
                .get(spo.getStatementIdentifier())
                : null);
        
        if (spo.hasStatementType() == false) {

            log.error("statement with no type: "
                    + valueFactory.createStatement(s, p, o, c, null));

        }
        
        // the statement.
        final BigdataStatement stmt = valueFactory.createStatement(s, p, o, c,
                spo.getStatementType());

        if (DEBUG) {
            
            log.debug("stmt=" + stmt);

        }

        return stmt;

    }

    /**
     * @throws UnsupportedOperationException
     * 
     * @todo this could be implemented if we save a reference to the last
     *       {@link SPO} visited.
     */
    public void remove() {

        throw new UnsupportedOperationException();

    }

    public void close() {

        if (INFO)
            log.info("");
        
        src.close();

        chunk = null;
        
        terms = null;
        
    }

}
