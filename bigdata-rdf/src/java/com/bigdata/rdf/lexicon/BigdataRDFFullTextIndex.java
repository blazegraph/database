/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Jun 3, 2010
 */

package com.bigdata.rdf.lexicon;

import java.io.StringReader;
import java.util.Iterator;
import java.util.Properties;

import org.openrdf.model.Literal;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.search.FullTextIndex;
import com.bigdata.search.TokenBuffer;

/**
 * Implementation based on the built-in keyword search capabilities for bigdata.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataRDFFullTextIndex extends FullTextIndex implements
        ITextIndexer {

    static public ITextIndexer getInstance(final IIndexManager indexManager,
            final String namespace, final Long timestamp,
            final Properties properties) {

        if (namespace == null)
            throw new IllegalArgumentException();
        
        return new BigdataRDFFullTextIndex(indexManager, namespace, timestamp,
                properties);
        
    }

    /**
     * When true, index datatype literals as well.
     */
    private final boolean indexDatatypeLiterals;
    
    public boolean getIndexDatatypeLiterals() {
        
        return indexDatatypeLiterals;
        
    }
    
    /**
     * @param indexManager
     * @param namespace
     * @param timestamp
     * @param properties
     */
    public BigdataRDFFullTextIndex(IIndexManager indexManager,
            String namespace, Long timestamp, Properties properties) {

        super(indexManager, namespace, timestamp, properties);

        /*
         * Also index datatype literals?
         */
        indexDatatypeLiterals = Boolean
                .parseBoolean(getProperty(
                        AbstractTripleStore.Options.TEXT_INDEX_DATATYPE_LITERALS,
                        AbstractTripleStore.Options.DEFAULT_TEXT_INDEX_DATATYPE_LITERALS));

    }

    public void index(int capacity, Iterator<BigdataValue> valuesIterator) {
        final TokenBuffer buffer = new TokenBuffer(capacity, this);

        int n = 0;

        while (valuesIterator.hasNext()) {

            final BigdataValue val = valuesIterator.next();

            if (!(val instanceof Literal)) {

                /*
                 * Note: If you allow URIs to be indexed then the code which is
                 * responsible for free text search for quads must impose a
                 * filter on the subject and predicate positions to ensure that
                 * free text search can not be used to materialize literals or
                 * URIs from other graphs. This matters when the named graphs
                 * are used as an ACL mechanism. This would also be an issue if
                 * literals were allowed into the subject position.
                 */
                continue;

            }

            final Literal lit = (Literal) val;

            if (!indexDatatypeLiterals && lit.getDatatype() != null) {

                // do not index datatype literals in this manner.
                continue;

            }

            final String languageCode = lit.getLanguage();

            // Note: May be null (we will index plain literals).
            // if(languageCode==null) continue;

            final String text = lit.getLabel();

            /*
             * Note: The OVERWRITE option is turned off to avoid some of the
             * cost of re-indexing each time we see a term.
             */

            final IV termId = val.getIV();

            assert termId != null; // the termId must have been
                                                    // assigned.

            // don't bother text indexing inline values for now
            if (termId.isInline()) {
                continue;
            }
            
            index(buffer, termId.getTermId(), 0/* fieldId */, languageCode,
                    new StringReader(text));

            n++;

        }

        // flush writes to the text index.
        buffer.flush();

        if (log.isInfoEnabled())
            log.info("indexed " + n + " new terms");

    }

}
