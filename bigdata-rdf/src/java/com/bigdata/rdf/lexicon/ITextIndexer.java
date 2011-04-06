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

import java.util.Iterator;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.openrdf.model.Value;

import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.search.FullTextIndex;
import com.bigdata.search.Hiterator;
import com.bigdata.search.IHit;

/**
 * Abstraction for the text indexer for RDF {@link Value}s allowing either the
 * built-in bigdata {@link FullTextIndex} or support for Lucene, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see AbstractTripleStore.Options#TEXT_INDEXER_CLASS
 */
public interface ITextIndexer<A extends IHit> {
   
    public void create();

    public void destroy();

    /**
     * <p>
     * Add the terms to the full text index so that we can do fast lookup of the
     * corresponding term identifiers. Only literals are tokenized. Literals
     * that have a language code property are parsed using a tokenizer
     * appropriate for the specified language family. Other literals and URIs
     * are tokenized using the default {@link Locale}.
     * </p>
     * 
     * @param capacity
     *            A hint to the underlying layer about the buffer size before an
     *            incremental flush of the index.
     * @param itr
     *            Iterator visiting the terms to be indexed.
     * 
     * @todo allow registeration of datatype specific tokenizers (we already
     *       have language family based lookup).
     */
    public void index(int capacity, Iterator<BigdataValue> valuesIterator);

    /**
     * Return <code>true</code> iff datatype literals are being indexed.
     */
    public boolean getIndexDatatypeLiterals();

    /**
     * Do free text search
     * 
     * @param query
     *            The query (it will be parsed into tokens).
     * @param languageCode
     *            The language code that should be used when tokenizing the
     *            query -or- <code>null</code> to use the default {@link Locale}
     *            ).
     * @param prefixMatch
     *            When <code>true</code>, the matches will be on tokens which
     *            include the query tokens as a prefix. This includes exact
     *            matches as a special case when the prefix is the entire token,
     *            but it also allows longer matches. For example,
     *            <code>free</code> will be an exact match on <code>free</code>
     *            but a partial match on <code>freedom</code>. When
     *            <code>false</code>, only exact matches will be made.
     * @param minCosine
     *            The minimum cosine that will be returned.
     * @param maxRank
     *            The upper bound on the #of hits in the result set.
     * @param matchAllTerms
     * 			  if true, return only hits that match all search terms
     * @param timeout
     *            The timeout -or- ZERO (0) for NO timeout (this is equivalent
     *            to using {@link Long#MAX_VALUE}).
     * @param unit
     *            The unit in which the timeout is expressed.
     * 
     * @return The result set.
     */
    public Hiterator<A> search(final String query, final String languageCode,
            final boolean prefixMatch, final double minCosine,
            final int maxRank, final boolean matchAllTerms, 
            long timeout, final TimeUnit unit);

}
