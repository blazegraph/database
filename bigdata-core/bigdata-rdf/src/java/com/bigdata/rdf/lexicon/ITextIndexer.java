/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.io.Serializable;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.openrdf.model.Value;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BDS;
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

/*
 * Moved to IValueCentricTextIndexer    
 */
//    /**
//     * <p>
//     * Add the terms to the full text index so that we can do fast lookup of the
//     * corresponding term identifiers. Only literals are tokenized. Literals
//     * that have a language code property are parsed using a tokenizer
//     * appropriate for the specified language family. Other literals and URIs
//     * are tokenized using the default {@link Locale}.
//     * </p>
//     * 
//     * @param capacity
//     *            A hint to the underlying layer about the buffer size before an
//     *            incremental flush of the index.
//     * @param itr
//     *            Iterator visiting the terms to be indexed.
//     * 
//     * @todo allow registeration of datatype specific tokenizers (we already
//     *       have language family based lookup).
//     */
//    public void index(int capacity, Iterator<BigdataValue> valuesIterator);

    /**
     * Return <code>true</code> iff datatype literals are being indexed.
     */
    public boolean getIndexDatatypeLiterals();

	/**
	 * Do a free text search.
	 * 
	 * @param query
	 *            The query.
	 * 
	 * @return The result set.
	 */
    public Hiterator<A> search(final FullTextQuery query);

	/**
	 * Count free text search results.
	 * 
	 * @param query
	 *            The query.
	 *
	 * @return The result count.
	 */
    public int count(final FullTextQuery query);

    
    public static class FullTextQuery implements Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 4159873519447769476L;
    	
		final String query;
		final String languageCode;
        final boolean prefixMatch; 
        final double minCosine;
        final double maxCosine;
        final int minRank;
        final int maxRank; 
        final boolean matchAllTerms;
        final boolean matchExact; 
        final long timeout; 
        final TimeUnit unit; 
        final String matchRegex;
		
        public FullTextQuery(final String query) {
        	this(
    			query, 
    			null,
    			BDS.DEFAULT_PREFIX_MATCH,
    			null,
    			BDS.DEFAULT_MATCH_ALL_TERMS,
    			BDS.DEFAULT_MATCH_EXACT,
    			BDS.DEFAULT_MIN_RELEVANCE,
    			BDS.DEFAULT_MAX_RELEVANCE,
    			BDS.DEFAULT_MIN_RANK,
    			BDS.DEFAULT_MAX_RANK,
    			BDS.DEFAULT_TIMEOUT,
    			TimeUnit.MILLISECONDS
    			);
        }
        
        public FullTextQuery(final String query, final String languageCode,
	            final boolean prefixMatch) {
        	this(
    			query, 
    			languageCode,
    			prefixMatch,
    			null,
    			BDS.DEFAULT_MATCH_ALL_TERMS,
    			BDS.DEFAULT_MATCH_EXACT,
    			BDS.DEFAULT_MIN_RELEVANCE,
    			BDS.DEFAULT_MAX_RELEVANCE,
    			BDS.DEFAULT_MIN_RANK,
    			BDS.DEFAULT_MAX_RANK,
    			BDS.DEFAULT_TIMEOUT,
    			TimeUnit.MILLISECONDS
    			);
        }
        
        public FullTextQuery(final String query, final String languageCode,
	            final boolean prefixMatch, final String matchRegex, 
	            final boolean matchAllTerms, final boolean matchExact) {
        	this(
    			query, 
    			languageCode,
    			prefixMatch,
    			matchRegex,
    			matchAllTerms,
    			matchExact,
    			BDS.DEFAULT_MIN_RELEVANCE,
    			BDS.DEFAULT_MAX_RELEVANCE,
    			BDS.DEFAULT_MIN_RANK,
    			BDS.DEFAULT_MAX_RANK,
    			BDS.DEFAULT_TIMEOUT,
    			TimeUnit.MILLISECONDS
    			);
        }
        
        public FullTextQuery(final String query, final String languageCode,
	            final boolean prefixMatch, final String matchRegex, 
	            final boolean matchAllTerms, final boolean matchExact,
	            final double minCosine, final double maxCosine,
	            final int minRank, final int maxRank) {
        	this(
    			query, 
    			languageCode,
    			prefixMatch,
    			matchRegex,
    			matchAllTerms,
    			matchExact,
    			minCosine,
    			maxCosine,
    			minRank,
    			maxRank,
    			BDS.DEFAULT_TIMEOUT,
    			TimeUnit.MILLISECONDS
    			);
        }
        
		/**
		 * Construct a full text query.
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
		 * @param matchRegex
		 * 			  A regex filter to apply to the search.            
		 * @param matchAllTerms
		 *            if true, return only hits that match all search terms
		 * @param matchExact
		 *            if true, return only hits that have an exact match of the search string
		 * @param minCosine
		 *            The minimum cosine that will be returned (in [0:maxCosine]).
		 *            If you specify a minimum cosine of ZERO (0.0) you can drag in
		 *            a lot of basically useless search results.
		 * @param maxCosine
		 *            The maximum cosine that will be returned (in [minCosine:1.0]).
		 *            Useful for evaluating in relevance ranges.
		 * @param minRank
		 *            The min rank of the search result.
		 * @param maxRank
		 *            The max rank of the search result.
		 * @param timeout
		 *            The timeout -or- ZERO (0) for NO timeout (this is equivalent
		 *            to using {@link Long#MAX_VALUE}).
		 * @param unit
		 *            The unit in which the timeout is expressed.
		 */
		public FullTextQuery(final String query, final String languageCode,
	            final boolean prefixMatch, final String matchRegex, 
	            final boolean matchAllTerms, final boolean matchExact, 
	            final double minCosine, final double maxCosine,
	            final int minRank, final int maxRank, 
	            long timeout, final TimeUnit unit) {
			
			this.query = query;
			this.languageCode = languageCode;
			this.prefixMatch = prefixMatch;
			this.matchRegex = matchRegex;
			this.matchAllTerms = matchAllTerms;
			this.matchExact = matchExact;
			this.minCosine = minCosine;
			this.maxCosine = maxCosine;
			this.minRank = minRank;
			this.maxRank = maxRank;
			this.timeout = timeout;
			this.unit = unit;
			
		}
		
		/**
		 * @return the query
		 */
		public String getQuery() {
			return query;
		}

		/**
		 * @return the languageCode
		 */
		public String getLanguageCode() {
			return languageCode;
		}

		/**
		 * @return the prefixMatch
		 */
		public boolean isPrefixMatch() {
			return prefixMatch;
		}

		/**
		 * @return the match regex filter to apply
		 */
		public String getMatchRegex() {
			return matchRegex;
		}

		/**
		 * @return the matchAllTerms
		 */
		public boolean isMatchAllTerms() {
			return matchAllTerms;
		}

		/**
		 * @return the matchExact
		 */
		public boolean isMatchExact() {
			return matchExact;
		}

		/**
		 * @return the minCosine
		 */
		public double getMinCosine() {
			return minCosine;
		}

		/**
		 * @return the maxCosine
		 */
		public double getMaxCosine() {
			return maxCosine;
		}

		/**
		 * @return the minRank
		 */
		public int getMinRank() {
			return minRank;
		}

		/**
		 * @return the maxRank
		 */
		public int getMaxRank() {
			return maxRank;
		}

		/**
		 * @return the timeout
		 */
		public long getTimeout() {
			return timeout;
		}

		/**
		 * @return the unit
		 */
		public TimeUnit getTimeUnit() {
			return unit;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((languageCode == null) ? 0 : languageCode.hashCode());
			result = prime * result + (matchAllTerms ? 1231 : 1237);
			result = prime * result + (matchExact ? 1231 : 1237);
			result = prime * result + (prefixMatch ? 1231 : 1237);
			result = prime * result + ((query == null) ? 0 : query.hashCode());
			result = prime * result + ((matchRegex == null) ? 0 : matchRegex.hashCode());
			return result;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			FullTextQuery other = (FullTextQuery) obj;
			if (languageCode == null) {
				if (other.languageCode != null)
					return false;
			} else if (!languageCode.equals(other.languageCode))
				return false;
			if (matchAllTerms != other.matchAllTerms)
				return false;
			if (matchExact != other.matchExact)
				return false;
			if (prefixMatch != other.prefixMatch)
				return false;
			if (query == null) {
				if (other.query != null)
					return false;
			} else if (!query.equals(other.query))
				return false;
			if (matchRegex == null) {
				if (other.matchRegex != null)
					return false;
			} else if (!matchRegex.equals(other.matchRegex))
				return false;
			return true;
		}

    }

}
