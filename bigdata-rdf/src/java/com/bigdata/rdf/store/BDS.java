/*

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Apr 12, 2008
 */

package com.bigdata.rdf.store;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;


/**
 * A vocabulary for bigdata specific extensions.
 * 
 * @see <a
 *      href="http://sourceforge.net/apps/mediawiki/bigdata/index.php?title=FullTextSearch">
 *      Free Text Index </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: BD.java 6786 2012-12-19 18:43:27Z thompsonbry $
 */
public interface BDS {

    /**
     * The namespace used for magic search predicates.
     * <p>
     * @see #SEARCH
     * @see #RELEVANCE
     * @see #RANK
     * @see #NUM_MATCHED_TOKENS
     */
    final String NAMESPACE = "http://www.bigdata.com/rdf/search#";

    /**
     * The name of a magic predicate recognized by the {@link com.bigdata.rdf.sail.BigdataSail} when
     * it occurs in statement patterns such as:
     * 
     * <pre>
     * 
     * ( s?, bigdata:search, &quot;scale-out RDF triplestore&quot; )
     * 
     * </pre>
     * 
     * The value MUST be bound and MUST be a literal. The languageCode attribute
     * is permitted, but a datatype attribute is not allowed. When specified,
     * the languageCode attribute will be used to determine how the literal is
     * tokenized - it does not filter for matches marked with that languageCode
     * attribute.
     * <p>
     * The subject MUST NOT be bound.
     * <p>
     * 
     * This expression will evaluate to a set of bindings for the subject
     * position corresponding to the indexed literals matching any of the
     * terms obtained when the literal was tokenized.
     * 
     * <p>
     * Note: The context position should be unbound when using statement
     * identifiers.
     */
//    URI SEARCH = new URIImpl(NAMESPACE+"search");
    final URI SEARCH = new URIImpl(NAMESPACE+"search");
    
    /**
     * Magic predicate used to query for free text search metadata, reporting
     * the relevance of the search result to the search query. Use in
     * conjunction with {@link #SEARCH} as follows:
     * <p>
     * 
     * <pre>
     * 
     * select ?s ?relevance
     * where {
     *   ?s bd:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bd:relevance ?relevance .
     * }
     * 
     * </pre>
     * 
     * @see #MIN_RELEVANCE
     * @see #MAX_RELEVANCE
     */
    final URI RELEVANCE = new URIImpl(NAMESPACE+"relevance");
    
    /**
     * Magic predicate used to query for free text search metadata, reporting
     * the rank (origin ONE (1)) of the search result amoung the search results
     * obtained for the search query. The rank is from ONE to N, where N is the
     * number of search results from the full text index. Use in conjunction
     * with {@link #SEARCH} as follows:
     * <p>
     * 
     * <pre>
     * 
     * select ?s ?rank
     * where {
     *   ?s bd:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bd:rank ?rank .
     * }
     * 
     * </pre>
     * 
     * @see #MIN_RANK
     * @see #MAX_RANK
     */
    final URI RANK = new URIImpl(NAMESPACE+"rank");
    
    /**
     * Magic predicate used to limit the maximum rank of the free text search
     * results to the specified value (default {@value #DEFAULT_MAX_RANK)}. Use
     * in conjunction with {@link #SEARCH} as follows:
     * <p>
     * 
     * <pre>
     * 
     * select ?s
     * where {
     *   ?s bd:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bd:maxRank "5"^^xsd:int .
     * }
     * 
     * </pre>
     * 
     * You can use {@link #MIN_RANK} and {@link #MAX_RANK} together to page
     * through the search results. This is often key to achieving low latency
     * graph search. By limiting the number of results that are fed into the
     * remained of the SPARQL query, you can ensure that the SPARQL query runs
     * quickly. If you do not get enough results from the SPARQL query, you can
     * feed the next "page" of free text results by changing the values for the
     * {@link #MIN_RANK} AND {@link #MAX_RANK} query hints.
     */
    final URI MAX_RANK = new URIImpl(NAMESPACE+"maxRank");

    /**
     * The default for {@link #MAX_RANK}.
     */
    final int DEFAULT_MAX_RANK = Integer.MAX_VALUE;
    
    /**
     * Magic predicate used to limit the minimum rank of the free text search
     * results to the specified value (default {@value #DEFAULT_MIN_RANK}). Use
     * in conjunction with {@link #SEARCH} as follows:
     * <p>
     * 
     * <pre>
     * 
     * select ?s
     * where {
     *   ?s bd:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bd:minRank "5"^^xsd:int .
     * }
     * 
     * </pre>
     * 
     * The default is {@value #DEFAULT_MIN_RANK}.
     */
    final URI MIN_RANK = new URIImpl(NAMESPACE+"minRank");

    /**
     * The default for {@link #MIN_RANK} is 1, full text search results will
     * start with the #1 most relevant hit by default.
     */
    final int DEFAULT_MIN_RANK = 1;
    
	/**
	 * Magic predicate used to query for free text search metadata. Use in
	 * conjunction with {@link #SEARCH} as follows:
	 * <p>
	 * 
	 * <pre>
	 * 
	 * select ?s
	 * where {
	 *   ?s bd:search &quot;scale-out RDF triplestore&quot; .
	 *   ?s bd:minRelevance "0.5"^^xsd:double .
	 * }
	 * 
	 * </pre>
	 * 
	 * The relevance scores are in [0.0:1.0]. You should NOT specify a minimum
	 * relevance of ZERO (0.0) as this can drag in way too many unrelated
	 * results. The default is {@value #DEFAULT_MIN_RELEVANCE}.
	 */
    final URI MIN_RELEVANCE = new URIImpl(NAMESPACE+"minRelevance");

    final double DEFAULT_MIN_RELEVANCE = 0.0d;

	/**
	 * Magic predicate used to query for free text search metadata. Use in
	 * conjunction with {@link #SEARCH} as follows:
	 * <p>
	 * 
	 * <pre>
	 * 
	 * select ?s
	 * where {
	 *   ?s bd:search &quot;scale-out RDF triplestore&quot; .
	 *   ?s bd:maxRelevance "0.9"^^xsd:double .
	 * }
	 * 
	 * </pre>
	 * 
	 * The relevance scores are in [0.0:1.0]. The default maximum relevance is
	 * {@value #DEFAULT_MAX_RELEVANCE}.
	 */
    final URI MAX_RELEVANCE = new URIImpl(NAMESPACE+"maxRelevance");

    /**
     * The default value for {@link #MAX_RELEVANCE} unless overridden. 
     */
    final double DEFAULT_MAX_RELEVANCE = 1.0d;

    /**
     * Magic predicate used to query for free text search metadata.  Use 
     * in conjunction with {@link #SEARCH} as follows:
     * <p>
     * <pre>
     * 
     * select ?s
     * where {
     *   ?s bd:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bd:matchAllTerms "true" .
     * }
     * 
     * </pre>
     */
    final URI MATCH_ALL_TERMS = new URIImpl(NAMESPACE+"matchAllTerms");

    final boolean DEFAULT_MATCH_ALL_TERMS = false;
    
    /**
     * Magic predicate used to query for free text search metadata.  Use 
     * in conjunction with {@link #SEARCH} as follows:
     * <p>
     * <pre>
     * 
     * select ?s
     * where {
     *   ?s bd:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bd:matchExact "true" .
     * }
     * 
     * </pre>
     * <p>
     * This operation will be rather expensive as it will require materializing
     * all the hits to check their values.
     */
    final URI MATCH_EXACT = new URIImpl(NAMESPACE+"matchExact");

    final boolean DEFAULT_MATCH_EXACT = false;
    
    /**
     * Magic predicate used to query for free text search metadata.  Use 
     * in conjunction with {@link #SEARCH} as follows:
     * <p>
     * <pre>
     * 
     * select ?s
     * where {
     *   ?s bd:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bd:matchRegex &quot;regex to apply to ?s bindings&quot; .
     * }
     * 
     * </pre>
     * <p>
     * This operation will be rather expensive as it will require materializing
     * all the hits to check their values.
     */
    final URI MATCH_REGEX = new URIImpl(NAMESPACE+"matchRegex");

    final String DEFAULT_MATCH_REGEX = null;
    
    /**
     * Magic predicate used to query for free text search metadata.  Use 
     * in conjunction with {@link #SEARCH} as follows:
     * <p>
     * <pre>
     * 
     * select ?s
     * where {
     *   ?s bd:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bd:prefixMatch &quot;true&quot; .
     * }
     * 
     * </pre>
     * <p>
     * This will turn on prefix matching.
     */
    final URI PREFIX_MATCH = new URIImpl(NAMESPACE+"prefixMatch");
    
    final boolean DEFAULT_PREFIX_MATCH = false;
    
    /**
     * Magic predicate used to query for free text search metadata.  Use 
     * in conjunction with {@link #SEARCH} as follows:
     * <p>
     * <pre>
     * 
     * select ?s
     * where {
     *   ?s bd:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bd:subjectSearch "true" .
     * }
     * 
     * </pre>
     * <p>
     * The subject-centric search index must be enabled via
     * {@link AbstractTripleStore.Options#SUBJECT_CENTRIC_TEXT_INDEX}.
     */
    final URI SUBJECT_SEARCH = new URIImpl(NAMESPACE+"subjectSearch");

    final boolean DEFAULT_SUBJECT_SEARCH = false;
    
    /**
     * Magic predicate used for the "search in search" service.  Also serves
     * as the identifier for the service itself.
     */
    final URI SEARCH_IN_SEARCH = new URIImpl(NAMESPACE+"searchInSearch");
    
    /**
     * Magic predicate used to query for free text search metadata.  Use 
     * in conjunction with {@link #SEARCH} as follows:
     * <p>
     * <pre>
     * 
     * select ?s
     * where {
     *   ?s bd:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bd:searchTimeout "5000" .
     * }
     * 
     * </pre>
     * <p>
     * Timeout specified in milliseconds.
     */
    final URI SEARCH_TIMEOUT = new URIImpl(NAMESPACE+"searchTimeout");
    
    /**
     * The default timeout for a free text search (milliseconds).
     */
    final long DEFAULT_TIMEOUT = Long.MAX_VALUE;
    
}
