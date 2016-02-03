/*

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
 * Created on Apr 12, 2008
 */

package com.bigdata.rdf.store;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.sparql.ast.eval.SliceServiceFactory;


/**
 * A vocabulary for the bigdata full text search facility. Full text search may
 * be used to combine text search and graph search. Low-latency, user facing
 * search applications may be created by slicing the full text search results
 * and feeding them incrementally into SPARQL queries. This approach allows the
 * application to manage the cost of the SPARQL query by bounding the input. If
 * necessary, additional results can be feed into the query. 
 * 
 * @see SliceServiceFactory
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
     * The name of a magic predicate recognized in SPARQL queries when it occurs
     * in statement patterns such as:
     * 
     * <pre>
     * 
     * ( s?, bigdata:search, &quot;scale-out RDF triplestore&quot; )
     * 
     * </pre>
     * 
     * The value MUST be bound and MUST be a literal. The
     * <code>languageCode</code> attribute is permitted. When specified, the
     * <code>languageCode</code> attribute will be used to determine how the
     * literal is tokenized - it does not filter for matches marked with that
     * <code>languageCode</code> attribute. The <code>datatype</code> attribute
     * is not allowed.
     * <p>
     * The subject MUST NOT be bound. 
     * <p>
     * 
     * This expression will evaluate to a set of bindings for the subject
     * position corresponding to the indexed literals matching any of the terms
     * obtained when the literal was tokenized.
     * 
     * <p>
     * Note: The context position should be unbound when using statement
     * identifiers.
     */
    final URI SEARCH = new URIImpl(NAMESPACE + "search");
    
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
     *   ?s bds:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bds:relevance ?relevance .
     * }
     * 
     * </pre>
     * 
     * Relevance is the cosine of the angle between the query vector (search
     * terms) and the document vector (terms in the indexed literals). The
     * minimum relevance is ZERO (0.0). The maximum relevance is ONE (1.0).
     * 
     * @see #MIN_RELEVANCE
     * @see #MAX_RELEVANCE
     */
    final URI RELEVANCE = new URIImpl(NAMESPACE + "relevance");
    
    /**
     * Magic predicate used to query for free text search metadata, reporting
     * the rank (origin ONE (1)) of the search result amoung the search results
     * obtained for the search query. The rank is from ONE to N, where N is the
     * number of search results from the full text index. {@link #MIN_RANK} and
     * {@link #MAX_RANK} may be used to "slice" the full text index search
     * results. Use this query hint conjunction with {@link #SEARCH} as follows:
     * <p>
     * 
     * <pre>
     * 
     * select ?s ?rank
     * where {
     *   ?s bds:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bds:rank ?rank .
     * }
     * 
     * </pre>
     * 
     * @see #MIN_RANK
     * @see #MAX_RANK
     */
    final URI RANK = new URIImpl(NAMESPACE + "rank");
    
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
     *   ?s bds:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bds:maxRank "5"^^xsd:int .
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
    final URI MAX_RANK = new URIImpl(NAMESPACE + "maxRank");

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
     *   ?s bds:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bds:minRank "5"^^xsd:int .
     * }
     * 
     * </pre>
     * 
     * The default is {@value #DEFAULT_MIN_RANK}.
     */
    final URI MIN_RANK = new URIImpl(NAMESPACE + "minRank");

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
     *   ?s bds:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bds:minRelevance "0.5"^^xsd:double .
     * }
     * 
     * </pre>
     * 
     * The relevance scores are in [0.0:1.0], where 0.0 is the minimum possible
     * relevance and 1.0 is the maximum possible relevance. You should NOT
     * specify a minimum relevance of ZERO (0.0) as this can drag in way too
     * many unrelated results. The default is {@value #DEFAULT_MIN_RELEVANCE}.
     */
    final URI MIN_RELEVANCE = new URIImpl(NAMESPACE + "minRelevance");

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
     *   ?s bds:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bds:maxRelevance "0.9"^^xsd:double .
     * }
     * 
     * </pre>
     * 
     * The relevance scores are in [0.0:1.0], where 0.0 is the minimum possible
     * relevance and 1.0 is the maximum possible relevance. You should NOT
     * specify a minimum relevance of ZERO (0.0) as this can drag in way too
     * many unrelated results. The default maximum relevance is
     * {@value #DEFAULT_MAX_RELEVANCE}.
     */
    final URI MAX_RELEVANCE = new URIImpl(NAMESPACE + "maxRelevance");

    /**
     * The default value for {@link #MAX_RELEVANCE} unless overridden.
     */
    final double DEFAULT_MAX_RELEVANCE = 1.0d;

    /**
     * Magic predicate used to query for free text search metadata indicates
     * that all terms in the query must be found within a given literal in order
     * for that literal to "match" the query (default
     * {@value #DEFAULT_MATCH_ALL_TERMS}). Use in conjunction with
     * {@link #SEARCH} as follows:
     * <p>
     * 
     * <pre>
     * 
     * select ?s
     * where {
     *   ?s bds:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bds:matchAllTerms "true" .
     * }
     * 
     * </pre>
     */
    final URI MATCH_ALL_TERMS = new URIImpl(NAMESPACE + "matchAllTerms");

    final boolean DEFAULT_MATCH_ALL_TERMS = false;
    
    /**
     * Magic predicate used to query for free text search metadata indicates
     * that only exact string matches will be reported (the literal must contain
     * the search string). Use in conjunction with {@link #SEARCH} as follows:
     * <p>
     * 
     * <pre>
     * 
     * select ?s
     * where {
     *   ?s bds:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bds:matchExact "true" .
     * }
     * 
     * </pre>
     * <p>
     * This operation will be rather expensive as it will require materializing
     * all the hits to check their values.
     */
    final URI MATCH_EXACT = new URIImpl(NAMESPACE + "matchExact");

    final boolean DEFAULT_MATCH_EXACT = false;
    
    /**
     * Magic predicate used to query for free text search metadata indicates
     * that only search results that also pass the specified REGEX filter will
     * be reported. Use in conjunction with {@link #SEARCH} as follows:
     * <p>
     * 
     * <pre>
     * 
     * select ?s
     * where {
     *   ?s bds:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bds:matchRegex &quot;regex to apply to ?s bindings&quot; .
     * }
     * 
     * </pre>
     * <p>
     * This operation will be rather expensive as it will require materializing
     * all the hits to check their values.
     */
    final URI MATCH_REGEX = new URIImpl(NAMESPACE + "matchRegex");

    final String DEFAULT_MATCH_REGEX = null;
    
    /**
     * 
     * <strong>Prefix matching is now indicated using a wildcard</strong>
     * 
     * <pre>
     * PREFIX bds: <http://www.bigdata.com/rdf/search#>
     * 
     * SELECT ?subj ?label 
     * WHERE {
     *       ?label bds:search "mi*" .
     *       ?label bds:relevance ?cosine .
     *       ?subj ?p ?label .
     * }
     * </pre>
     * 
     * <strong>The following approach is no longer supported. </strong>
     * 
     * Magic predicate used to query for free text search metadata to turn on
     * prefix matching. Prefix matching will match all full text index tokens
     * that begin with the specified token(s) (default
     * {@value #DEFAULT_PREFIX_MATCH}). Use in conjunction with {@link #SEARCH}
     * as follows:
     * <p>
     * 
     * <pre>
     * 
     * select ?s
     * where {
     *   ?s bds:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bds:prefixMatch &quot;true&quot; .
     * }
     * 
     * </pre>
     * <p>
     * This will turn on prefix matching.
     * 
     * @deprecated Prefix matching is now invoked using a wildcard.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/803" >
     *      prefixMatch does not work in full text search </a>
     */
    @Deprecated
    final URI PREFIX_MATCH = new URIImpl(NAMESPACE + "prefixMatch");

    /**
     * @deprecated This option is now invoked using a wildcard.
     */
    final boolean DEFAULT_PREFIX_MATCH = false;
    
    /**
	 * Magic predicate used to query for free text search metadata. Use in
	 * conjunction with {@link #SEARCH} as follows:
	 * <p>
	 * 
	 * <pre>
	 * 
	 * select ?s
	 * where {
	 *   ?s bds:search &quot;scale-out RDF triplestore&quot; .
	 *   ?s bds:subjectSearch "true" .
	 * }
	 * 
	 * </pre>
	 * <p>
	 * The subject-centric search index must be enabled via
	 * {@link AbstractTripleStore.Options#SUBJECT_CENTRIC_TEXT_INDEX}.
	 * 
	 * @deprecated Feature was never completed due to scalability issues. See
	 *             BZLG-1548, BLZG-563.
	 */
    @Deprecated
    final URI SUBJECT_SEARCH = new URIImpl(NAMESPACE + "subjectSearch");

    @Deprecated
    final boolean DEFAULT_SUBJECT_SEARCH = false;
    
    /**
     * Magic predicate used for the "search in search" service. Also serves as
     * the identifier for the service itself.
     */
    final URI SEARCH_IN_SEARCH = new URIImpl(NAMESPACE + "searchInSearch");

    /**
     * Magic predicate used to query for free text search metadata to set a
     * deadline in milliseconds on the full text index search (
     * {@value #DEFAULT_TIMEOUT}). Use in conjunction with {@link #SEARCH} as
     * follows:
     * <p>
     * 
     * <pre>
     * 
     * select ?s
     * where {
     *   ?s bds:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bds:searchTimeout "5000" .
     * }
     * 
     * </pre>
     * <p>
     * Timeout specified in milliseconds.
     */
    final URI SEARCH_TIMEOUT = new URIImpl(NAMESPACE + "searchTimeout");

    /**
     * The default timeout for a free text search (milliseconds).
     */
    final long DEFAULT_TIMEOUT = Long.MAX_VALUE;
    
    /**
     * Magic predicate to specify that we want a range count done on the search.
     * Bind the range count to the variable in the object position.  Will
     * attempt to do a fast range count on the index rather than materializing
     * the hits into an array.  This is only possible if matchExact == false
     * and matchRegex == null.
     */
    final URI RANGE_COUNT = new URIImpl(NAMESPACE + "rangeCount");

}
