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

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.SESAME;

import com.bigdata.rdf.sparql.ast.cache.DescribeServiceFactory;


/**
 * A vocabulary for bigdata specific extensions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface BD {

    /**
     * The namespace used for bigdata specific extensions.
     */
    String NAMESPACE = "http://www.bigdata.com/rdf#";
    
    /**
     * The namespace used for magic search predicates.
     * <p>
     * @see #SEARCH
     * @see #RELEVANCE
     * @see #RANK
     * @see #NUM_MATCHED_TOKENS
     */
    final String SEARCH_NAMESPACE = "http://www.bigdata.com/rdf/search#";

    /**
     * The name of a per-statement attribute whose value is recognized in
     * RDF/XML as the statement identifier for statement described by the
     * element on which it appears. The <code>bigdata:sid</code> attribute is
     * allowed on elements describing RDF statements and serves as a
     * per-statement identifier. The value of the <code>bigdata:sid</code>
     * attribute must conform to the syntactic constraints of a blank node. By
     * re-using the value of the <code>bigdata:sid</code> attribute within
     * other statements in the same RDF/XML document, the client can express
     * statements about the statements.
     * <p>
     * This RDF/XML extension allows us to inline provenance (statements about
     * statements) with RDF/XML to clients in a manner which has minor impact on
     * unaware clients (they will perceive additional statements whose predicate
     * is <code>bigdata:sid</code>). In addition, clients aware of this
     * extension can submit RDF/XML with inline provenance.
     * <p>
     * For example:
     * 
     * <pre>
     * &lt;rdf:Description rdf:about=&quot;http://www.foo.org/A&quot;&gt;
     *     &lt;label bigdata:sid=&quot;_S67&quot; xmlns=&quot;http://www.w3.org/2000/01/rdf-schema#&quot;&gt;abc&lt;/label&gt;
     * &lt;/rdf:Description&gt;
     * </pre>
     */
    URI SID = new URIImpl(NAMESPACE+"sid");
    
    /**
     * The name of a per-statement attribute whose indicates whether the
     * statement is an axiom, inference, or explicit in the knowledge base. This
     * attribute is strictly informative for clients and is ignored when loading
     * data into a knowledge base.
     * <p>
     * The value will be one of
     * <dl>
     * <dt>Axiom</dt>
     * <dd>The statement is an axiom that has not been explicitly asserted.</dd>
     * <dt>Inferred</dt>
     * <dd>The statement is an inference that has not been explicitly asserted.</dd>
     * <dt>Explicit</dt>
     * <dd>The statement has been explicitly asserted.</dd>
     * </dl>
     * Note: If the knowledge base supports truth maintenance and an explicit
     * statement is deleted from the knowledge base but it remains provable as
     * an inference or an axiom then then knowledge base will continue to report
     * it as either an axiom or an inference as appropriate.
     */
    URI STATEMENT_TYPE = new URIImpl(NAMESPACE+"statementType");
    
    /**
     * The URI for the "DESCRIBE" service.
     * 
     * @see DescribeServiceFactory
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/584">
     *      Describe Cache </a>
     */
    URI DESCRIBE = new URIImpl(NAMESPACE + "describe");

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
    final URI SEARCH = new URIImpl(SEARCH_NAMESPACE+"search");
    
    /**
     * Magic predicate used to query for free text search metadata.  Use 
     * in conjunction with {@link #SEARCH} as follows:
     * <p>
     * <pre>
     * 
     * select ?s ?relevance
     * where {
     *   ?s bd:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bd:relevance ?relevance .
     * }
     * 
     * </pre>
     */
    final URI RELEVANCE = new URIImpl(SEARCH_NAMESPACE+"relevance");
    
    /**
     * Magic predicate used to query for free text search metadata.  Use 
     * in conjunction with {@link #SEARCH} as follows:
     * <p>
     * <pre>
     * 
     * select ?s ?rank
     * where {
     *   ?s bd:search &quot;scale-out RDF triplestore&quot; .
     *   ?s bd:rank ?rank .
     * }
     * 
     * </pre>
     */
    final URI RANK = new URIImpl(SEARCH_NAMESPACE+"rank");
    
    /**
     * Magic predicate used to query for free text search metadata.  Use 
     * in conjunction with {@link #SEARCH} as follows:
     * <p>
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
     * The default is {@value #DEFAULT_MAX_RANK}.
     */
    final URI MAX_RANK = new URIImpl(SEARCH_NAMESPACE+"maxRank");

    /**
     * The default for {@link #MAX_RANK}.
     */
    final int DEFAULT_MAX_RANK = Integer.MAX_VALUE;
    
    /**
     * Magic predicate used to query for free text search metadata.  Use 
     * in conjunction with {@link #SEARCH} as follows:
     * <p>
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
    final URI MIN_RANK = new URIImpl(SEARCH_NAMESPACE+"minRank");

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
    final URI MIN_RELEVANCE = new URIImpl(SEARCH_NAMESPACE+"minRelevance");

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
    final URI MAX_RELEVANCE = new URIImpl(SEARCH_NAMESPACE+"maxRelevance");

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
    final URI MATCH_ALL_TERMS = new URIImpl(SEARCH_NAMESPACE+"matchAllTerms");

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
    final URI MATCH_EXACT = new URIImpl(SEARCH_NAMESPACE+"matchExact");

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
    final URI MATCH_REGEX = new URIImpl(SEARCH_NAMESPACE+"matchRegex");

    final String DEFAULT_MATCH_REGEX = null;
    
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
    final URI SUBJECT_SEARCH = new URIImpl(SEARCH_NAMESPACE+"subjectSearch");

    final boolean DEFAULT_SUBJECT_SEARCH = false;
    
    /**
     * Magic predicate used for the "search in search" service.  Also serves
     * as the identifier for the service itself.
     */
    final URI SEARCH_IN_SEARCH = new URIImpl(SEARCH_NAMESPACE+"searchInSearch");
    
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
    final URI SEARCH_TIMEOUT = new URIImpl(SEARCH_NAMESPACE+"searchTimeout");
    
    /**
     * The default timeout for a free text search (milliseconds).
     */
    final long DEFAULT_TIMEOUT = Long.MAX_VALUE;
    
    /**
     * Sesame has the notion of a "null" graph. Any time you insert a statement
     * into a quad store and the context position is not specified, it is
     * actually inserted into this "null" graph. If SPARQL <code>DATASET</code>
     * is not specified, then all contexts are queried and you will see
     * statements from the "null" graph as well as from any other context.
     * {@link com.bigdata.rdf.sail.BigdataSailConnection#getStatements(Resource, URI, Value, boolean, Resource...)}
     * will return statements from the "null" graph if the context is either
     * unbound or is an array whose sole element is <code>null</code>.
     * 
     * @see com.bigdata.rdf.sail.BigdataSailConnection#addStatement(Resource, URI, Value, Resource...)
     * @see com.bigdata.rdf.sail.BigdataSailConnection#getStatements(Resource, URI, Value, boolean, Resource...)
     * @see SESAME#NIL
     */
    URI NULL_GRAPH = new URIImpl(NAMESPACE + "nullGraph");

    /**
     * <p>
     * A predicate used to model the membership of a virtual graph. The
     * following assertions declare the membership of a virtual graph as the
     * graphs (:g1,:g2).
     * </p>
     * 
     * <pre>
     * :vg bd:virtualGraph :g1
     * :vg bd:virtualGraph :g2
     * </pre>
     * 
     * </p>
     * <p>
     * Virtual graphs are addressed through a SPARQL syntax extension:
     * </p>
     * 
     * <pre>
     * FROM VIRTUAL GRAPH
     * FROM NAMED VIRTUAL GRAPH
     * </pre>
     * 
     * <p>
     * If <code>:vg</code> as declared above is addressed using
     * <code>FROM VIRTUAL GRAPH</code> then its member graphs (:g1,:g2) are
     * added to the default graph for the query.
     * </p>
     * <p>
     * If <code>:vg</code> as declared above is addressed using
     * <code>FROM NAMED VIRTUAL GRAPH</code> then its member graphs (:g1,:g2)
     * are added to the named graphs for the query.
     * </p>
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/493">
     *      Virtual Graphs (ticket)</a>
     * @see <a
     *      href="https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=VirtualGraphs">
     *      Virtual Graphs (wiki)</a>
     */
    URI VIRTUAL_GRAPH = new URIImpl(NAMESPACE + "virtualGraph");
    
//    /**
//     * We need the ability to do atomic add+drop in one operation via the
//     * remoting interface.  Thus we need the ability to place 
//     * statements to add and to delete in the same serialized document sent
//     * across the wire.  This separator key, when included in a comment, will
//     * mark the separation point between statements to drop (above the 
//     * separator) and statements to add (below the separator).
//     */
//    URI ATOMIC_UPDATE_SEPARATOR_KEY = new URIImpl(NAMESPACE + "atomicUpdateSeparatorKey");
    
}
