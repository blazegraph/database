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
 * Created on Apr 12, 2008
 */

package com.bigdata.rdf.store;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;

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
    final String NAMESPACE = "http://www.bigdata.com/rdf#";
    
    final String SEARCH_NAMESPACE = "http://www.bigdata.com/rdf/search#";
    
    /**
     * The namespace prefix used in SPARQL queries to signify query hints.  You
     * can embed query hints into a SPARQL query as follows:
     * <code>
     * PREFIX BIGDATA_QUERY_HINTS: &lt;http://www.bigdata.com/queryHints#com.bigdata.relation.rule.eval.DefaultRuleTaskFactory.nestedSubquery=true&amp;com.bigdata.fullScanTreshold=1000&gt;
     * </code>
     */
    String QUERY_HINTS_NAMESPACE = "BIGDATA_QUERY_HINTS";

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
    final URI SEARCH = new URIImpl(SEARCH_NAMESPACE+"search");
    
    final URI RELEVANCE = new URIImpl(SEARCH_NAMESPACE+"relevance");
    
    final URI RANK = new URIImpl(SEARCH_NAMESPACE+"rank");
    
    final URI NUM_MATCHED_TOKENS = new URIImpl(SEARCH_NAMESPACE+"numMatchedTokens");

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
     */
    URI NULL_GRAPH = new URIImpl(NAMESPACE + "nullGraph");
    
    /**
     * We need the abiltiy to do atomic add+drop in one operation via the
     * remoting interface.  Thus we need the ability to place 
     * statements to add and to delete in the same serialized document sent
     * across the wire.  This separator key, when included in a comment, will
     * mark the separation point between statements to drop (above the 
     * separator) and statements to add (below the separator).
     */
    URI ATOMIC_UPDATE_SEPARATOR_KEY = new URIImpl(NAMESPACE + "atomicUpdateSeparatorKey");
    
}
