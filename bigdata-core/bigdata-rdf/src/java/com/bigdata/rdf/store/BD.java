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

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.SESAME;

import com.bigdata.rdf.sparql.ast.cache.DescribeServiceFactory;
import com.bigdata.rdf.sparql.ast.eval.ServiceParams;
import com.bigdata.rdf.sparql.ast.optimizers.ASTALPServiceOptimizer;


/**
 * A vocabulary for bigdata specific extensions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface BD {

    /**
     * The namespace used for bigdata specific extensions.
     */
    String NAMESPACE = "http://www.bigdata.com/rdf#";
    
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
    * Sesame has the notion of a "null" graph. Any time you insert a statement
    * into a quad store and the context position is not specified, it is
    * actually inserted into this "null" graph. If SPARQL <code>DATASET</code>
    * is not specified, then all contexts are queried and you will see
    * statements from the "null" graph as well as from any other context.
    * {@link com.bigdata.rdf.sail.BigdataSailConnection#getStatements(Resource, URI, Value, boolean, Resource...)}
    * will return statements from the "null" graph if the context is either
    * unbound or is an array whose sole element is <code>null</code>.
    * <p>
    * Given
    * 
    * <pre>
    * void foo(Resource s, final URI p, final Value o, final Resource... contexts)
    * </pre>
    * 
    * <dl>
    * <dt>foo(s,p,o)</dt>
    * <dd>All named graphs are addressed. The <i>contexts</i> parameter will be
    * a Resource[0] reference.</dd>
    * <dt>foo(s,p,o,(Resource[]) null)</dt>
    * <dd>
    * <em>Note: openrdf does not allow this invocation pattern - the Resource[] MUST NOT be a <code>null</code> reference.</em>
    * </dd>
    * <dt>foo(s,p,o,(Resource) null)</dt>
    * <dd>The openrdf "nullGraph" is addressed. The <i>contexts</i> parameter
    * will be Resource[]{null}. Java will autobox the Resource reference as a
    * Resource[]{null} array.</dd>
    * <dt>foo(s,p,o,x,y,z)</dt>
    * <dd>The openrdf named graphs (x,y,z) are addressed. The <i>contexts</i>
    * parameter will be Resource[]{x,y,z}.</dd>
    * <dt>foo(s,p,o,x,null,z)</dt>
    * <dd>The openrdf named graphs (x,nullGraph,z) are addressed. The
    * <i>contexts</i> parameter will be Resource[]{x,null,z}</dd>
    * </dd>
    * </dl>
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1177"> Resource... contexts
    *      not encoded/decoded according to openrdf semantics (REST API) </a>
    * 
    * @see com.bigdata.rdf.sail.BigdataSailConnection#addStatement(Resource,
    *      URI, Value, Resource...)
    * @see com.bigdata.rdf.sail.BigdataSailConnection#getStatements(Resource,
    *      URI, Value, boolean, Resource...)
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
    
    /**
     * URI that can be used as the Subject of magic triple patterns for bigdata
     * SERVICEs. There may be zero or more such triple patterns. The Predicate
     * (key) and Object (val) positions for those triple patterns are extracted
     * into a {@link ServiceParams} object. For each key, there may be one or
     * more values.
     * 
     * <pre>
     * SERVICE <uri> {
     *   bd:serviceParam :key1 :val1 .
     *   bd:serviceParam :key1 :val2 .
     *   bd:serviceParam :key2 :val3 .
     * }
     * </pre>
     * 
     * @see ServiceParams.
     */
    URI SERVICE_PARAM = new URIImpl(NAMESPACE + "serviceParam");
    
    /**
    * The well-known URI of the ALP SERVICE extension.
    * 
    * @see ASTALPServiceOptimizer
    * @see <a href="http://trac.blazegraph.com/ticket/1072"> Configurable ALP
    *      Service </a>
    * @see <a href="http://trac.blazegraph.com/ticket/1117"> Document the ALP
    *      Service </a>
    */
    URI ALP_SERVICE = new URIImpl(NAMESPACE + "alp");

}
