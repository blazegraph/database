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
 * Created on Oct 15, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import org.openrdf.query.Dataset;

import com.bigdata.rdf.internal.IV;

/**
 * Test suite for named and default graph stuff.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestNamedGraphs extends AbstractDataDrivenSPARQLTestCase {

    public TestNamedGraphs() {
    }

    public TestNamedGraphs(final String name) {
        super(name);
    }

    /**
     * A series of quads mode unit tests for named and default graph query of a
     * data set containing a single triple.
     */
    public void test_namedGraphs_01a() throws Exception {

        if(!store.isQuads())
            return;
        
        new TestHelper(
                "named-graphs-01a",// testURI
                "named-graphs-01a.rq", // queryURI
                "named-graphs-01.trig", // dataURI
                "named-graphs-01.srx" // resultURI
                ).runTest();

    }

    /**
     * This version of the query specifies a FROM clause and hence causes the
     * {@link Dataset} to become non-<code>null</code> but does not specify any
     * FROM NAMED clauses. However, this query (like all the others in this
     * series) is only querying the named graphs.
     * <p>
     * Note: The correct result for this query should be NO solutions. This is
     * one of the cases covered by the DAWG/TCK graph-02 and graph-04 tests. One
     * of those tests verifies that when the data is from the default graph and
     * the query is against the named graphs then there are no solutions. The
     * other test verifies that the opposite is also true - that when the data
     * are from the named graphs and the query is against the default graph that
     * there are no solutions.
     */
    public void test_namedGraphs_01b() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
                "named-graphs-01b",// testURI
                "named-graphs-01b.rq", // queryURI
                "named-graphs-01.trig", // dataURI
                "named-graphs-01b.srx" // resultURI
                ).runTest();

    }
    
    public void test_namedGraphs_01c() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
                "named-graphs-01c",// testURI
                "named-graphs-01c.rq", // queryURI
                "named-graphs-01.trig", // dataURI
                "named-graphs-01.srx" // resultURI
                ).runTest();

    }

    public void test_namedGraphs_01d() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
                "named-graphs-01d",// testURI
                "named-graphs-01d.rq", // queryURI
                "named-graphs-01.trig", // dataURI
                "named-graphs-01.srx" // resultURI
                ).runTest();

    }

    /*
     * Ported from com.bigdata.rdf.sail.TestNamedGraphs
     */
    
    /**
     * 8.2.1 Specifying the Default Graph
     * <p>
     * Each FROM clause contains an IRI that indicates a graph to be used to 
     * form the default graph. This does not put the graph in as a named graph.
     * <p>
     * In this example, the RDF Dataset contains a single default graph and no 
     * named graphs:
     * <pre>
     * # Default graph (stored at http://example.org/foaf/aliceFoaf)
     * @prefix  foaf:  <http://xmlns.com/foaf/0.1/> .
     * 
     * _:a  foaf:name     "Alice" .
     * _:a  foaf:mbox     <mailto:alice@work.example> .
     * 
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * SELECT  ?name
     * FROM    <http://example.org/foaf/aliceFoaf>
     * WHERE   { ?x foaf:name ?name }
     * 
     * name
     * "Alice"
     * </pre>
     * If a query provides more than one FROM clause, providing more than one 
     * IRI to indicate the default graph, then the default graph is based on 
     * the RDF merge of the graphs obtained from representations of the 
     * resources identified by the given IRIs.     
     */
    public void test_8_2_1() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
                "named-graphs-8-2-1",// testURI
                "named-graphs-8-2-1.rq", // queryURI
                "named-graphs-8-2-1.trig", // dataURI
                "named-graphs-8-2-1.srx" // resultURI
                ).runTest();

    }

    /**
     * 8.2.3 Combining FROM and FROM NAMED
     * <p>
     * The FROM clause and FROM NAMED clause can be used in the same query.
     * <pre>
     * # Default graph (stored at http://example.org/dft.ttl)
     * @prefix dc: <http://purl.org/dc/elements/1.1/> .
     * 
     * <http://example.org/bob>    dc:publisher  "Bob Hacker" .
     * <http://example.org/alice>  dc:publisher  "Alice Hacker" .
     * 
     * # Named graph: http://example.org/bob
     * @prefix foaf: <http://xmlns.com/foaf/0.1/> .
     * 
     * _:a foaf:name "Bob" .
     * _:a foaf:mbox <mailto:bob@oldcorp.example.org> .
     * 
     * # Named graph: http://example.org/alice
     * @prefix foaf: <http://xmlns.com/foaf/0.1/> .
     * 
     * _:a foaf:name "Alice" .
     * _:a foaf:mbox <mailto:alice@work.example.org> .
     * 
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * 
     * SELECT ?who ?g ?mbox
     * FROM <http://example.org/dft.ttl>
     * FROM NAMED <http://example.org/alice>
     * FROM NAMED <http://example.org/bob>
     * WHERE
     * {
     *    ?g dc:publisher ?who .
     *    GRAPH ?g { ?x foaf:mbox ?mbox }
     * }
     * </pre>
     * The RDF Dataset for this query contains a default graph and two named 
     * graphs. The GRAPH keyword is described below.
     * <p>
     * The actions required to construct the dataset are not determined by the 
     * dataset description alone. If an IRI is given twice in a dataset 
     * description, either by using two FROM clauses, or a FROM clause and a 
     * FROM NAMED clause, then it does not assume that exactly one or 
     * exactly two attempts are made to obtain an RDF graph associated with the 
     * IRI. Therefore, no assumptions can be made about blank node identity in 
     * triples obtained from the two occurrences in the dataset description. 
     * In general, no assumptions can be made about the equivalence of the 
     * graphs. 
     */
    public void test_8_2_3() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
                "named-graphs-8-2-3",// testURI
                "named-graphs-8-2-3.rq", // queryURI
                "named-graphs-8-2-3.trig", // dataURI
                "named-graphs-8-2-3.srx" // resultURI
                ).runTest();

    }
    
    /**
     * 8.3.1 Accessing Graph Names
     * 
     * The following two graphs will be used in examples:
     * <pre>
     * # Named graph: http://example.org/foaf/aliceFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:a  foaf:name     "Alice" .
     * _:a  foaf:mbox     <mailto:alice@work.example> .
     * _:a  foaf:knows    _:b .
     * 
     * _:b  foaf:name     "Bob" .
     * _:b  foaf:mbox     <mailto:bob@work.example> .
     * _:b  foaf:nick     "Bobby" .
     * _:b  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * 
     * # Named graph: http://example.org/foaf/bobFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:z  foaf:mbox     <mailto:bob@work.example> .
     * _:z  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * _:z  foaf:nick     "Robert" .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * </pre>
     * 
     * The query below matches the graph pattern against each of the named 
     * graphs in the dataset and forms solutions which have the src variable 
     * bound to IRIs of the graph being matched. The graph pattern is matched 
     * with the active graph being each of the named graphs in the dataset.
     * <pre> 
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * 
     * SELECT ?src ?bobNick
     * FROM NAMED <http://example.org/foaf/aliceFoaf>
     * FROM NAMED <http://example.org/foaf/bobFoaf>
     * WHERE
     *   {
     *     GRAPH ?src
     *     { ?x foaf:mbox <mailto:bob@work.example> .
     *       ?x foaf:nick ?bobNick
     *     }
     *   }
     * </pre>
     * The query result gives the name of the graphs where the information was 
     * found and the value for Bob's nick:
     * <pre>
     * src     bobNick
     * <http://example.org/foaf/aliceFoaf>     "Bobby"
     * <http://example.org/foaf/bobFoaf>   "Robert"
     * </pre>
     */
    public void test_8_3_1() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
                "named-graphs-8-3-1",// testURI
                "named-graphs-8-3-1.rq", // queryURI
                "named-graphs-8-3-1.trig", // dataURI
                "named-graphs-8-3-1.srx" // resultURI
                ).runTest();

    }
    
    /**
     * 8.3.2 Restricting by Graph IRI
     * 
     * The following two graphs will be used in examples:
     * <pre>
     * # Named graph: http://example.org/foaf/aliceFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:a  foaf:name     "Alice" .
     * _:a  foaf:mbox     <mailto:alice@work.example> .
     * _:a  foaf:knows    _:b .
     * 
     * _:b  foaf:name     "Bob" .
     * _:b  foaf:mbox     <mailto:bob@work.example> .
     * _:b  foaf:nick     "Bobby" .
     * _:b  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * 
     * # Named graph: http://example.org/foaf/bobFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:z  foaf:mbox     <mailto:bob@work.example> .
     * _:z  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * _:z  foaf:nick     "Robert" .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * </pre>
     * The query can restrict the matching applied to a specific graph by 
     * supplying the graph IRI. This sets the active graph to the graph named 
     * by the IRI. This query looks for Bob's nick as given in the graph 
     * http://example.org/foaf/bobFoaf.
     * <pre>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * PREFIX data: <http://example.org/foaf/>
     * 
     * SELECT ?nick
     * FROM NAMED <http://example.org/foaf/aliceFoaf>
     * FROM NAMED <http://example.org/foaf/bobFoaf>
     * WHERE
     *   {
     *      GRAPH data:bobFoaf {
     *          ?x foaf:mbox <mailto:bob@work.example> .
     *          ?x foaf:nick ?nick }
     *   }
     * </pre>
     * which yields a single solution:
     * <pre>
     * nick
     * "Robert"
     * </pre>
     */
    public void test_8_3_2() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
                "named-graphs-8-3-1",// testURI
                "named-graphs-8-3-1.rq", // queryURI
                "named-graphs-8-3-1.trig", // dataURI
                "named-graphs-8-3-1.srx" // resultURI
                ).runTest();

    }
    
    /**
     * 8.3.3 Restricting Possible Graph IRIs
     * 
     * The following two graphs will be used in examples:
     * <pre>
     * # Named graph: http://example.org/foaf/aliceFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:a  foaf:name     "Alice" .
     * _:a  foaf:mbox     <mailto:alice@work.example> .
     * _:a  foaf:knows    _:b .
     * 
     * _:b  foaf:name     "Bob" .
     * _:b  foaf:mbox     <mailto:bob@work.example> .
     * _:b  foaf:nick     "Bobby" .
     * _:b  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * 
     * # Named graph: http://example.org/foaf/bobFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:z  foaf:mbox     <mailto:bob@work.example> .
     * _:z  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * _:z  foaf:nick     "Robert" .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * </pre>
     * A variable used in the GRAPH clause may also be used in another GRAPH 
     * clause or in a graph pattern matched against the default graph in the 
     * dataset.
     *<p>
     * The query below uses the graph with IRI http://example.org/foaf/aliceFoaf 
     * to find the profile document for Bob; it then matches another pattern 
     * against that graph. The pattern in the second GRAPH clause finds the 
     * blank node (variable w) for the person with the same mail box (given by 
     * variable mbox) as found in the first GRAPH clause (variable whom), 
     * because the blank node used to match for variable whom from Alice's FOAF 
     * file is not the same as the blank node in the profile document (they are 
     * in different graphs).
     * <pre>
     * PREFIX  data:  <http://example.org/foaf/>
     * PREFIX  foaf:  <http://xmlns.com/foaf/0.1/>
     * PREFIX  rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
     * 
     * SELECT ?mbox ?nick ?ppd
     * FROM NAMED <http://example.org/foaf/aliceFoaf>
     * FROM NAMED <http://example.org/foaf/bobFoaf>
     * WHERE
     * {
     *   GRAPH data:aliceFoaf
     *   {
     *     ?alice foaf:mbox <mailto:alice@work.example> ;
     *            foaf:knows ?whom .
     *     ?whom  foaf:mbox ?mbox ;
     *            rdfs:seeAlso ?ppd .
     *     ?ppd  a foaf:PersonalProfileDocument .
     *   } .
     *   GRAPH ?ppd
     *   {
     *       ?w foaf:mbox ?mbox ;
     *          foaf:nick ?nick
     *   }
     * }
     * </pre>
     * <pre>
     * mbox    nick    ppd
     * <mailto:bob@work.example>   "Robert"    <http://example.org/foaf/bobFoaf>
     * </pre>
     * Any triple in Alice's FOAF file giving Bob's nick is not used to provide 
     * a nick for Bob because the pattern involving variable nick is restricted 
     * by ppd to a particular Personal Profile Document.
     */
    public void test_8_3_3() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
                "named-graphs-8-3-1",// testURI
                "named-graphs-8-3-1.rq", // queryURI
                "named-graphs-8-3-1.trig", // dataURI
                "named-graphs-8-3-1.srx" // resultURI
                ).runTest();

    }
    
    /*
     * Unit test focusing on queries against a default graph comprised of the
     * RDF merge of zero or more graphs where the query involves joins (versus
     * simple access path reads). These were ported from TestNamedGraphs in the
     * sail package.
     */
    
    /**
     * One graph in the defaultGraphs set, but the specified graph has no data.
     * There should be no solutions.
     */
    public void test_default_graph_joins_01a() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
            "default-graph-joins-01a",// testURI
            "default-graph-joins-01a.rq", // queryURI
            "default-graph-joins-01.trig", // dataURI
            "default-graph-joins-01a.srx" // resultURI
            ).runTest();

    }

    /**
     * The data set is not specified. The query will run against the "default"
     * defaultGraph. For Sesame, I am assuming that the query is run against the
     * RDF merge of all graphs, including the null graph. Rather than expanding
     * the access path, we put a distinct triple filter on the access path. The
     * filter will strip off the context information and filter for distinct
     * (s,p,o).
     * <p>
     * Note: In this case there is a duplicate solution based on the duplicate
     * (john,loves,mary) triple which gets filtered out.
     */
    public void test_default_graph_joins_01b() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
            "default-graph-joins-01b",// testURI
            "default-graph-joins-01b.rq", // queryURI
            "default-graph-joins-01.trig", // dataURI
            "default-graph-joins-01b.srx" // resultURI
            ).runTest();

    }

    /**
     * Two graphs (c1,c2) are used. There should be one solution.
     * <p>
     * There is a second solution if c4 is considered, but it should not be
     * since it is not in the default graph.
     * <p>
     * If you are getting an extra (john,mark,paul) solution then the default
     * graph access path is not enforcing "DISTINCT SPO" semantics.
     */
    public void test_default_graph_joins_01c() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
            "default-graph-joins-01c",// testURI
            "default-graph-joins-01c.rq", // queryURI
            "default-graph-joins-01.trig", // dataURI
            "default-graph-joins-01c.srx" // resultURI
            ).runTest();

    }

    /**
     * Named graph query finds everyone who loves someone and the graph in which
     * that relationship was asserted.
     * <p>
     * Note: There is an additional occurrence of the relationship in [c4], but
     * the query does not consider that graph and that occurrence should not be
     * matched.
     * <p>
     * Note: there is a statement duplicated in two of the named graphs
     * (john,loves,mary) so we can verify that both occurrences are reported.
     * <p>
     * Note: This query DOES NOT USE JOINS. See the next example below for one
     * that does.
     */
    public void test_default_graph_joins_01d() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
            "default-graph-joins-01d",// testURI
            "default-graph-joins-01d.rq", // queryURI
            "default-graph-joins-01.trig", // dataURI
            "default-graph-joins-01d.srx" // resultURI
            ).runTest();

    }

    /**
     * Named graph query finds everyone who loves someone and the state in which
     * they live. Each side of the join is drawn from one of the two named
     * graphs (the graph variables are bound).
     */
    public void test_default_graph_joins_01e() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
            "default-graph-joins-01e",// testURI
            "default-graph-joins-01e.rq", // queryURI
            "default-graph-joins-01.trig", // dataURI
            "default-graph-joins-01e.srx" // resultURI
            ).runTest();

    }

    /**
     * Named graph query finds everyone who loves someone and the graph in which
     * that relationship was asserted, and the state in which they live.
     * <p>
     * Note: There is an additional occurrence of the relationship in [c4], but
     * the query does not consider that graph and that occurrence should not be
     * matched.
     * <p>
     * Note: there is a statement duplicated in two of the named graphs
     * (john,loves,mary) so we can verify that both occurrences are reported.
     * <p>
     * Note: This test case can fail if the logic is leaving [c] unbound when it
     * should be restricting [c] to the specific graphs in the FROM NAMED
     * clauses.
     * 
     * @todo query pattern involving a defaultGraph and one or more named
     *       graphs.
     */
    public void test_default_graph_joins_01f() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
            "default-graph-joins-01f",// testURI
            "default-graph-joins-01f.rq", // queryURI
            "default-graph-joins-01.trig", // dataURI
            "default-graph-joins-01f.srx" // resultURI
            ).runTest();

    }
    
    /**
     * Unit test for case where there is a single named group which will be
     * visited by the query. In this case, the decision tree says that we should
     * bind the context position to the {@link IV} for that graph in order to
     * restrict the query to exactly the desired graph (rather than using an
     * expander pattern). However, the graph variable also needs to become bound
     * in the query solutions. If we simply replace the graph variable with a
     * constant, then the as-bound value of the graph variable will not be
     * reported in the result set.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/359
     */
    public void test_named_graphs_ticket_359() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
            "named-graphs-ticket-359",// testURI
            "named-graphs-ticket-359.rq", // queryURI
            "named-graphs-ticket-359.trig", // dataURI
            "named-graphs-ticket-359.srx" // resultURI
            ).runTest();

    }

    /**
     * Note: This is a duplicate of <a href="http://trac.blazegraph.com/ticket/792>
     * GRAPH ?g { FILTER NOT EXISTS { ?s ?p ?o } } not respecting ?g </a>
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/888> GRAPH ignored by FILTER
     *      NOT EXISTS </a>
     */
    public void test_named_graphs_ticket_888() throws Exception {
        
        if(!store.isQuads())
            return;

        new TestHelper(
                "named-graphs-ticket-888",// testURI
                "named-graphs-ticket-888.rq", // queryURI
                "named-graphs-ticket-888.trig", // dataURI
                "named-graphs-ticket-888.srx" // resultURI
                ).runTest();

    }
    
    /**
     * Unit test of a work around for {@link #test_named_graphs_ticket_888()}.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/888> GRAPH ignored by FILTER
     *      NOT EXISTS </a>
     */
    public void test_named_graphs_ticket_888b() throws Exception {
        
        if(!store.isQuads())
            return;

        new TestHelper(
                "named-graphs-ticket-888",// testURI
                "named-graphs-ticket-888b.rq", // queryURI
                "named-graphs-ticket-888.trig", // dataURI
                "named-graphs-ticket-888.srx" // resultURI
                ).runTest();

    }
    
}
