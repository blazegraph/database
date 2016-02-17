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
 * Created on Nov 7, 2007
 */
package com.bigdata.rdf.sail.sparql;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.QueryParserUtil;

import com.bigdata.BigdataStatics;
import com.bigdata.rdf.sparql.AbstractBigdataExprBuilderTestCase;

/**
 * Non-manifest driven versions of the manifest driven test suite to facilitate
 * debugging.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BigdataSPARQL2ASTParserTest extends AbstractBigdataExprBuilderTestCase {

    public BigdataSPARQL2ASTParserTest() {
    }

    public BigdataSPARQL2ASTParserTest(String name) {
        super(name);
    }

    /**
     * PrefixName with backslash-escaped colons.
     * <pre>
     * SELECT * WHERE {
     *     ?page og:audio\:title ?title
     * }
     * </pre>
     * @throws MalformedQueryException 
     * @see syntax-query/qname-escape-01.rq
     */
    public void test_qname_escape_01() throws MalformedQueryException {
       
       if (!BigdataStatics.runKnownBadTests) {
          // FIXME See #1076 Negative parser tests
          return;
       }

        final String query = "PREFIX og: <http://ogp.me/ns#>\n"
                + " SELECT * WHERE {\n" + "    ?page og:audio\\:title ?title\n"
                + "}";

        parseOperation(query);
        
    }

    /**
     * PrefixName with backslash-escaped colon (qname in select).
     */
    public void test_qname_escape_01b() throws MalformedQueryException {

      if (!BigdataStatics.runKnownBadTests) {
         // FIXME See #1076 Negative parser tests
         return;
      }
       
        final String query = "PREFIX og: <http://ogp.me/ns#>\n"
                + "SELECT ( og:audio\\:title as ?x )"
                + "WHERE {?page og:foo ?title}";

        parseOperation(query);

    }

    /**
     * PrefixName with hex-encoded colon.
     */
    public void test_qname_escape_02() throws MalformedQueryException {

        final String query = "PREFIX og: <http://ogp.me/ns#>\n"
                + "SELECT * WHERE { ?page og:audio%3Atitle ?title }";

        parseOperation(query);

    }

    /**
     * Positive test (originally failed because it was being passed to
     * parseQuery() rather than parseUpdate()).
     */
    public void test_syntax_update_01() throws MalformedQueryException {
        
        final String query="BASE <http://example/base#>\n"+
            "PREFIX : <http://example/>\n"+
            "LOAD <http://example.org/faraway>";
    
        parseOperation(query);
        
    }

    /** grouping by expression, done wrong */
    public void test_agg08() throws MalformedQueryException {

        final String query = "PREFIX : <http://www.example.org/>\n"
                + "SELECT ((?O1 + ?O2) AS ?O12) (COUNT(?O1) AS ?C)\n"
                + "WHERE { ?S :p ?O1; :q ?O2 } GROUP BY (?O1 + ?O2)\n"
                + "ORDER BY ?O12";

        negativeTest(query);

    }

    /**
     * Projection of an ungrouped variable (not appearing in the GROUP BY
     * expression)
     */
    public void test_agg09() throws MalformedQueryException {

        final String query = "PREFIX : <http://www.example.org/>\n"
                + "SELECT ?P (COUNT(?O) AS ?C)\n"
                + "WHERE { ?S ?P ?O } GROUP BY ?S";
        
        negativeTest(query);

    }
    
    /**
     * Projection of an ungrouped variable (no GROUP BY expression at all)
     */
    public void test_agg10() throws MalformedQueryException {

        final String query = "PREFIX : <http://www.example.org/>\n"
                + "SELECT ?P (COUNT(?O) AS ?C)\n"
                + "WHERE { ?S ?P ?O }";

        negativeTest(query);

    }
    
    /**
     * Use of an ungrouped variable in a project expression
     */
    public void test_agg11() throws MalformedQueryException {

        final String query = "PREFIX : <http://www.example.org/>\n"
                + "SELECT ((?O1 + ?O2) AS ?O12) (COUNT(?O1) AS ?C)\n"
                + "WHERE { ?S :p ?O1; :q ?O2 } GROUP BY (?S)";

        negativeTest(query);

    }

    /**
     * Use of an ungrouped variable in a project expression, where the variable
     * appears in a GROUP BY expression
     */
    public void test_agg12() throws MalformedQueryException {

        final String query = "PREFIX : <http://www.example.org/>\n"+
                "SELECT ?O1 (COUNT(?O2) AS ?C)\n"+
                "WHERE { ?S :p ?O1; :q ?O2 } GROUP BY (?O1 + ?O2)";

        negativeTest(query);

    }
    
    /**
     * projection of ungrouped variable
     */
    public void test_group06() throws MalformedQueryException {

        final String query = "PREFIX : <http://example/>\n"+
                "SELECT ?s ?v\n"+
                "{\n"+
                "  ?s :p ?v .\n"+
                "}\n"+
                "GROUP BY ?s";

        negativeTest(query);

    }
    
    /**
     * projection of ungrouped variable, more complex example than Group-6
     */
    public void test_group07() throws MalformedQueryException {

        final String query = "prefix lode: <http://linkedevents.org/ontology/>\n"+
        "prefix dc: <http://purl.org/dc/elements/1.1/>\n"+
"prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"+
"select ?event ?eventName ?venue ?photo\n"+
"where {\n"+
"   ?photo lode:illustrate ?event .\n"+
"   {\n"+
"   select ?event ?eventName ?venue\n"+
"   where {\n"+
"         ?event dc:title ?eventName .\n"+
"         ?event lode:atPlace ?venue .\n"+
"         ?venue rdfs:label \"Live Music Hall\" .\n"+
"         }\n"+
"   }\n"+
"}\n"+
"GROUP BY ?event\n"
;

        negativeTest(query);

    }

    /**
     * Select * Not allowed with GROUP BY
     */
    public void test_syn_bad_01() throws MalformedQueryException {

        final String query = "SELECT * { ?s ?p ?o } GROUP BY ?s";

        negativeTest(query);

    }
    
    /**
     * required syntax error : out of scope variable in SELECT from group.
     */
    public void test_syn_bad_02() throws MalformedQueryException {

        final String query = "SELECT ?o { ?s ?p ?o } GROUP BY ?s";

        negativeTest(query);

    }

    /**
     * Same variable can not be projected more than once.
     */
    public void test_syn_bad_03() throws MalformedQueryException {

        final String query = "SELECT (1 AS ?X) (1 AS ?X) {}";

        negativeTest(query);

    }

    /** Empty UPDATE. */
    public void test_syntax_update_38() throws MalformedQueryException {

        final String query = "# Empty\n";

        parseOperation(query);

    }

    /** BASE but otherwise empty UPDATE. */
    public void test_syntax_update_39() throws MalformedQueryException {

        final String query = "BASE <http://example/>\n# Otherwise empty\n";

        parseOperation(query);

    }
    /** PREFIX but otherwise empty UPDATE. */
    public void test_syntax_update_30() throws MalformedQueryException {

        final String query = "PREFIX : <http://example/>\n# Otherwise empty\n";

        parseOperation(query);

    }

    /**
     * Variable in DELETE DATA's data.
     */
    public void test_syntax_update_bad_03() throws MalformedQueryException {

        final String query = "DELETE DATA { ?s <:p> <:o> }";

        negativeTest(query);

    }

    /** Variable in INSERT DATA's data. */
    public void test_syntax_update_bad_04() throws MalformedQueryException {

        final String query = "INSERT DATA { GRAPH ?g {<:s> <:p> <:o> } }";

        negativeTest(query);

    }
    
    /** Too many separators (in UPDATE request) */
    public void test_syntax_update_bad_08() throws MalformedQueryException {

        final String query = "CREATE GRAPH <:g> ;; LOAD <:remote> into GRAPH <:g>";

        negativeTest(query);

    }
    
    /** Too many separators (in UPDATE request) */
    public void test_syntax_update_bad_09() throws MalformedQueryException {

        final String query = "CREATE GRAPH <:g> ; LOAD <:remote> into GRAPH <:g> ;;";

        negativeTest(query);

    }
    
    /** BNode in DELETE WHERE */
    public void test_syntax_update_bad_10() throws MalformedQueryException {

        final String query = "DELETE WHERE { _:a <:p> <:o> }";

        negativeTest(query);

    }
    
    /**
     * When <code>true</code> use the {@link Bigdata2ASTSPARQLParser} otherwise
     * use the openrdf parser.
     */
    private static final boolean useBigdataParser = true;
    
    /**
     * Parse with expectation of failure.
     * 
     * @param query
     *            The query or update request.
     */
    private void negativeTest(final String query) {

        try {

            parseOperation(query);

            fail("Negative test - should fail");
            
        } catch (MalformedQueryException ex) {
            
            // Ignore expected exception.
            
        }

    }
    
    /**
     * Parse with expectation of success.
     * 
     * @param query
     *            The query or update request.
     *            
     * @throws MalformedQueryException
     */
    private void parseOperation(final String query)
            throws MalformedQueryException {

        if (useBigdataParser) {

            new Bigdata2ASTSPARQLParser().parseOperation(query,
                    baseURI);
            
        } else {
            
            QueryParserUtil.parseOperation(QueryLanguage.SPARQL, query, baseURI);
            
        }

    }

}
