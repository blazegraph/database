/**

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
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import org.openrdf.query.MalformedQueryException;

import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.QueryRoot;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/232
 */
public class TestASTVariableScopeAnalysis extends AbstractASTEvaluationTestCase {

    public TestASTVariableScopeAnalysis() {
        super();
    }

    public TestASTVariableScopeAnalysis(String name) {
        super(name);
    }

    /**
     * In this query, the ?x projected from the first subquery should not be
     * evaluated within/joined with the ?x within the second subquery, since the
     * ?x within the second subquery is local to the subquery since it is not
     * projected. This query would only join on ?s, since it is the only common
     * var between the 2 subqueries.
     * 
     * TODO Rule: A variable within a subquery is distinct from the same name
     * variable outside of the subquery unless the variable is projected from
     * the subquery.
     * 
     * TODO Rule: A variable bound within an OPTIONAL *MAY* be bound in the
     * parent group.
     * 
     * TODO Rule: A variable bound within a UNION *MAY* be bound in the parent
     * group. Exception: if the variable is bound on all alternatives in the
     * UNION, then it MUST be bound in the parent group.
     * 
     * TODO A variable bound by a statement pattern or a let/bind MUST be bound
     * within the parent group and within all contexts which are evaluated
     * *after* it is bound. (This is the basis for propagation of bindings to
     * the parent. Since SPARQL demands bottom up evaluation semantics a
     * variable which MUST be bound in a group MUST be bound in its parent.)
     */
    public void test_fail() throws MalformedQueryException {
        final String queryStr = ""
                + "PREFIX : <http://example.org/>\n"
                + "SELECT ?s ?x\n"
                + "WHERE {\n"
                + "     {\n"
                + "        SELECT ?s ?x { ?s :p ?x }\n"
                + "     }\n"
                + "     {\n" // LET ( ?x := expr ) == BIND( expr AS ?x )
//                + "        SELECT ?s ?fake1 ?fake2 { ?x :q ?s . BIND(1 AS ?fake1) . BIND(2 AS ?fake2) . }\n"
                + "        SELECT ?s ?fake1 ?fake2 { ?x :q ?s . LET(?fake1 := 1) . LET(?fake2 := 2) . }\n"
                + "     }\n" //
                + "}\n"//
        ;

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, null/* baseURI */);
        
        if(log.isInfoEnabled())
            log.info(queryRoot);
        
        fail("write test");

    }

}
