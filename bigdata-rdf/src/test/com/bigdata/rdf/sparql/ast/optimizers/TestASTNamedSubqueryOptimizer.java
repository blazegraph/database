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
 * Created on Sep 13, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Set;

import org.openrdf.query.MalformedQueryException;

import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.IBindingProducerNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;

/**
 * Test suite for the {@link ASTNamedSubqueryOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          FIXME Add unit test where there are no join variables because there
 *          is nothing which must be bound in one context or the other.
 * 
 *          FIXME Add unit test where there are no join variables because the
 *          named solution set is included from multiple locations without any
 *          overlap in the incoming bindings.
 * 
 *          FIXME Add unit test where there are join variables, but they are
 *          only those which overlap for the different locations at which the
 *          named solution set is joined into the query.
 */
public class TestASTNamedSubqueryOptimizer extends
        AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTNamedSubqueryOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTNamedSubqueryOptimizer(String name) {
        super(name);
    }

    /**
     * Unit test for computing the join variables for a named subquery based on
     * the analysis of the bindings which MUST be produced by the subquery and
     * those which MUST be bound on entry into the group in which the subquery
     * solution set is included within the main query.
     * <p>
     * The join should be on <code>?x</code> in this example.
     * 
     * FIXME Actually, whether we use a join on <code>?x</code> or whether there
     * are NO join variables depends on the expected cardinality of the named
     * solution set and <code>?x rdfs:label ?o</code>. We should only use
     * <code>?x</code> as a join variable if the expected cardinality of
     * <code>?x rdfs:label ?o</code> is LT the expected cardinality of the named
     * solution set. In fact, for this case it probably won't be.
     * 
     * FIXME Write unit tests where the INCLUDE is embedded into a child group.
     * In that location we know that some things are already bound by the parent
     * group so we can be assured that we will use the available join variables.
     * [This really depends on running the INCLUDE after the non-optional joins
     * in the parent, which is itself just a heuristic. In fact, the entire
     * notion of
     * {@link StaticAnalysis#getIncomingBindings(IBindingProducerNode, Set)}
     * depends on this heuristic!]
     */
    public void test_static_analysis_join_vars() throws MalformedQueryException {

        final String queryStr = "" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"+
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"+
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n"+
                "select ?x ?o \n"+
                " with {"+
                "   select ?x where { ?x rdf:type foaf:Person }\n"+
                " } AS %namedSet1 \n"+
                "where { \n" +
                "   ?x rdfs:label ?o \n" +
                "   INCLUDE %namedSet1 \n"+
                "}";

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot queryRoot = astContainer.getOriginalAST();
        
        // Run the optimizers to determine the join variables.
        queryRoot = (QueryRoot) new DefaultOptimizerList().optimize(context,
                queryRoot, null/* bindingSets */);

        // The expected join variables.
        final IVariable[] joinVars = new IVariable[] { Var.var("x") };

        final NamedSubqueryRoot nsr = (NamedSubqueryRoot) queryRoot
                .getNamedSubqueries().get(0);

        final NamedSubqueryInclude nsi = (NamedSubqueryInclude) queryRoot
                .getWhereClause().get(1);

        /*
         * TODO This is failing for the reasons documented above. I've left this
         * test case here as a place holder for the issue.
         */
        assertEquals(joinVars, nsr.getJoinVars());

        assertEquals(joinVars, nsi.getJoinVars());

    }

}
