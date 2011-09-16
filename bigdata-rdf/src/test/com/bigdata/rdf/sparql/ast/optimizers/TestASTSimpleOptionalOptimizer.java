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
 * Created on Sep 15, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Iterator;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for {@link ASTSimpleOptionalOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTSimpleOptionalOptimizer extends
        AbstractASTEvaluationTestCase {

    public TestASTSimpleOptionalOptimizer() {
        super();
    }

    public TestASTSimpleOptionalOptimizer(final String name) {
        super(name);
    }

    /**
     * Unit test for recognizing a "simple optional" and lifting it into the
     * parent join group.
     * 
     * TODO Unit test for a variation where there is a FILTER in the simple
     * optional. Make sure that it also gets lifted.
     * 
     * TODO Unit test for a variation where there are FILTERS or other things in
     * the optional which mean that we can not lift out the statement pattern.
     * 
     * TODO Unit test for a variation where the FILTER *does* require
     * materialization but we can lift it out anyway because the variables on
     * which it depends will be "known" bound in the parent group by the time we
     * evaluate the optional.
     */
    public void test_simpleOptional() throws MalformedQueryException {

        final String queryStr = "" + //
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"+//
                "PREFIX dc: <http://purl.org/dc/terms/> \n"+//
                "PREFIX p1: <http://www.bigdata.com/> \n"+//
                "SELECT * \n" + //
                "WHERE { \n" + //
                "  ?_var1 rdf:type <http://suawa.org/mediadb#Album>. \n" + //
                "  ?_var1 p1:genre ?_var8.  \n" + //
                "  ?_var8 dc:title ?_var9.  \n" + //
                "  FILTER ((?_var9 in(\"Folk\", \"Hip-Hop\"))) . \n" + //
                "  OPTIONAL { \n" + //
                "    ?_var1 dc:title ?_var10 \n" + //
                "  }.  \n" + //
                "  OPTIONAL { \n" + //
                "    ?_var1 p1:mainArtist ?_var12. \n" + //
                "    ?_var12 dc:title ?_var11 \n" + //
                "  } \n" + //
                "}";

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer,store);

        QueryRoot queryRoot = astContainer.getOriginalAST();

        queryRoot = (QueryRoot) new ASTWildcardProjectionOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        queryRoot = (QueryRoot) new ASTSimpleOptionalOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        final GraphPatternGroup<?> whereClause = queryRoot.getWhereClause();

        // Verify that we lifted out the simple optional statement pattern.
        {
            int nstmts = 0;
            for(IGroupMemberNode child : whereClause) {
                if(child instanceof StatementPatternNode) {
                    nstmts++;
                }
            }
            assertEquals("#statements", 4, nstmts);
        }
        
        /*
         * Verify that there is one optional remaining, and that it is the one
         * with two statement patterns.
         */
        {
            final Iterator<JoinGroupNode> itr = BOpUtility.visitAll(
                    whereClause, JoinGroupNode.class);
            int ngroups = 0;
            int noptionalGroups = 0;
            while (itr.hasNext()) {
                final JoinGroupNode tmp = itr.next();
                ngroups++;
                if(tmp.isOptional())
                    noptionalGroups++;
            }
            assertEquals("#ngroups", 2, ngroups);
            assertEquals("#optionalGroups", 1, noptionalGroups);
        }

    }

    /**
     * Test effective boolean value - optional.
     * 
     * <pre>
     * PREFIX  xsd: <http://www.w3.org/2001/XMLSchema#>
     * PREFIX  : <http://example.org/ns#>
     * SELECT  ?a
     * WHERE
     *     { ?a :p ?v . 
     *       OPTIONAL
     *         { ?a :q ?w } . 
     *       FILTER (?w) .
     *     }
     * </pre>
     * 
     * @see ASTSimpleOptionalOptimizer
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_sparql_bev_5() throws Exception {

        final String queryStr = "" + //
                "PREFIX  xsd: <http://www.w3.org/2001/XMLSchema#>\n" + //
                "PREFIX  : <http://example.org/ns#>" + //
                "SELECT ?a \n" + //
                "WHERE" + //
                "    { ?a :p ?v . \n" + //
                "      OPTIONAL \n" + //
                "        { ?a :q ?w } . \n" + //
                "      FILTER (?w) . \n" + //
                "    }\n" //
        ;

        /*
         * Add the Values used in the query to the lexicon. This makes it
         * possible for us to explicitly construct the expected AST and
         * the verify it using equals().
         */
        final BigdataValueFactory f = store.getValueFactory();
        final BigdataURI p = f.createURI("http://example.org/ns#p");
        final BigdataURI q = f.createURI("http://example.org/ns#q");
        final BigdataValue[] values = new BigdataValue[] { p, q };
        store.getLexiconRelation()
                .addTerms(values, values.length, false/* readOnly */);

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);
    
        QueryRoot queryRoot = astContainer.getOriginalAST();
        
        queryRoot = (QueryRoot) new ASTSimpleOptionalOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        /*
         * Create the expected AST.
         */
        final JoinGroupNode expectedClause = new JoinGroupNode();
        {

            // :x3 :q ?w
            expectedClause.addChild(new StatementPatternNode(//
                    new VarNode("a"),// s
                    new ConstantNode(new Constant(p.getIV())),// p
                    new VarNode("v"),// o
                    null,// c
                    Scope.DEFAULT_CONTEXTS//
                    ));
            // :x3 :q ?w
            final StatementPatternNode liftedSp = new StatementPatternNode(//
                    new VarNode("a"),// s
                    new ConstantNode(new Constant(q.getIV())),// p
                    new VarNode("w"),// o
                    null,// c
                    Scope.DEFAULT_CONTEXTS//
                    );
            liftedSp.setSimpleOptional(true);
            expectedClause.addChild(liftedSp);
            
            expectedClause.addChild(new FilterNode(new VarNode("w")));
            
        }
        
        assertEquals("modifiedClause", expectedClause,
                queryRoot.getWhereClause());        

    }
    
}
