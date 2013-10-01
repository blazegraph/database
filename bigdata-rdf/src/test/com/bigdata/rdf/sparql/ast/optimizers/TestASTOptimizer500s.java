/**

Copyright (C) SYSTAP, LLC 2006-2013.  All rights reserved.

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
 * Created on Oct 1, 2013
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;

import org.apache.commons.io.IOUtils;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.EmptyBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sail.sparql.TestSubqueryPatterns;
import com.bigdata.rdf.sail.sparql.ast.ParseException;
import com.bigdata.rdf.sail.sparql.ast.TokenMgrError;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;

/**
 * This test suite is for trac items where the failure mode is a 500 error caused
 * by a software error in the static optimizer.
 * 
 * The tests each consist of a test query in a file in this package.
 * The typical test succeeds if the optimizers run on this query without a disaster.
 * This test suite does NOT have either of the following objectives:
 * - that the static optimizer is correct in the sense that the optimized query has the same meaning as the original query
 * or
 * - an optimizer in the sense that the optimized query is likely to be faster than the original query.
 * 
 * The very limited goal is that no uncaught exceptions are thrown!
 * 
 */
public class TestASTOptimizer500s extends
        AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTOptimizer500s() {
    }

    /**
     * @param name
     */
    public TestASTOptimizer500s(String name) {
        super(name);
    }


    /**
     * Unit test for WITH {subquery} AS "name" and INCLUDE. The WITH must be in
     * the top-level query. 
     * 
     * This is specifically for Trac 746 which crashed out during optimize.
     * So the test simply runs that far, and does not verify anything
     * other than the ability to optimize without an exception
     * @throws IOException 
     */
    public void test_namedSubquery746() throws MalformedQueryException,
            TokenMgrError, ParseException, IOException {
        optimizeQuery("ticket746");

    }
    
/**
 * <pre>
SELECT *
{  { SELECT * { ?s ?p ?o } LIMIT 1 }
   FILTER ( ?s = &lt;eg:a&gt; )
}
 </pre>
 * @throws MalformedQueryException
 * @throws TokenMgrError
 * @throws ParseException
 * @throws IOException
 */
    public void test_filterSubselect737() throws MalformedQueryException,
            TokenMgrError, ParseException, IOException {
        optimizeQuery("filterSubselect737");

    }
    

/**
 * <pre>
SELECT *
WHERE {

   { FILTER ( false ) }
    UNION
    {
    {  SELECT ?Subject_A 
      WHERE {
        { SELECT $j__5 ?Subject_A
          {
          } ORDER BY $j__5
        }
      } GROUP BY ?Subject_A
    }
   }
  OPTIONAL {
    {  SELECT ?Subject_A 
      WHERE {
        { SELECT $j__8 ?Subject_A
          {
         
          }  ORDER BY $j_8
        }
      } GROUP BY ?Subject_A
    }
  }
}
 </pre>
 * @throws MalformedQueryException
 * @throws TokenMgrError
 * @throws ParseException
 * @throws IOException
 */
    public void test_nestedSubselectsWithUnion737() throws MalformedQueryException,
            TokenMgrError, ParseException, IOException {
        optimizeQuery("nestedSubselectsWithUnion737");

    }

	void optimizeQuery(final String queryfile) throws IOException, MalformedQueryException {
		final String sparql = IOUtils.toString(getClass().getResourceAsStream(queryfile+".rq"));


        final QueryRoot ast = new Bigdata2ASTSPARQLParser(store).parseQuery2(sparql,baseURI).getOriginalAST();

        final IASTOptimizer rewriter = new DefaultOptimizerList();
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(ast), store);
        rewriter.optimize(context, ast/* queryNode */, new IBindingSet[]{EmptyBindingSet.INSTANCE});
	}
    
}
