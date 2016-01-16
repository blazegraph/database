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
 * Created on Aug 24, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Collection;
import java.util.LinkedHashSet;

import org.openrdf.model.vocabulary.RDF;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.DescribeModeEnum;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * This optimizer rewrites the projection node of a DESCRIBE query into,
 * generating a CONSTRUCT clause and extending the WHERE clause to capture the
 * semantics of the DESCRIBE query. The query type is also changed to CONSTRUCT.
 * <p>
 * For example, the optimizer changes this:
 * 
 * <pre>
 * describe term1 term2 ...
 * where {
 *    whereClause ...
 * }
 * </pre>
 * 
 * Into this:
 * 
 * <pre>
 * construct {
 *   term1 ?p1a ?o1 .
 *   ?s1   ?p1b term1 .
 *   term2 ?p2a ?o2 .
 *   ?s2   ?p2b term2 .
 * }
 * where {
 *   whereClause ...
 *   {
 *     term1 ?p1a ?o1 .
 *   } union {
 *     ?s1   ?p1b term1 .
 *   } union {
 *     term2 ?p2a ?o2 .
 *   } union {
 *     ?s2   ?p2b term2 .
 *   }
 * </pre>
 * <p>
 * Note: The {@link ASTConstructOptimizer} will add a {@link ProjectionNode}
 * based on the generated CONSTRUCT template.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/578"> Concise
 *      Bounded Description </a>
 */
public class ASTDescribeOptimizer implements IASTOptimizer {

//    private static final Logger log = Logger.getLogger(DescribeOptimizer.class); 
    
	@SuppressWarnings("unchecked")
    @Override
	public QueryNodeWithBindingSet optimize(
      final AST2BOpContext context, final QueryNodeWithBindingSet input) {

      final IQueryNode queryNode = input.getQueryNode();
      final IBindingSet[] bindingSet = input.getBindingSets();     
		
		final QueryRoot queryRoot = (QueryRoot) queryNode;
		
		if (queryRoot.getQueryType() != QueryType.DESCRIBE) {
			
		    // Not a query that we will rewrite.
		   return new QueryNodeWithBindingSet(queryRoot, bindingSet);
		    
		}

        // Change the query type. This helps prevent multiple rewrites.
		queryRoot.setQueryType(QueryType.CONSTRUCT);
		
		final GraphPatternGroup<IGroupMemberNode> where;
		
		if (queryRoot.hasWhereClause()) {
			
		    // start with the existing WHERE clause.
			where = queryRoot.getWhereClause();
			
		} else {
			
			// some describe queries don't have where clauses
			queryRoot.setWhereClause(where = new JoinGroupNode());
			
		}
		
		final UnionNode union = new UnionNode();
		
		where.addChild(union); // append UNION to WHERE clause.

        final ConstructNode construct = new ConstructNode(context);

        final ProjectionNode projection = queryRoot.getProjection();

        if (projection == null) {

            throw new RuntimeException("No projection?");

        }

        // The effective DescribeMode.
//        final DescribeModeEnum describeMode = projection.getDescribeMode() == null ? QueryHints.DEFAULT_DESCRIBE_MODE
//                : projection.getDescribeMode();
        final DescribeModeEnum describeMode = context.getDescribeMode(projection);
        
//        final IDescribeCache describeCache = context.getDescribeCache();
//        
//        if (describeCache != null) {

        /**
         * We need to keep the projection so we can correlate the original
         * variables for the resources that are being described with the
         * bindings on those variables in order to figure out what resources
         * were described when we are maintaining a DESCRIBE cache.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/584">
         *      DESCRIBE CACHE </a>
         */
        /**
         * We need to keep the projection since the DescribeMode annotation is
         * attached to the projection.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/578">
         *      Concise Bounded Description </a>
         */

        projection.setReduced(true);

//        } else {
//         
//            // remove the projection.
//            queryRoot.setProjection(null);
//            
//        }

        queryRoot.setConstruct(construct); // add CONSTRUCT node.

        final Collection<TermNode> terms = new LinkedHashSet<TermNode>();

        if (projection.isWildcard()) {
         
            /*
             * The projection node should have been rewritten first.
             */
            
            throw new AssertionError("Wildcard projection was not rewritten.");
            
        }

        if (projection.isEmpty()) {

            throw new RuntimeException(
                    "DESCRIBE, but no variables are projected.");

        }

        /*
         * Note: A DESCRIBE may have only variables and constants in the
         * projection. Generalized value expressions are not allowed.
         */

        for (AssignmentNode n : projection) {

            terms.add((TermNode) n.getValueExpressionNode());

        }

        final BigdataURI rdfSubject;
        if (describeMode.isForward() && describeMode.isReifiedStatements()) {
            /*
             * We will need to look for the rdf:subject property, so resolve
             * that against the lexicon. If the property is not in the lexicon
             * then there can not be any reified statement models in the data.
             * 
             * TODO REIFICATION DONE RIGHT : handle this differently if we are
             * inlining statements about statements.
             */

            final AbstractTripleStore db = context.getAbstractTripleStore();

            final BigdataURI tmp = db.getValueFactory().asValue(RDF.SUBJECT);

            db.getLexiconRelation().addTerms(new BigdataValue[] { tmp },
                    1/* numTerms */, true/* readOnly */);

            if (tmp.getIV() == null) {
                // Unknown.
                rdfSubject = null;
            } else {
                rdfSubject = tmp;
                // and set the valueCache on the BigdataURI.
                rdfSubject.getIV().setValue(rdfSubject);
            }

        } else {
            
            // Not interested in reified statement models.
            rdfSubject = null;
            
        }

		int i = 0;
		
		for (TermNode term : terms) {
			
			final int termNum = i++;
			
			if(describeMode.isForward())
			{ // <term> ?pN-a ?oN
			
                /*
                 * Note: Each statement has to be in a different part of the
                 * UNION. Also, note that we do not allow a bare statement
                 * pattern in a union. The statement pattern has to be embedded
                 * within a group.
                 */
                final StatementPatternNode sp = new StatementPatternNode(//
                        term, //
                        new VarNode("p" + termNum + "a"),//
                        new VarNode("o" + termNum)//
                );

				construct.addChild(sp);
				
				final JoinGroupNode group = new JoinGroupNode();
				group.addChild(sp);
                union.addChild(group);
				
//				union.addChild(sp);
				
			}

			if(describeMode.isReverse())
			{ // ?sN ?pN-b <term>
			
                final StatementPatternNode sp = new StatementPatternNode(//
                        new VarNode("s" + termNum),//
                        new VarNode("p" + termNum + "b"),//
                        term//
                );

				construct.addChild(sp);

                final JoinGroupNode group = new JoinGroupNode();
                group.addChild(sp);
                union.addChild(group);

//				union.addChild(sp);
				
			}

            if (describeMode.isForward() && describeMode.isReifiedStatements()) {
                /*
                 * Pick up properties associated with reified statement models
                 * where the value of an rdf:subject assertion is the resource
                 * to be described.
                 * 
                 * ?stmtN rdf:subject <term>
                 */
                final StatementPatternNode sp = new StatementPatternNode(//
                        new VarNode("stmt" + termNum),//
                        new ConstantNode(rdfSubject.getIV()),//
                        term//
                );

                construct.addChild(sp);

                final JoinGroupNode group = new JoinGroupNode();
                group.addChild(sp);
                union.addChild(group);

            }
			
		}
		
		return new QueryNodeWithBindingSet(queryRoot, bindingSet);
		
	}
	
}
