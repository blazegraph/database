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
 * Created on Aug 28, 2012
 */
package com.bigdata.rdf.sparql.ast.eval;

import info.aduna.iteration.CloseableIteration;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.iterator.CollectionIteration;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Utility class for computing the Concise Bounded Description.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/578"> Concise
 *      Bounded Description </a>
 * @see <a href="http://www.w3.org/Submission/CBD/"> CBD - Concise Bounded
 *      Description </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class CBD {

    private static final Logger log = Logger.getLogger(CBD.class);

    /**
     * The description of the original resource(s) is expanded for each blank
     * node encountered in the constructed statements until no new blank nodes
     * are encountered. The resulting set of statements is then reported as the
     * description for the resources identified either as constants in the
     * projection of the original query or as variables in the projection of the
     * original query that became bound in the WHERE clause of that original
     * query.
     * 
     * @param src
     *            An iterator from which we can drain the solutions to the
     *            top-level DESCRIBE query. This is the input into the CBD
     *            expansion.
     * 
     * @return An iterator from which the concise bounded description of the may
     *         be drained.
     * 
     * @throws QueryEvaluationException
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/578">
     *      Concise Bounded Description </a>
     * @see <a href="http://www.w3.org/Submission/CBD/"> CBD - Concise Bounded
     *      Description </a>
     * 
     *      TODO We might need to insist that the ASTConstructIterator pass
     *      through the original IV for the blank node by setting the original
     *      IV on the blank node.
     * 
     *      TODO There are some cases where the ASTConstructIterator can create
     *      new blank nodes. Can any of those cases arise for a DESCRIBE? If so,
     *      then it needs to use a consistent assignment.
     */
    static CloseableIteration<BigdataStatement, QueryEvaluationException> conciseBoundedDescription(
            final AbstractTripleStore store,
            CloseableIteration<BigdataStatement, QueryEvaluationException> src)
            throws QueryEvaluationException {

        // Round ZERO (0) is the top-level describe.
        int nrounds = 0;
        // The blank nodes identified in the previous round.
        final Set<IV<?,?>> bnodes_tm1 = new LinkedHashSet<IV<?,?>>();
        // The statements identified so far.
        final Set<BigdataStatement> stmts = new LinkedHashSet<BigdataStatement>();
        while (true) {

            // CBD expansion begins at round ONE (1).
            nrounds++;

            if (nrounds > 10) {
                /*
                 * TODO Limit on #of rounds or maybe just watch a timeout on the
                 * top-level query (if present)?
                 */
                src.close();
                throw new QueryEvaluationException(
                        "CSB would exceed 10 rounds.");
            }

            /*
             * Build a collection of all distinct statements and all distinct
             * blank node IVs encountered in the source statements. Any of the
             * (s,o,c) positions can be blank nodes.
             */

            // The blank nodes IVs identified in this round that were NOT known
            // in the previous round(s).
            final Set<IV<?, ?>> newBnodes = consumeStatements(src, stmts,
                    bnodes_tm1);
            
            if (newBnodes.isEmpty()) {

                // All done.
                break;

            }

            /*
             * We will have to do another CBD round since there is at least one
             * new blank node IV that needs to be described.
             */

            if (log.isInfoEnabled())
                log.info("#rounds=" + nrounds + ", #stmts(in)=" + stmts.size()
                        + ", #bnodes(in)=" + bnodes_tm1.size()
                        + ", #bnodes(new)=" + newBnodes.size() + " : "
                        + newBnodes);

            src = doRound(store, newBnodes);
            
            // All of these blank nodes have been resolved.
            bnodes_tm1.addAll(newBnodes);
        
        }

        // Done.
        
        if (log.isInfoEnabled())
            log.info("#rounds=" + nrounds + ", #stmts(in)=" + stmts.size()
                    + ", #bnodes(in)=" + bnodes_tm1.size());

        /*
         * Stream out the fixed point collection of statements that are the
         * Concise Bounded Description of the resources identified in/by the
         * top-level DESCRIBE query.
         */

        return new CollectionIteration<BigdataStatement, QueryEvaluationException>(
                stmts);

    }

    /**
     * Consume statements from the source iterator, adding new statements into a
     * collection and adding new blank node {@link IV}s into another collection.
     * 
     * @param src
     *            The statements to be consumed.
     * @param stmts
     *            The set of statements in the description of the resources from
     *            the previous round(s) (if any).
     * @param bnodes_tm1
     *            The blank node {@link IV}s already known on entry to the
     *            current round. This is empty for on entry to the first
     *            expansion round.
     * 
     * @return The set of blank node {@link IV}s not previous encountered in the
     *         CBD expansion.
     * 
     * @throws QueryEvaluationException
     */
    private static Set<IV<?, ?>> consumeStatements(
            final CloseableIteration<BigdataStatement, QueryEvaluationException> src,
            final Set<BigdataStatement> stmts, final Set<IV<?, ?>> bnodes_tm1)
            throws QueryEvaluationException {

        final Set<IV<?, ?>> newBnodes = new LinkedHashSet<IV<?, ?>>();
        try {

            while (src.hasNext()) {

                final BigdataStatement stmt = src.next();

                if (stmts.add(stmt)) {

                    /*
                     * New blank node IVs can only be encountered for new
                     * statements.
                     * 
                     * TODO Consider using an ISPO => BigdataStatement map for
                     * the statements so we can avoid duplicate entries for
                     * BigdataStatements having different blank nodes but the
                     * same IVs for those blank nodes.
                     */
                    
                    collectBNodeIVs(bnodes_tm1, newBnodes,
                            getBNodeIV(stmt.getSubject()));

                    collectBNodeIVs(bnodes_tm1, newBnodes,
                            getBNodeIV(stmt.getObject()));

                    collectBNodeIVs(bnodes_tm1, newBnodes,
                            getBNodeIV(stmt.getContext()));

                }

            }
            
            return newBnodes;

        } finally {

            src.close();

        }

    }
    
    /**
     * Create a new DESCRIBE query to describe each new blank node identifier in
     * the previous round. We need to tunnel the evaluation of the DESCRIBE
     * query in order to: (a) ensure that the blank node {@link IV}s are
     * attached to the blank nodes in the DESCRIBE clause; and (b) avoid the
     * describe cache materialization logic since rounds GT ZERO (0) are not
     * top-level DESCRIBE queries and do not describe top-level resources.
     * 
     * @param store
     *            The triple store.
     * @param bnodeIVs
     *            The blank nodes that need to be described.
     * @return An iterator from which the description of those blank nodes may
     *         be read.
     * @throws QueryEvaluationException
     */
    private static CloseableIteration<BigdataStatement, QueryEvaluationException> doRound(
            final AbstractTripleStore store, final Set<IV<?, ?>> bnodeIVs)
            throws QueryEvaluationException {

        /*
         * Create the DESCRIBE query for the blank node IVs.
         */
        final ASTContainer astContainer;
        {

            /*
             * Ensure that the bnode IVs are resolved to the corresponding
             * BigdataBlankNode objects and that the valueCache relation is set
             * on those bnode IVs.
             */
            final Map<IV<?, ?>, BigdataValue> terms = store
                    .getLexiconRelation().getTerms(bnodeIVs);
            
            for (Map.Entry<IV<?, ?>, BigdataValue> e : terms.entrySet()) {

                ((IV)e.getKey()).setValue(e.getValue());
                
            }

            final QueryRoot queryRoot = new QueryRoot(QueryType.DESCRIBE);
            {

                final ProjectionNode projection = new ProjectionNode();
                queryRoot.setProjection(projection);

                for (IV<?, ?> iv : bnodeIVs) {

                    if (!iv.hasValue())
                        throw new AssertionError("valueCache not set : " + iv);

                    final VarNode anonvar = new VarNode("-cbd-bnode-1");
                    
                    anonvar.setAnonymous(true);
                    
                    projection.addProjectionExpression(new AssignmentNode(
                            anonvar, new ConstantNode(iv)));
                    
                }
                
            }

            astContainer = new ASTContainer(queryRoot);

        }
        
        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        // Clear the optimized AST.
        astContainer.clearOptimizedAST();
        
        // Batch resolve Values to IVs and convert to bigdata binding set.
        final IBindingSet[] bindingSets = ASTEvalHelper.mergeBindingSets(astContainer,
                ASTEvalHelper.batchResolveIVs(store, null/* no-bindings */));

        // Convert the query (generates an optimized AST as a side-effect).
        AST2BOpUtility.convert(context, bindingSets);

        // The optimized AST.
        final QueryRoot optimizedQuery = astContainer.getOptimizedAST();
        
        if (log.isDebugEnabled()) {
            log.debug("OriginalAST: " + astContainer.getOriginalAST());
            log.debug("OptimizedAST: " + optimizedQuery);
        }

        final boolean materializeProjectionInQuery = context.materializeProjectionInQuery
                && !optimizedQuery.hasSlice();

        // Solutions to the WHERE clause (as projected).
        final CloseableIteration<BindingSet, QueryEvaluationException> solutions = ASTEvalHelper
                .evaluateQuery(astContainer, context, bindingSets//
                        , materializeProjectionInQuery//
                        , optimizedQuery.getProjection().getProjectionVars()//
                );

        // Constructed Statements.
        final CloseableIteration<BigdataStatement, QueryEvaluationException> src =
                new ASTConstructIterator(store, //
                        optimizedQuery.getConstruct(), //
                        optimizedQuery.getWhereClause(),//
                        solutions);

        return src;
        
    }

    /**
     * Collect blank nodes {@link IV}s not already declared in a previous round.
     * 
     * @param bnodes_tm1
     *            The blank node {@link IV}s already declared in the previous
     *            round(s).
     * @param newBnodes
     *            The set of blank node {@link IV}s that were discovered in this
     *            round.
     * @param bNodeIV
     *            A blank node {@link IV} (may be <code>null</code>).
     */
    private static void collectBNodeIVs(final Set<IV<?, ?>> bnodes_tm1,
            final Set<IV<?, ?>> newBnodes, final IV<?, ?> bNodeIV) {
        
        if (bNodeIV == null) {

            /* The corresponding position in the statement was not a blank node. */
            return;

        }

        if (bnodes_tm1.contains(bNodeIV)) {
            
            /* This blank node was already declared in the previous round. */
            return;
            
        }

        newBnodes.add(bNodeIV);
        
    }

    /**
     * If the value is a blank node, then return the IV for that blank node and
     * otherwise return <code>null</code>.
     * 
     * @param v
     *            The value.
     *            
     * @return The {@link IV} for that blank node and <code>null</code> iff the
     *         value is not a blank node.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    static private IV<?,?> getBNodeIV(final BigdataValue v) {

        if (v == null) {

            /*
             * Note: This case is allowed for the context position of the
             * statement.
             */
            return null;

        }

        final BigdataBNode bnode = (BigdataBNode) ((v instanceof BigdataBNode) ? v
                : null);

        if (bnode == null) {

            // Not a blank node.
            return null;

        }

        final IV<?, ?> iv = bnode.getIV();

        /*
         * FIXME No. What we really need to do is ensure that the blank node
         * variables are projected out of the CONSTRUCT queries that are
         * evaluated in the expansion rounds. That will ensure that the blank
         * nodes are materialized. (I am hitting a problem where blank nodes
         * that were given as inputs to the CONSTRUCT are not materialized in
         * the query).
         * 
         * OR We can resolve the blank node IVs to blank nodes when we setup the
         * expansion round DESCRIBE query.
         */
//        /*
//         * We need to set the valueCache relation on the IV. The
//         * ASTConstructIterator depends on this in makeStatement().
//         */
//        
//        ((IV) iv).setValue(bnode);

        return iv;
        
    }
    
}
