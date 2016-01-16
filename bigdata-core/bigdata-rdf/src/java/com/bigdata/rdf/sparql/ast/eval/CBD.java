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
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.DescribeModeEnum;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.optimizers.ASTDescribeOptimizer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Utility class for computing the Concise Bounded Description.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/578"> Concise
 *      Bounded Description </a>
 * @see <a href="http://www.w3.org/Submission/CBD/"> CBD - Concise Bounded
 *      Description </a>
 * @see ASTDescribeOptimizer
 * @see ASTConstructIterator
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         FIXME Watch a timeout on the top-level query (if present)
 */
public class CBD {

    private static final Logger log = Logger.getLogger(CBD.class);
   
    /** The {@link AbstractTripleStore}. */
    private final AbstractTripleStore store;
    
    /**
     * The {@link DescribeModeEnum} specifying how to evaluate the top-level
     * DESCRIBE query.
     */
    private final DescribeModeEnum describeMode;

    /**
     * The limit on the #of iterations (iff the statement limit is also
     * reached) -or- ZERO (0) for no limit.
     */
    private final int describeIterationLimit;
    
    /**
     * The limit on the #of statements (iff the iteration limit is also
     * reached) -or- ZERO (0) for no limit.
     */
    private final int describeStatementLimit;
    
    /**
     * The {@link DescribeModeEnum} specifying how to evaluate each expansion
     * round of the DESCRIBE query.
     */
    private final DescribeModeEnum describeExpansionMode;

    /**
     * A mapping that is used to preserve a consistent assignment from blank
     * node IDs to {@link BigdataBNode}s scoped to the subgraph reported by the
     * top-level DESCRIBE query.
     */
    private final Map<String, BigdataBNode> bnodes;
    
    /**
     * @param store
     *            The {@link AbstractTripleStore}.
     * @param describeMode
     *            The {@link DescribeModeEnum} specifying how to evaluate the
     *            DESCRIBE query.
     * @param describeIterationLimit
     *            The limit on the #of iterations (iff the statement limit is
     *            also reached) -or- ZERO (0) for no limit.
     * @param describeStatementLimit
     *            The limit on the #of statements (iff the iteration limit is
     *            also reached) -or- ZERO (0) for no limit..
     * @param bnodes
     *            A mapping that is used to preserve a consistent assignment
     *            from blank node IDs to {@link BigdataBNode}s scoped to the
     *            subgraph reported by the top-level DESCRIBE query.
     */
    public CBD(final AbstractTripleStore store,
            final DescribeModeEnum describeMode,
            final int describeIterationLimit,
            final int describeStatementLimit,
            final Map<String, BigdataBNode> bnodes) {

        if (store == null)
            throw new IllegalArgumentException();
        
        if (describeMode == null)
            throw new IllegalArgumentException();
        
        if (describeIterationLimit < 0)
            throw new IllegalArgumentException();
        
        if (describeStatementLimit < 0)
            throw new IllegalArgumentException();
        
        if (bnodes == null)
            throw new IllegalArgumentException();

        this.store = store;
        
        this.describeMode = describeMode;

        this.describeIterationLimit = describeIterationLimit;
        
        this.describeStatementLimit = describeStatementLimit;
        
        this.bnodes = bnodes;
        
        switch(describeMode) {
        case CBD:
//        case CBDNR:
            // Expansion only explores the forward links.
            describeExpansionMode = DescribeModeEnum.ForwardOneStep;
            break;
        case SCBD:
//        case SCBDNR:
            // Expansion explores both forward and reverse links.
            describeExpansionMode = DescribeModeEnum.SymmetricOneStep;
            break;
        case ForwardOneStep:
        case SymmetricOneStep:
            // There are no expansion steps for these modes.
            throw new UnsupportedOperationException();
        default:
            // Unknown describe mode.
            throw new AssertionError();
        }
        
    }
    
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
     */
    CloseableIteration<BigdataStatement, QueryEvaluationException> computeClosure(
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

            // #of statements on entry to this round.
            final int nstmts = stmts.size();

            if (cutoffQuery(nrounds - 1, nstmts)) {
                src.close();
                throw new QueryEvaluationException("CBD cutoff: nrounds="
                        + nrounds + ", nstatements=" + nstmts + ".");
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

            if (log.isInfoEnabled()) {
                log.info("#rounds=" + nrounds + ", describeMode="
                        + describeMode + ", #stmts(in)=" + stmts.size()
                        + ", #bnodes(in)=" + bnodes_tm1.size()
                        + ", #bnodes(new)=" + newBnodes.size() + " : "
                        + newBnodes);

                // Conditional logging.
                logState(stmts, bnodes_tm1, newBnodes);

            }

            src = doRound(newBnodes);
            
            // All of these blank nodes have been resolved.
            bnodes_tm1.addAll(newBnodes);
        
        }

        // Done.
        
        if (log.isInfoEnabled()) {
        
            log.info("#rounds=" + nrounds + " (done), describeMode="
                    + describeMode + ", #stmts(in)=" + stmts.size()
                    + ", #bnodes(in)=" + bnodes_tm1.size());
            
            // Conditional logging.
            logState(stmts, bnodes_tm1, null/* newBNodes */);
            
        }

        /*
         * Stream out the fixed point collection of statements that are the
         * Concise Bounded Description of the resources identified in/by the
         * top-level DESCRIBE query.
         */

        return new CollectionIteration<BigdataStatement, QueryEvaluationException>(
                stmts);

    }

    /**
     * Return <code>true</code> iff the DESCRIBE query should be cutoff because
     * the limits have been exceeded.
     * 
     * @param nrounds
     *            The #of evaluation rounds that have already been computed and
     *            ZERO (0) if this is the ffirst round.
     * @param nstmts
     *            The #of statements at the start of this round.
     *            
     * @return <code>true</code> iff evaluation should be cutoff.
     */
    private boolean cutoffQuery(int nrounds, int nstmts) {

        // ZERO implies MAX_INT
        final int describeIterationLimit = this.describeIterationLimit == 0 ? Integer.MAX_VALUE
                : this.describeIterationLimit;

        final int describeStatementLimit = this.describeStatementLimit == 0 ? Integer.MAX_VALUE
                : this.describeStatementLimit;
        
        final boolean cutoffRounds = nrounds >= describeIterationLimit;

        final boolean cutoffStatements = nstmts >= describeStatementLimit;

        return cutoffRounds && cutoffStatements;

    }

    /**
     * Log the statements and bnode {@link IV}s @ DEBUG.
     * 
     * @param stmts
     *            The statements.
     * @param bnodes_tm1
     *            The bnode {@link IV}s from the last round (initially empty).
     * @param newBnodes
     *            The bnode {@link IV}s (optional and <code>null</code> if we
     *            are done).
     */
    private void logState(final Set<BigdataStatement> stmts,
            final Set<IV<?, ?>> bnodes_tm1, final Set<IV<?, ?>> newBnodes) {

        if (!log.isDebugEnabled())
            return;

        final StringBuilder sb = new StringBuilder(stmts.size() * 100);
        {
            sb.append("Statements: ("+stmts.size()+")\n");
            for (BigdataStatement st : stmts) {
                sb.append(st.toString());
                sb.append("\n");
            }
            log.debug(sb.toString());
        }
        {
            sb.setLength(0);// truncate.
            sb.append("BNodes(t-1): ("+bnodes_tm1.size()+")\n");
            for (IV<?, ?> iv : bnodes_tm1) {
                sb.append(iv.toString());
                sb.append("\n");
            }
            log.debug(sb.toString());
        }
        if (newBnodes != null) {
            sb.setLength(0);// truncate.
            sb.append("BNodes(new): ("+newBnodes.size()+")\n");
            for (IV<?, ?> iv : newBnodes) {
                sb.append(iv.toString());
                sb.append("\n");
            }
            log.debug(sb.toString());
        }

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

//                /*
//                 * A statement of the form
//                 * 
//                 * ?stmtN rdf:subject <term>
//                 * 
//                 * where <term> is a blank node.
//                 */
//                final boolean foo = stmt.getPredicate().equals(RDF.SUBJECT)
//                        && bnodes_tm1.contains(stmt.getObject());
                
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
     * @param bnodeIVs
     *            The blank nodes that need to be described.
     * @return An iterator from which the description of those blank nodes may
     *         be read.
     * @throws QueryEvaluationException
     */
    private CloseableIteration<BigdataStatement, QueryEvaluationException> doRound(
            final Set<IV<?, ?>> bnodeIVs) throws QueryEvaluationException {

        /*
         * Create the DESCRIBE query for the blank node IVs.
         */
        final ASTContainer astContainer = getDescribeQuery(bnodeIVs);
        
        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        // Clear the optimized AST.
        astContainer.clearOptimizedAST();
        
        // Batch resolve Values to IVs and convert to bigdata binding set.
        final IBindingSet[] bindingSets = 
              new IBindingSet[] { new ListBindingSet() };

        // Convert the query (generates an optimized AST as a side-effect).
        AST2BOpUtility.convert(context, bindingSets);

        // The optimized AST.
        final QueryRoot optimizedQuery = astContainer.getOptimizedAST();
        
        if (log.isDebugEnabled()) {
            log.debug("describeMode=" + describeMode + ", expansionMode="
                    + describeExpansionMode);
            log.debug("OriginalAST: " + astContainer.getOriginalAST());
            log.debug("OptimizedAST: " + optimizedQuery);
        }

        final boolean materializeProjectionInQuery = context.materializeProjectionInQuery
                && !optimizedQuery.hasSlice();

        // Solutions to the WHERE clause (as projected).
        final CloseableIteration<BindingSet, QueryEvaluationException> solutions = ASTEvalHelper
                .evaluateQuery(astContainer, context, materializeProjectionInQuery//
                        , optimizedQuery.getProjection().getProjectionVars()//
                );

        // Constructed Statements.
        final CloseableIteration<BigdataStatement, QueryEvaluationException> src =
                new ASTConstructIterator(context, store, //
                        optimizedQuery.getConstruct(), //
                        optimizedQuery.getWhereClause(),//
                        bnodes,//
                        solutions//
                        );

        return src;
        
    }

    /**
     * Generate a DESCRIBE query for one of the expansion rounds.
     * 
     * @param bnodeIVs
     *            The blank nodes that need to be described.
     * @return The {@link ASTContainer} wrapping that DESCRIBE query.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private ASTContainer getDescribeQuery(final Set<IV<?, ?>> bnodeIVs) {

        /*
         * Ensure that the bnode IVs are resolved to the corresponding
         * BigdataBlankNode objects and that the valueCache relation is set on
         * those bnode IVs.
         */
        final Map<IV<?, ?>, BigdataValue> terms = store.getLexiconRelation()
                .getTerms(bnodeIVs);

        for (Map.Entry<IV<?, ?>, BigdataValue> e : terms.entrySet()) {

            ((IV) e.getKey()).setValue(e.getValue());

        }

        final QueryRoot queryRoot = new QueryRoot(QueryType.DESCRIBE);
        {

            final ProjectionNode projection = new ProjectionNode();
            queryRoot.setProjection(projection);
         
            /*
             * Specify the describe mode appropriate for the expansion given the
             * top-level describe algorithm that we are evaluating.
             */
            projection.setDescribeMode(describeExpansionMode);
            
            int i=1;
            for (IV<?, ?> iv : bnodeIVs) {

                if (!iv.hasValue())
                    throw new AssertionError("valueCache not set : " + iv);

                final VarNode anonvar = new VarNode("-cbd-bnode-"+i++);

                anonvar.setAnonymous(true);

                projection.addProjectionExpression(new AssignmentNode(anonvar,
                        new ConstantNode(iv)));

            }

        }

        return new ASTContainer(queryRoot);

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
         * No. What we really need to do is ensure that the blank node variables
         * (a) have their valueCache set when we create the DESCRIBE query for
         * the expansion rounds; and (b) are projected out of the CONSTRUCT
         * queries that are evaluated in the expansion rounds. That will ensure
         * that the blank nodes are materialized.
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
