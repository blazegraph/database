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
 * Created on Mar 17, 2012
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.algebra.StatementPattern.Scope;
//import org.openrdf.query.impl.MutableTupleQueryResult;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.sail.SailException;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.rdf.update.ChunkedResolutionOp;
import com.bigdata.bop.rdf.update.CommitOp;
import com.bigdata.bop.rdf.update.InsertStatementsOp;
import com.bigdata.bop.rdf.update.ParseOp;
import com.bigdata.bop.rdf.update.RemoveStatementsOp;
import com.bigdata.rdf.error.SparqlDynamicErrorException.GraphEmptyException;
import com.bigdata.rdf.error.SparqlDynamicErrorException.GraphExistsException;
import com.bigdata.rdf.error.SparqlDynamicErrorException.SolutionSetDoesNotExistException;
import com.bigdata.rdf.error.SparqlDynamicErrorException.SolutionSetExistsException;
import com.bigdata.rdf.error.SparqlDynamicErrorException.UnknownContentTypeException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.rio.IRDFParserOptions;
import com.bigdata.rdf.rio.RDFParserOptions;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.SPARQLUpdateEvent;
import com.bigdata.rdf.sail.SPARQLUpdateEvent.DeleteInsertWhereStats;
import com.bigdata.rdf.sail.Sesame2BigdataIterator;
import com.bigdata.rdf.sail.webapp.client.MiniMime;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractGraphDataUpdate;
import com.bigdata.rdf.sparql.ast.AddGraph;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.CopyGraph;
import com.bigdata.rdf.sparql.ast.CreateGraph;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.DeleteInsertGraph;
import com.bigdata.rdf.sparql.ast.DropGraph;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.LoadGraph;
import com.bigdata.rdf.sparql.ast.MoveGraph;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QuadData;
import com.bigdata.rdf.sparql.ast.QuadsDataOrNamedSolutionSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.Update;
import com.bigdata.rdf.sparql.ast.UpdateRoot;
import com.bigdata.rdf.sparql.ast.UpdateType;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.BigdataOpenRDFBindingSetsResolverator;
import com.bigdata.striterator.Chunkerator;

import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;
import info.aduna.iteration.CloseableIteration;

/**
 * Class handles SPARQL update query plan generation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AST2BOpUpdate extends AST2BOpUtility {

    private static final Logger log = Logger.getLogger(AST2BOpUpdate.class);

    /**
     * When <code>true</code>, convert the SPARQL UPDATE into a physical
     * operator plan and execute it on the query engine. When <code>false</code>
     * , the UPDATE is executed using the {@link BigdataSail} API.
     * 
     * TODO By coming in through the SAIL, we automatically pick up truth
     * maintenance and related logics. All of that needs to be integrated into
     * the generated physical operator plan before we can run updates on the
     * query engine. However, there will be advantages to running updates on the
     * query engine, including declarative control of parallelism, more
     * similarity for code paths between a single machine and cluster
     * deployments, and a unified operator model for query and update
     * evaluation.
     */
    private final static boolean runOnQueryEngine = false;
    
    /**
     * 
     */
    public AST2BOpUpdate() {
        super();
    }
    
    /**
     * The s,p,o, and c variable names used for binding sets which model
     * {@link Statement}s.
     */
    private static final Var<?> s = Var.var("s"), p = Var.var("p"), o = Var
            .var("o"), c = Var.var("c");

    /**
     * Convert the query
     * <p>
     * Note: This is currently a NOP.
     */
    protected static void optimizeUpdateRoot(final AST2BOpUpdateContext context) {

//        final ASTContainer astContainer = context.astContainer;
//
//        // Clear the optimized AST.
//        // astContainer.clearOptimizedUpdateAST();
//
//        /*
//         * Build up the optimized AST for the UpdateRoot for each Update to
//         * be executed. Maybe do this all up front before we run anything since
//         * we might reorder or regroup some operations (e.g., parallelized LOAD
//         * operations, parallelized INSERT data operations, etc).
//         */
//        final UpdateRoot updateRoot = astContainer.getOriginalUpdateAST();
//
//        /*
//         * Evaluate each update operation in the optimized UPDATE AST in turn.
//         */
//        for (Update op : updateRoot) {
//  
//            ...
//        
//        }

    }

    /**
     * Convert and/or execute the update request.
     * 
     * @throws Exception
     */
    protected static PipelineOp convertUpdate(final AST2BOpUpdateContext context)
            throws Exception {

        if (context.db.isReadOnly())
            throw new UnsupportedOperationException("Not a mutable view.");
       
        if (context.conn.isReadOnly())
            throw new UnsupportedOperationException("Not a mutable view.");
        
        if (log.isTraceEnabled())
            log.trace("beforeUpdate:\n" + context.getAbstractTripleStore().dumpStore());

        final ASTContainer astContainer = context.astContainer;

		/*
		 * Note: Change this to the optimized AST if we start doing AST
		 * optimizations for UPDATE.
		 */
        final UpdateRoot updateRoot = astContainer.getOriginalUpdateAST();
        
        // Set as annotation on the ASTContainer.
        // astContainer.setQueryPlan(left);

        /*
         * Evaluate each update operation in the optimized UPDATE AST in turn.
         */
        PipelineOp left = null;
		int updateIndex = 0;
		for (Update op : updateRoot) {

//          log.error("\nbefore op=" + op + "\n" + context.conn.getTripleStore().dumpStore());

			long connectionFlushNanos = 0L;
			long batchResolveNanos = 0L;
			if (updateIndex > 0) {

				/*
				 * There is more than one update operation in this request.
				 */
				
				/*
				 * Note: We need to flush the assertion / retraction buffers if
				 * the Sail is local since some of the code paths supporting
				 * UPDATEs do not go through the BigdataSail and would otherwise
				 * not have their updates flushed until the commit (which does
				 * go through the BigdataSail).
				 * 
				 * @see https://sourceforge.net/apps/trac/bigdata/ticket/558
				 */
				final long t1 = System.nanoTime();
				context.conn.flush();
				final long t2 = System.nanoTime();
				connectionFlushNanos = t2 - t1;

				/*
				 * We need to re-resolve any RDF Values appearing in this UPDATE
				 * operation which have a 0L term identifier in case they have
				 * become defined through the previous update(s).
				 * 
				 * Note: Since 2.0, also re-resolves RDF Values appearing in the
				 * binding sets or data set.
				 * 
				 * @see BLZG-1176 SPARQL Parsers should not be db mode aware
                 * 
                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/558
				 */
				ASTDeferredIVResolution.resolveUpdate(context.db,
				      op, context.getQueryBindingSet(), context.getDataset());
				
				batchResolveNanos = System.nanoTime() - t2;

			}
			
			final long begin = System.nanoTime();
			final DeleteInsertWhereStats deleteInsertWhereStats = new DeleteInsertWhereStats(); // @see BLZG-1446.
			Throwable cause = null;
            try {
                // convert/run the update operation.
                left = convertUpdateSwitch(left, op, context, deleteInsertWhereStats);
            } catch (Throwable t) {
                cause = t;
                log.error("SPARQL UPDATE failure: op=" + op + ", ex=" + t, t);
                // notify listener(s)
                final long elapsed = System.nanoTime() - begin;
                context.conn.getSailConnection().fireEvent(
                        new SPARQLUpdateEvent(op, elapsed, connectionFlushNanos, batchResolveNanos, cause, deleteInsertWhereStats));
                if (t instanceof Exception)
                    throw (Exception) t;
                if (t instanceof RuntimeException)
                    throw (RuntimeException) t;
                throw new RuntimeException(t);
            }

			final long elapsed = System.nanoTime() - begin;
			
			// notify listener(s)
            context.conn.getSailConnection().fireEvent(
                    new SPARQLUpdateEvent(op, elapsed, connectionFlushNanos, batchResolveNanos, cause, deleteInsertWhereStats));

			updateIndex++;
			
        }

        /*
         * Commit mutation.
         */
        left = convertCommit(left, context);

        if (log.isTraceEnabled())
            log.trace("afterCommit:\n" + context.getAbstractTripleStore().dumpStore());

        return left;

    }

    /**
     * MP: Make SPARQL Update an auto-commit operation by default.
     * 
     * TODO:  Make this private once merged down.
     */
	public static boolean AUTO_COMMIT = Boolean.parseBoolean(System
			.getProperty(AST2BOpBase.Annotations.AUTO_COMMIT, "true"));
    
    /**
     * Commit.
     * <p>
     * Note: Not required on cluster (Shard-wise ACID updates).
     * <p>
     * Note: Not required unless the end of the update sequence or we desire a
     * checkpoint on the sequences of operations.
     * <p>
     * Note: The commit really must not happen until the update plan(s) are
     * known to execute successfully. We can do that with an AT_ONCE annotation
     * on the {@link CommitOp} or we can just invoke commit() at appropriate
     * checkpoints in the UPDATE operation.
     */
    private static PipelineOp convertCommit(PipelineOp left,
            final AST2BOpUpdateContext context) throws Exception {

    	/*
    	 * Note: Since we are using the BigdataSail interface, we DO have to
    	 * do a commit on the cluster.  It is only if we are running on the
    	 * query engine that things could be different (but that requires a
    	 * wholly different plan).
    	 */
//        if (!context.isCluster())
        if (AUTO_COMMIT) {

            if (runOnQueryEngine) {
            
                left = new CommitOp(leftOrEmpty(left), NV.asMap(
                        //
                        new NV(BOp.Annotations.BOP_ID, context.nextId()),//
                        new NV(CommitOp.Annotations.TIMESTAMP, context
                                .getTimestamp()),//
                        new NV(CommitOp.Annotations.PIPELINED, false)//
                        ));
            
            } else {

                final long commitTime = context.conn.commit2();
                
                context.setCommitTime(commitTime);
                
                if (log.isDebugEnabled())
                    log.debug("COMMIT: commitTime=" + commitTime);
                
            }
            
        }
        
        return left;

    }
    
    /**
     * Method provides the <code>switch()</code> for handling the different
     * {@link UpdateType}s.
     * 
     * @param left
     * @param op
     * @param context
     * @return
     * @throws Exception
     */
    private static PipelineOp convertUpdateSwitch(PipelineOp left,
            final Update op, final AST2BOpUpdateContext context,
            final DeleteInsertWhereStats deleteInsertWhereStats)
            throws Exception {

        final UpdateType updateType = op.getUpdateType();

        switch (updateType) {
        case Create: {
            left = convertCreateGraph(left, (CreateGraph) op, context);
            break;
        }
        case Add: {
            // Copy all statements from source to target.
            left = convertAddGraph(left, (AddGraph) op, context);
            break;
        }
        case Copy: {
            // Drop() target, then Add().
            left = convertCopyGraph(left, (CopyGraph) op, context);
            break;
            }
        case Move: {
            // Drop() target, Add(source,target), Drop(source).
            left = convertMoveGraph(left, (MoveGraph) op, context);
            break;
        }
        case Clear:
        case Drop: {
            left = convertClearOrDropGraph(left, (DropGraph) op, context);
            break;
        }
        case InsertData:
        case DeleteData: {
            left = convertInsertOrDeleteData(left, (AbstractGraphDataUpdate) op,
                    context);
            break;
        }
        case Load: {
            left = convertLoadGraph(left, (LoadGraph) op, context);
            break;
        }
        case DeleteInsert: {
            left = convertDeleteInsert(left, (DeleteInsertGraph) op, context, deleteInsertWhereStats);
            break;
        }
        case DropEntailments: {
        	left = convertDropEntailments(left, context);
        	break;
        }
        case CreateEntailments: {
        	left = convertCreateEntailments(left, context);
        	break;
        }
        case EnableEntailments: {
        	left = convertEnableEntailments(left, context);
        	break;
        }
        case DisableEntailments: {
        	left = convertDisableEntailments(left, context);
        	break;
        }
        default:
            throw new UnsupportedOperationException("updateType=" + updateType);
        }

        return left;
        
    }

    /**
     * <pre>
     * ( WITH IRIref )?
     * ( ( DeleteClause InsertClause? ) | InsertClause )
     * ( USING ( NAMED )? IRIref )*
     * WHERE GroupGraphPattern
     * </pre>
     * 
     * @param left
     * @param op
     * @param context
     * @return
     * 
     * @throws QueryEvaluationException
     * @throws RepositoryException 
     * @throws SailException 
     */
    private static PipelineOp convertDeleteInsert(PipelineOp left,
            final DeleteInsertGraph op, final AST2BOpUpdateContext context,
            final DeleteInsertWhereStats deleteInsertWhereStats)
            throws QueryEvaluationException, RepositoryException, SailException {

        if (runOnQueryEngine)
            throw new UnsupportedOperationException();

        /*
         * This models the DELETE/INSERT request as a QUERY. The data from the
         * query are fed into a handler which adds or removes the statements (as
         * appropriate) from the [conn].
         */
        {

            /*
             * Create a new query using the WHERE clause.
             */
            final JoinGroupNode whereClause = new JoinGroupNode(
                    op.getWhereClause());

            final QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);

            queryRoot.setWhereClause(whereClause);

            final DatasetNode dataset = op.getDataset();

            if (dataset != null)
                queryRoot.setDataset(dataset);

            /*
             * Setup the PROJECTION for the new query.
             * 
             * TODO retainAll() for only those variables used in the template
             * for the InsertClause or RemoveClause (less materialization, more
             * efficient).
             */
            {

                final StaticAnalysis sa = new StaticAnalysis(queryRoot,context);

                final Set<IVariable<?>> projectedVars = sa
                        .getMaybeProducedBindings(whereClause,
                                new LinkedHashSet<IVariable<?>>()/* vars */,
                                true/* recursive */);

                for (IBindingSet bs : context.getBindings()) {
                    
                    @SuppressWarnings("rawtypes")
                    final Iterator<IVariable> it = bs.vars();
                    
                    while (it.hasNext()) {
                        
                        projectedVars.add(it.next());
                        
                    }
                    
                }

                final ProjectionNode projection = new ProjectionNode();

                for (IVariable<?> var : projectedVars) {

                    projection.addProjectionVar(new VarNode(var.getName()));

                }
                
                queryRoot.setProjection(projection);
                
            }

            final ASTContainer astContainer = new ASTContainer(queryRoot);
            /*
             * Inherit 'RESOLVED' flag, so resolution will not be run
             * for ASTContainer, constracted from parts of already resolved update.
             */
            astContainer.setProperty(ASTContainer.Annotations.RESOLVED, context.astContainer.getProperty(ASTContainer.Annotations.RESOLVED));

            final QuadsDataOrNamedSolutionSet insertClause = op.getInsertClause();

            if (insertClause == null && op.getDeleteClause() == null) {
                
                /*
                 * DELETE WHERE QuadPattern
                 * 
                 * We need to build the appropriate CONSTRUCT clause from the
                 * WHERE clause.
                 * 
                 * Note: This could be lifted into an AST optimizer, but we are
                 * not yet running those against the UPDATE AST.
                 */
                
                final QuadData deleteTemplate = new QuadData();
                
                final Iterator<StatementPatternNode> itr = BOpUtility.visitAll(
                        whereClause, StatementPatternNode.class);

                while (itr.hasNext()) {

                    final StatementPatternNode t = (StatementPatternNode) itr
                            .next().clone();

                    deleteTemplate.addChild(t);

                }

                final QuadsDataOrNamedSolutionSet deleteClause = new QuadsDataOrNamedSolutionSet(
                        deleteTemplate);

                op.setDeleteClause(deleteClause);

            }

            final QuadsDataOrNamedSolutionSet deleteClause = op.getDeleteClause();

            // Just the insert clause.
            /*
             * TODO FIXME Forcing all updates through the delete+insert code path.
             * 
             * https://jira.blazegraph.com/browse/BLZG-1913
             */
            final boolean isInsertOnly = false; //insertClause != null && deleteClause == null;

//            // Just the delete clause.
//            final boolean isDeleteOnly = insertClause == null
//                    && deleteClause != null;

            // Both the delete clause and the insert clause.
            /*
             * TODO FIXME Forcing all updates through the delete+insert code path.
             * 
             * https://jira.blazegraph.com/browse/BLZG-1913
             */
            final boolean isDeleteInsert = true; //insertClause != null && deleteClause != null;
            
            /*
             * Run the WHERE clause.
             */

            if (isDeleteInsert) {
                
                /*
				 * DELETE + INSERT.
				 * 
				 * Note: The semantics of DELETE + INSERT are that the WHERE
				 * clause is executed once. The solutions to that need to be fed
				 * once through the DELETE clause. After the DELETE clause has
				 * been processed for all solutions to the WHERE clause, the
				 * INSERT clause is then processed. So, we need to materialize
				 * the WHERE clause results when both the DELETE clause and the
				 * INSERT clause are present.
				 * 
				 * FIXME For large intermediate results, we would be much better
				 * off putting the data onto an HTree (or, better yet, a chain
				 * of blocks) and processing the bindings as IVs rather than
				 * materializing them as RDF Values (and even for small data
				 * sets, we would be better off avoiding materialization of the
				 * RDF Values and using an ASTConstructIterator which builds
				 * ISPOs using IVs rather than Values).
				 * 
				 * Note: Unlike operations against a graph, we do NOT perform
				 * truth maintenance for updates against solution sets,
				 * therefore we could get by nicely with operations on
				 * IBindingSet[]s without RDF Value materialization.
				 * 
				 * @see https://sourceforge.net/apps/trac/bigdata/ticket/524
				 * (SPARQL Cache)
				 */

				final LexiconRelation lexicon = context
						.getAbstractTripleStore().getLexiconRelation();

				final int chunkSize = 100; // TODO configure.
				
				/*
				 * Run as a SELECT query.
				 * 
				 * Note: This *MUST* use the view of the tripleStore which is
				 * associated with the SailConnection in case the view is
				 * isolated by a transaction.
				 */

				// Note: Blocks until the result set is materialized.
				final long beginWhereClauseNanos = System.nanoTime();
				final MutableTupleQueryResult result = new MutableTupleQueryResult(
						ASTEvalHelper.evaluateTupleQuery(
								context.conn.getTripleStore(), astContainer,
								context.getQueryBindingSet()/* bindingSets */, null /* dataset */));
				deleteInsertWhereStats.whereNanos.set(System.nanoTime() - beginWhereClauseNanos);
				
				// If the query contains a nativeDistinctSPO query hint then
				// the line below unfortunately isolates the query so that the hint does
				// not impact any other execution, this is hacked by putting a property on the query root.
				
				final boolean nativeDistinct = astContainer.getOptimizedAST().getProperty(ConstructNode.Annotations.NATIVE_DISTINCT,
						ConstructNode.Annotations.DEFAULT_NATIVE_DISTINCT);

                try {

                    // Play it once through the DELETE clause.
                    if (deleteClause != null) {
                    	final long beginDeleteNanos = System.nanoTime();
                    	
                        // rewind.
                        result.beforeFirst();

                        // Figure out if operating on solutions or graphs.
                        final boolean isSolutionSet = deleteClause.isSolutions();

						if (isSolutionSet) {

                            /*
                             * Target is solution set.
                             * 
                             * @see
                             * https://sourceforge.net/apps/trac/bigdata/ticket
                             * /524 (SPARQL Cache)
                             * 
                             * FIXME [Is this fixed now?] The DELETE+INSERT code
                             * path is failing because it is based on the DELETE
                             * FROM SELECT code path below and attempts to
                             * rewrite the query to use a MINUS operator.
                             * However, the setup is different in this case
                             * since we have already run the original WHERE
                             * clause into a rewindable tuple result set.
                             * 
                             * The best way to fix this would be to stay within
                             * the native IBindingSet[] model and write the
                             * solutions from the WHERE clause onto a chained
                             * list of blocks, just as we do when writing on a
                             * named solution set (or an htree with appropriate
                             * join variables). That could then be joined into
                             * the query with an INCLUDE. Since we do not want
                             * this "temporary" solution set to be visible, we
                             * could prefix it with a UUID and make sure that it
                             * is written onto a memory manager, and also make
                             * sure that we eventually delete the named solution
                             * set since it should be temporary.
                             */

							// The named solution set on which we will write.
							final String solutionSet = deleteClause.getName();

							// A unique named solution set used to INCLUDE the
							// solutions to be deleted.
							final String tempSolutionSet = "-" + solutionSet
									+ "-" + UUID.randomUUID();

							// Write solutions to be deleted onto temp set.
							context.solutionSetManager.putSolutions(
									tempSolutionSet,
									asBigdataIterator(lexicon, chunkSize,
											result));

							try {

								/*
								 * Replace WHERE clause with an join group
								 * containing an INCLUDE for the solutions to be
								 * removed.
								 * 
								 * WHERE := { INCLUDE %namedSet MINUS {INCLUDE %temp} }
								 */
//								final JoinGroupNode oldWhereClause = (JoinGroupNode) queryRoot
//										.getWhereClause();
						    	    
								final JoinGroupNode newWhereClause = new JoinGroupNode();
								queryRoot.setWhereClause(newWhereClause);
						    	    
								// Include the source solutions.
								newWhereClause.addArg(new NamedSubqueryInclude(
										solutionSet));
								
								// MINUS solutions to be removed.
								final JoinGroupNode minusOp = new JoinGroupNode(
										new NamedSubqueryInclude(
												tempSolutionSet));
								newWhereClause.addArg(minusOp);
								minusOp.setMinus(true);

//						    	    log.error("oldWhereClause="+oldWhereClause);
//						    	    log.error("newWhereClause="+newWhereClause);

//								/*
//								 * Re-write the AST to handle DELETE solutions.
//								 */
//								convertQueryForDeleteSolutions(queryRoot,
//										solutionSet);

								// Set the projection node.
								queryRoot.setProjection(deleteClause
										.getProjection());

								/*
								 * Run as a SELECT query : Do NOT materialize
								 * IVs.
								 * 
								 * Note: This *MUST* use the view of the
								 * tripleStore which is associated with the
								 * SailConnection in case the view is isolated
								 * by a transaction.
								 */
								final ICloseableIterator<IBindingSet[]> titr = ASTEvalHelper
										.evaluateTupleQuery2(
												context.conn.getTripleStore(),
												astContainer,
												context.getQueryBindingSet()/* bindingSets */, false/* materialize */);

								try {

									// Write onto named solution set.
									context.solutionSetManager.putSolutions(
											solutionSet, titr);

								} finally {

									titr.close();

								}

							} finally {

								/*
								 * Make sure that we do not leave this hanging
								 * around.
								 */

								context.solutionSetManager
										.clearSolutions(tempSolutionSet);

							}

						} else {

							/*
							 * DELETE triples/quads constructed from the
							 * solutions.
							 */
							
							final ConstructNode template = op.getDeleteClause()
									.getQuadData().flatten(new ConstructNode(context));
							
							template.setDistinctQuads(true);
							
							if (nativeDistinct) {
								template.setNativeDistinct(true);
							}

                            final ASTConstructIterator itr = new ASTConstructIterator(
                                    context,//
                                    context.conn.getTripleStore(), template,
                                    op.getWhereClause(), null/* bnodesMap */,
                                    result);

							while (itr.hasNext()) {

								final BigdataStatement stmt = itr.next();

								addOrRemoveStatement(
										context.conn.getSailConnection(), stmt,
										false/* insert */);

							}

						}

						deleteInsertWhereStats.deleteNanos.set(System.nanoTime() - beginDeleteNanos);
						
					} // End DELETE clause.

                    // Play it once through the INSERT clause.
                    if (insertClause != null) {

                    	final long beginInsertNanos = System.nanoTime();
                    	
                        // rewind.
                        result.beforeFirst();

                        // Figure out if operating on solutions or graphs.
                        final boolean isSolutionSet = insertClause.isSolutions();

						if (isSolutionSet) {

							/*
							 * Target is solution set.
							 * 
							 * @see
							 * https://sourceforge.net/apps/trac/bigdata/ticket
							 * /524 (SPARQL Cache)
							 */

							// The named solution set on which we will write.
							final String solutionSet = insertClause.getName();

							// Set the projection node.
							queryRoot.setProjection(insertClause.getProjection());

							final ICloseableIterator<IBindingSet[]> titr = asBigdataIterator(
									lexicon, chunkSize, result);

							try {

								// Write the solutions onto the named solution
								// set.
								context.solutionSetManager.putSolutions(solutionSet,
										titr);

							} finally {

								titr.close();

							}

						} else {
							
							/*
							 * INSERT triples/quads CONSTRUCTed from solutions.
							 */
							
							final ConstructNode template = op.getInsertClause()
									.getQuadData().flatten(new ConstructNode(context));

							template.setDistinctQuads(true);
							
							if (nativeDistinct) {
								template.setNativeDistinct(true);
							}

                            final ASTConstructIterator itr = new ASTConstructIterator(
                                    context,//
                                    context.conn.getTripleStore(), template,
                                    op.getWhereClause(), null/* bnodesMap */,
                                    result);

							while (itr.hasNext()) {

								final BigdataStatement stmt = itr.next();

								addOrRemoveStatement(
										context.conn.getSailConnection(), stmt,
										true/* insert */);

							}

						}

						deleteInsertWhereStats.insertNanos.set(System.nanoTime() - beginInsertNanos);

					} // End INSERT clause

                } finally {

                    // Close the result set.
                    result.close();

                }

            } else {

                
                /*
                 * DELETE/INSERT.
                 * 
                 * Note: For this code path, only the INSERT clause -or- the
                 * DELETE clause was specified. We handle the case where BOTH
                 * clauses were specified above.
                 */
                
                // true iff this is an INSERT
                final boolean isInsert = insertClause != null;
//                final boolean isDelete = deleteClause != null;
  
                // The clause (either for INSERT or DELETE)
                final QuadsDataOrNamedSolutionSet clause = isInsert ? insertClause
                        : deleteClause;
                
                assert clause != null;

                // Figure out if operating on solutions or graphs.
                final boolean isSolutionSet = clause.isSolutions();

                if(isSolutionSet) {
                    
                    /*
                     * Target is solution set.
                     * 
                     * @see https://sourceforge.net/apps/trac/bigdata/ticket/524
                     * (SPARQL Cache)
                     */
                    
                    // The named solution set on which we will write.
                    final String solutionSet = clause.getName();

                    // Set the projection node.
                    queryRoot.setProjection(clause.getProjection());

					if (!isInsert) {

						/*
						 * Re-write the AST to handle DELETE solutions.
						 */
						
						convertQueryForDeleteSolutions(queryRoot, solutionSet);
						
					}
                    
                    /*
					 * Run as a SELECT query : Do NOT materialize IVs.
					 * 
					 * Note: This *MUST* use the view of the tripleStore which
					 * is associated with the SailConnection in case the view is
					 * isolated by a transaction.
					 */
					final ICloseableIterator<IBindingSet[]> result = ASTEvalHelper
							.evaluateTupleQuery2(context.conn.getTripleStore(),
									astContainer, context.getQueryBindingSet()/* bindingSets */, false/* materialize */);

                    try {

                        // Write the solutions onto the named solution set.
						context.solutionSetManager.putSolutions(solutionSet, result);

                    } finally {

                        result.close();

                    }
                                        
                } else {

                    /*
                     * Target is graph.
                     */
                    
                    final QuadData quadData = (insertClause == null ? deleteClause
                            : insertClause).getQuadData();

                    // Flatten the original WHERE clause into a CONSTRUCT
                    // template.
                    final ConstructNode template = quadData
                            .flatten(new ConstructNode(context));

					template.setDistinctQuads(true);

                    // Set the CONSTRUCT template (quads patterns).
                    queryRoot.setConstruct(template);
                    
                    /*
					 * Run as a CONSTRUCT query
					 * 
					 * FIXME Can we avoid IV materialization for this code path?
					 * Note that we have to do Truth Maintenance. However, I
					 * suspect that we do not need to do IV materialization if
					 * we can tunnel into the Sail's assertion and retraction
					 * buffers.
					 * 
					 * Note: This *MUST* use the view of the tripleStore which
					 * is associated with the SailConnection in case the view is
					 * isolated by a transaction.
					 */
                    final GraphQueryResult result = ASTEvalHelper
							.evaluateGraphQuery(context.conn.getTripleStore(),
									astContainer, context.getQueryBindingSet()/* bindingSets */, null /* dataset */);
                    
                    try {

                        while (result.hasNext()) {

                            final BigdataStatement stmt = (BigdataStatement) result
                                    .next();

                            addOrRemoveStatement(
                                    context.conn.getSailConnection(), stmt,
                                    isInsertOnly);

                        }

                    } finally {

                        result.close();

                    }

                }

            }

        }

        return null;
        
    }

	/**
	 * Efficiently resolve openrdf {@link BindingSet} into a chunked bigdata
	 * {@link IBindingSet}[] iterator. The closeable semantics of the iteration
	 * pattern are preserved.
	 * 
	 * @param r
	 *            The {@link LexiconRelation}.
	 * @param chunkSize
	 *            When converting the openrdf binding set iteration pattern into
	 *            a chunked iterator pattern, this will be the target chunk
	 *            size.
	 * @param result
	 *            The openrdf solutions.
	 * @return An iterator visiting chunked bigdata solutions.
	 * 
	 *         TODO We should not have to do this. We should stay within native
	 *         bigdata IBindingSet[]s and the native bigdata iterators
	 */
	private static ICloseableIterator<IBindingSet[]> asBigdataIterator(
			final LexiconRelation r,
			final int chunkSize,
			final CloseableIteration<BindingSet, QueryEvaluationException> result) {
		
		// Wrap with streaming iterator pattern.
		final Striterator sitr = new Striterator(
				// Chunk up the openrdf solutions.
				new Chunkerator<BindingSet>(
						// Convert the Sesame iteration into a Bigdata iterator.
						new Sesame2BigdataIterator<BindingSet, QueryEvaluationException>(
								result), chunkSize));

		// Add filter to batch resolve BindingSet[] => IBindingSet[].
		sitr.addFilter(new Resolver() {
			
			private static final long serialVersionUID = 1L;

			@Override
			protected Object resolve(Object obj) {

				// Visiting openrdf BindingSet[] chunks.
				final BindingSet[] in = (BindingSet[]) obj;
				
				// Batch resolve to IBindingSet[].
				final IBindingSet[] out = BigdataOpenRDFBindingSetsResolverator
						.resolveChunk(r, in);
				
				// Return Bigdata IBindingSet[].
				return out;
			}
		});
		
		return sitr;

	}

    /**
	 * We need to find and remove the matching solutions. We handle this by
	 * transforming the WHERE clause with a MINUS joining against the target
	 * solution set via an INCLUDE. The solutions which are produced by the
	 * query can then be written directly onto the named solution set. That way,
	 * both DELETE and INSERT will wind up as putSolutions().
	 * 
	 * <pre>
	 * WHERE {...}
	 * </pre>
	 * 
	 * is rewritten as
	 * 
	 * <pre>
	 * WHERE { INCLUDE %namedSet MINUS { ... } }
	 * </pre>
	 * 
	 * TODO If there is a BIND() to a constant in the SELECT expression, then
	 * this transform will not capture that binding. We would also need to
	 * process the PROJECTION and pull down any constants into BIND()s in the
	 * WHERE clause.
	 */
	private static void convertQueryForDeleteSolutions(
			final QueryRoot queryRoot, final String solutionSet) {

		final JoinGroupNode oldWhereClause = (JoinGroupNode) queryRoot
				.getWhereClause();
    	    
		final JoinGroupNode newWhereClause = new JoinGroupNode();
    	    
		queryRoot.setWhereClause(newWhereClause);
    	    
		final NamedSubqueryInclude includeOp = new NamedSubqueryInclude(solutionSet);
    	    
		newWhereClause.addArg(includeOp);
    	    
		final JoinGroupNode minusOp = new JoinGroupNode();
    	    
		minusOp.setMinus(true);
    	    
		newWhereClause.addArg(minusOp);
    	    
		minusOp.addArg(oldWhereClause.clone());
		
//    	    log.error("oldWhereClause="+oldWhereClause);
//    	    log.error("newWhereClause="+newWhereClause);

    }
    
    /**
     * Copy all statements from source to target.
     * 
     * @param left
     * @param op
     * @param context
     * @return
     * @throws RepositoryException
     */
    private static PipelineOp convertAddGraph(PipelineOp left,
            final AddGraph op, final AST2BOpUpdateContext context)
            throws RepositoryException {

        if (runOnQueryEngine)
            throw new UnsupportedOperationException();

        final BigdataURI sourceGraph = (BigdataURI) (op.getSourceGraph() == null ? null
                : op.getSourceGraph().getValue());
        
        final BigdataURI targetGraph = (BigdataURI) (op.getTargetGraph() == null ? null
                : op.getTargetGraph().getValue());
        
        copyStatements(//
                context, //
                op.isSilent(), //
                sourceGraph,//
                targetGraph//
                );

        return null;
    }

    /**
     * Copy all statements from the sourceGraph to the targetGraph.
     * <p>
     * Note: The SILENT keyword for ADD, COPY, and MOVE indicates that the
     * implementation SHOULD/MAY report an error if the source graph does not
     * exist (the spec is not consistent here across those operations). Further,
     * there is no explicit create/drop of graphs in bigdata so it WOULD be Ok
     * if we just ignored the SILENT keyword.
     */
    private static void copyStatements(final AST2BOpUpdateContext context,
            final boolean silent, final BigdataURI sourceGraph,
            final BigdataURI targetGraph) throws RepositoryException {

        if (log.isDebugEnabled())
            log.debug("sourceGraph=" + sourceGraph + ", targetGraph="
                    + targetGraph);
        
        if (!silent) {

//            assertGraphNotEmpty(context, sourceGraph);

        }
        
        final RepositoryResult<Statement> result = context.conn.getStatements(
                null/* s */, null/* p */, null/* o */,
                context.isIncludeInferred(), new Resource[] { sourceGraph });
        
        try {

            context.conn.add(result, new Resource[] { targetGraph });

        } finally {

            result.close();

        }

    }

    /**
     * Drop() target, Add(source,target), Drop(source).
     * 
     * @param left
     * @param op
     * @param context
     * @return
     * @throws RepositoryException
     * @throws SailException 
     */
    private static PipelineOp convertMoveGraph(PipelineOp left,
            final MoveGraph op, final AST2BOpUpdateContext context)
            throws RepositoryException, SailException {

        if (runOnQueryEngine)
            throw new UnsupportedOperationException();

        final BigdataURI sourceGraph = (BigdataURI) (op.getSourceGraph() == null ? context.f
                .asValue(BD.NULL_GRAPH) : op.getSourceGraph().getValue());

        final BigdataURI targetGraph = (BigdataURI) (op.getTargetGraph() == null ? context.f
                .asValue(BD.NULL_GRAPH) : op.getTargetGraph().getValue());

        if (log.isDebugEnabled())
            log.debug("sourceGraph=" + sourceGraph + ", targetGraph="
                    + targetGraph);

        if (!sourceGraph.equals(targetGraph)) {

            clearOneGraph(targetGraph, context);

            copyStatements(context, op.isSilent(), sourceGraph, targetGraph);

            clearOneGraph(sourceGraph, context);
            
        }

        return null;
        
    }

    /**
     * Drop() target, then Add().
     * 
     * @param left
     * @param op
     * @param context
     * @return
     * @throws RepositoryException 
     * @throws SailException 
     */
    private static PipelineOp convertCopyGraph(PipelineOp left,
            final CopyGraph op, final AST2BOpUpdateContext context)
            throws RepositoryException, SailException {

        if (runOnQueryEngine)
            throw new UnsupportedOperationException();

        final BigdataURI sourceGraph = (BigdataURI) (op.getSourceGraph() == null ? context.f
                .asValue(BD.NULL_GRAPH) : op.getSourceGraph().getValue());

        final BigdataURI targetGraph = (BigdataURI) (op.getTargetGraph() == null ? context.f
                .asValue(BD.NULL_GRAPH) : op.getTargetGraph().getValue());

        if (log.isDebugEnabled())
            log.debug("sourceGraph=" + sourceGraph + ", targetGraph="
                    + targetGraph);

        if (!sourceGraph.equals(targetGraph)) {

            clearOneGraph(targetGraph, context);

            copyStatements(context, op.isSilent(), sourceGraph, targetGraph);

        }

        return null;
    }

    /**
     * <pre>
     * LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
     * </pre>
     * 
     * @param left
     * @param op
     * @param context
     * @return
     * @throws Exception
     */
    private static PipelineOp convertLoadGraph(PipelineOp left,
            final LoadGraph op, final AST2BOpUpdateContext context)
            throws Exception {
        
        if (!runOnQueryEngine) {

            final AtomicLong nmodified = new AtomicLong();

            final String urlStr = op.getSourceGraph().getValue().stringValue();

            try {

                final URL sourceURL = new URL(urlStr);

                final BigdataURI defaultContext = (BigdataURI) (op
                        .getTargetGraph() == null ? null : op.getTargetGraph()
                        .getValue());

                if (log.isDebugEnabled())
                    log.debug("sourceURI=" + urlStr + ", defaultContext="
                            + defaultContext);

                // Take overrides from LOAD request, defaults from triple store and fall back to static defaults.
                final Properties defaults = context.getAbstractTripleStore().getProperties();
                
				final boolean verifyData = Boolean
						.parseBoolean(op.getProperty(LoadGraph.Annotations.VERIFY_DATA, p.getProperty(
								RDFParserOptions.Options.VERIFY_DATA, RDFParserOptions.Options.DEFAULT_VERIFY_DATA)));

				final boolean preserveBlankNodeIDs = Boolean
						.parseBoolean(op.getProperty(LoadGraph.Annotations.PRESERVE_BLANK_NODE_IDS,
								p.getProperty(RDFParserOptions.Options.PRESERVE_BNODE_IDS,
										RDFParserOptions.Options.DEFAULT_PRESERVE_BNODE_IDS)));

				final boolean stopAtFirstError = Boolean
						.parseBoolean(op.getProperty(LoadGraph.Annotations.STOP_AT_FIRST_ERROR,
								p.getProperty(RDFParserOptions.Options.STOP_AT_FIRST_ERROR,
										RDFParserOptions.Options.DEFAULT_STOP_AT_FIRST_ERROR)));

				final DatatypeHandling dataTypeHandling = DatatypeHandling
						.valueOf(op.getProperty(LoadGraph.Annotations.DATA_TYPE_HANDLING,
								p.getProperty(RDFParserOptions.Options.DATATYPE_HANDLING,
										RDFParserOptions.Options.DEFAULT_DATATYPE_HANDLING)));

				final RDFParserOptions parserOptions = new RDFParserOptions(//
                		verifyData,//
                		preserveBlankNodeIDs,//
                		stopAtFirstError,//
                		dataTypeHandling//
                		);
                
                doLoad(context.conn.getSailConnection(), sourceURL,
                        defaultContext, parserOptions, nmodified,
                        op);

            } catch (Throwable t) {

                final String msg = "Could not load: url=" + urlStr + ", cause="
                        + t;

                if (op.isSilent()) {
                 
                    log.warn(msg);
                    
                } else {
                    
                    throw new RuntimeException(msg, t);
                    
                }

            }

            return null;
            
        }
        
        /*
         * Parse the file.
         * 
         * Note: After the parse step, the remainder of the steps are just like
         * INSERT DATA.
         */
        {

            final Map<String, Object> anns = new HashMap<String, Object>();

            anns.put(BOp.Annotations.BOP_ID, context.nextId());

            // required.
            anns.put(ParseOp.Annotations.SOURCE_URI, op.getSourceGraph()
                    .getValue());

            if(op.isSilent())
                anns.put(ParseOp.Annotations.SILENT, true);

            // optional.
            if (op.getTargetGraph() != null)
                anns.put(ParseOp.Annotations.TARGET_URI, op.getTargetGraph());

            // required.
            anns.put(ParseOp.Annotations.TIMESTAMP, context.getTimestamp());
            anns.put(ParseOp.Annotations.RELATION_NAME,
                    new String[] { context.getNamespace() });

            /*
             * TODO 100k is the historical default for the data loader. We
             * generally want to parse a lot of data at once and vector it in
             * big chunks. However, we could have a lot more parallelism with
             * the query engine. So, if there are multiple source URIs to be
             * loaded, then we might want to reduce the vector size (or maybe
             * not, probably depends on the JVM heap).
             */
            anns.put(ParseOp.Annotations.CHUNK_CAPACITY, 100000);

            left = new ParseOp(leftOrEmpty(left), anns);

        }

        /*
         * Append the pipeline operations to add/resolve IVs against the lexicon
         * and insert/delete statemetns.
         */
        left = addInsertOrDeleteDataPipeline(left, true/* insert */, context);

        /*
         * Execute the update.
         */
        executeUpdate(left, context.getBindings()/* bindingSets */, context);

        // Return null since pipeline was evaluated.
        return null;
        
    }
    
    /**
     * 
     * Utility method to get the {@link RDFFormat} for filename.
     * 
     * It checks for compressed endings and is provided as a utility.
     * 
     * @param fileName
     * @return
     */
    public static RDFFormat rdfFormatForFile(final String fileName) {
    	/*
         * Try to get the RDFFormat from the URL's file path.
         */

        RDFFormat fmt = RDFFormat.forFileName(fileName);

        if (fmt == null && fileName.endsWith(".zip")) {
            fmt = RDFFormat.forFileName(fileName.substring(0, fileName.length() - 4));
        }

        if (fmt == null && fileName.endsWith(".gz")) {
            fmt = RDFFormat.forFileName(fileName.substring(0, fileName.length() - 3));
        }

        if (fmt == null) {
            // Default format.
            fmt = RDFFormat.RDFXML;
        }
        
        return fmt;
    }

    /**
     * Parse and load a document.
     * 
     * @param conn
     * @param sourceURL
     * @param defaultContext
     * @param nmodified
     * @return
     * @throws IOException
     * @throws RDFHandlerException
     * @throws RDFParseException
     * 
     *             TODO See {@link ParseOp} for a significantly richer pipeline
     *             operator which will parse a document. However, this method is
     *             integrated into all of the truth maintenance mechanisms in
     *             the Sail and is therefore easier to place into service.
     */
    private static void doLoad(final BigdataSailConnection conn,
            final URL sourceURL, final URI defaultContext,
            final IRDFParserOptions parserOptions, final AtomicLong nmodified,
            final LoadGraph op) throws IOException,
            RDFParseException, RDFHandlerException {

        // Use the default context if one was given and otherwise
        // the URI from which the data are being read.
        final Resource defactoContext = defaultContext == null ? new URIImpl(
                sourceURL.toExternalForm()) : defaultContext;

        URLConnection hconn = null;
        try {

            hconn = sourceURL.openConnection();
            if (hconn instanceof HttpURLConnection) {
                ((HttpURLConnection) hconn).setRequestMethod("GET");
            }
            hconn.setDoInput(true);
            hconn.setDoOutput(false);
            hconn.setReadTimeout(0);// no timeout? http param?

            /*
             * There is a request body, so let's try and parse it.
             */

            final String contentType = hconn.getContentType();

            // The baseURL (passed to the parser).
            final String baseURL = sourceURL.toExternalForm();
            
            // The file path.
            //BLZG-1929
            final String n = sourceURL.getPath();

            /**
             * Attempt to obtain the format from the Content-Type.
             * 
             * <a href="https://sourceforge.net/apps/trac/bigdata/ticket/620">
             * UpdateServlet fails to parse MIMEType when doing conneg. </a>
             */
            RDFFormat format = RDFFormat.forMIMEType(new MiniMime(contentType)
                    .getMimeType());

            if (format == null) {

                format = rdfFormatForFile(n);

            }

            if (format == null)
                throw new UnknownContentTypeException(contentType);

            final RDFParserFactory rdfParserFactory = RDFParserRegistry
                    .getInstance().get(format);

            if (rdfParserFactory == null)
                throw new UnknownContentTypeException(contentType);

            final RDFParser rdfParser = rdfParserFactory
                    .getParser();

            rdfParser.setValueFactory(conn.getTripleStore().getValueFactory());

            /*
             * Apply the RDF parser options.
             */

            rdfParser.setVerifyData(parserOptions.getVerifyData());

            rdfParser.setPreserveBNodeIDs(parserOptions.getPreserveBNodeIDs());

            rdfParser.setStopAtFirstError(parserOptions.getStopAtFirstError());

            rdfParser.setDatatypeHandling(parserOptions.getDatatypeHandling());

            rdfParser.setRDFHandler(new AddStatementHandler(conn, nmodified,
                    defactoContext, op));

            /*
             * Setup the input stream.
             */
            InputStream is = hconn.getInputStream();

            try {

                /*
                 * Setup decompression.
                 */
                
                if (n.endsWith(".gz")) {

                    is = new GZIPInputStream(is);

//                } else if (n.endsWith(".zip")) {
//
//                    /*
//                     * TODO This will not process all entries in a zip input
//                     * stream, just the first.
//                     */
//                    is = new ZipInputStream(is);

                }

            } catch (Throwable t) {

                if (is != null) {

                    try {
                        is.close();
                    } catch (Throwable t2) {
                        log.warn(t2, t2);
                    }

                    throw new RuntimeException(t);

                }
                
            }
            
            /*
             * Run the parser, which will cause statements to be
             * inserted.
             */

            rdfParser.parse(is, baseURL);
            
        } finally {

            if (hconn instanceof HttpURLConnection) {
                /*
                 * Disconnect, but only after we have loaded all the
                 * URLs. Disconnect is optional for java.net. It is a
                 * hint that you will not be accessing more resources on
                 * the connected host. By disconnecting only after all
                 * resources have been loaded we are basically assuming
                 * that people are more likely to load from a single
                 * host.
                 */
                ((HttpURLConnection) hconn).disconnect();
            }

        }
        
    }
    
    /**
     * Helper class adds statements to the sail as they are visited by a parser.
     */
    private static class AddStatementHandler extends RDFHandlerBase {

        private final LoadGraph op;
        private final long beginNanos;
        private final BigdataSailConnection conn;
        private final AtomicLong nmodified;
        private final Resource[] defaultContexts;

        public AddStatementHandler(final BigdataSailConnection conn,
                final AtomicLong nmodified, final Resource defaultContext,
                final LoadGraph op) {
            this.conn = conn;
            this.nmodified = nmodified;
            final boolean quads = conn.getTripleStore().isQuads();
            if (quads && defaultContext != null) {
                // The default context may only be specified for quads.
                this.defaultContexts = new Resource[] { defaultContext };
            } else {
                this.defaultContexts = new Resource[0];
            }
            this.op = op;
            this.beginNanos = System.nanoTime();
        }

        public void handleStatement(final Statement stmt)
                throws RDFHandlerException {

            try {

                conn.addStatement(//
                        stmt.getSubject(), //
                        stmt.getPredicate(), //
                        stmt.getObject(), //
                        (Resource[]) (stmt.getContext() == null ?  defaultContexts
                                : new Resource[] { stmt.getContext() })//
                        );

            } catch (SailException e) {

                throw new RDFHandlerException(e);

            }

            final long nparsed = nmodified.incrementAndGet();

            if ((nparsed % 10000) == 0L) {

                final long elapsed = System.nanoTime() - beginNanos;

                // notify listener(s)
                conn.fireEvent(new SPARQLUpdateEvent.LoadProgress(op, elapsed,
                        nparsed, false/* done */));

            }

        }

        /**
         * Overridden to send out an incremental progress report for the end of
         * the LOAD operation.
         */
        @Override
        public void endRDF() throws RDFHandlerException {

            final long nparsed = nmodified.get();

            final long elapsed = System.nanoTime() - beginNanos;

            // notify listener(s)
            conn.fireEvent(new SPARQLUpdateEvent.LoadProgress(op, elapsed,
                    nparsed, true/* done */));
            
        }

    }

    /**
     * Note: Bigdata does not support empty graphs, so {@link UpdateType#Clear}
     * and {@link UpdateType#Drop} have the same semantics.
     * 
     * <pre>
     * DROP ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
     * </pre>
     * 
     * @param left
     * @param op
     * @param context
     * @return
     * @throws RepositoryException
     * @throws SailException 
     */
    private static PipelineOp convertClearOrDropGraph(PipelineOp left,
            final DropGraph op, final AST2BOpUpdateContext context)
            throws RepositoryException, SailException {

        if (runOnQueryEngine)
            throw new UnsupportedOperationException();

        final TermNode targetGraphNode = op.getTargetGraph();

        final BigdataURI targetGraph = targetGraphNode == null ? null
                : (BigdataURI) targetGraphNode.getValue();

        clearGraph(op.isSilent(), op.getTargetSolutionSet(), targetGraph,
                op.getScope(), op.isAllGraphs(), op.isAllSolutionSets(),
                context);

        return left;
        
    }

    /**
     * Clear one graph (SILENT).
     * 
     * @param targetGraph
     *            The graph to be cleared -or- <code>null</code> if no target
     *            graph was named.
     * @param context
     *            The {@link AST2BOpUpdateContext} used to perform the
     *            operation.
     * 
     * @throws RepositoryException
     * @throws SailException
     */
    private static final void clearOneGraph(final URI targetGraph, //
            final AST2BOpUpdateContext context//
    ) throws RepositoryException, SailException {

        clearGraph(true/* silent */, null/* targetSolutionSet */, targetGraph,
                null/* scope */, false/* allGraphs */,
                false/* allSolutionSets */, context);

    }
    
    /**
     * Clear one or more graphs and/or solution sets.
     * 
     * @param silent
     *            When <code>true</code>, some kinds of problems will not be
     *            reported to the caller.
     * @param targetSolutionSet
     *            The target solution set to be cleared -or- <code>null</code>
     *            if no target solution set was named.
     * @param targetGraph
     *            The graph to be cleared -or- <code>null</code> if no target
     *            graph was named.
     * @param scope
     *            The scope iff just the graphs in either the
     *            {@link Scope#DEFAULT_CONTEXTS} or {@link Scope#NAMED_CONTEXTS}
     *            should be cleared and otherwise <code>null</code>.
     * @param allGraphs
     *            iff all graphs should be cleared.
     * @param allSolutionSets
     *            iff all solution sets should be cleared.
     * @param context
     *            The {@link AST2BOpUpdateContext} used to perform the
     *            operation.
     * 
     * @throws RepositoryException
     * @throws SailException
     */
    // CLEAR/DROP ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL | GRAPHS | SOLUTIONS | SOLUTIONS %VARNAME)
    private static void clearGraph(//
            final boolean silent,//
            final String solutionSet,//
            final URI targetGraph, //
            final Scope scope,//
            final boolean allGraphs,//
            final boolean allSolutionSets,//
            final AST2BOpUpdateContext context//
    ) throws RepositoryException, SailException {

        if (log.isDebugEnabled())
            log.debug("targetGraph=" + targetGraph + ", scope=" + scope);

        /*
         * Note: removeStatements() is not exposed by the RepositoryConnection.
         */
        final BigdataSailConnection sailConn = context.conn.getSailConnection();
        
        if (solutionSet != null) {

            // Clear the named solution set.
            if (!context.solutionSetManager.clearSolutions(solutionSet) && !silent) {
                
                // Named solution set does not exists, but should exist.
                throw new SolutionSetDoesNotExistException(solutionSet);
    
            }
            
        }

        if (targetGraph != null) {

            /*
             * Addressing a specific graph.
             */

            sailConn.removeStatements(null/* s */, null/* p */, null/* o */,
                    targetGraph);

        } 
        
        if (scope != null) {

            if (scope == Scope.DEFAULT_CONTEXTS) {

                /*
                 * Addressing the defaultGraph (Sesame nullGraph).
                 */
                sailConn.removeStatements(null/* s */, null/* p */,
                        null/* o */, BD.NULL_GRAPH);

            } else {

                /*
                 * Addressing ALL NAMED GRAPHS.
                 * 
                 * Note: This is everything EXCEPT the nullGraph.
                 */
                final RepositoryResult<Resource> result = context.conn
                        .getContextIDs();

                try {

                    while (result.hasNext()) {

                        final Resource c = result.next();

                        sailConn.removeStatements(null/* s */, null/* p */,
                                null/* o */, c);

                    }
                    
                } finally {
                    
                    result.close();
                    
                }
                
            }

        } 
        
        if(allGraphs) {
            
            /*
             * Addressing ALL graphs.
             * 
             * TODO This should be optimized. If we are doing truth maintenance,
             * then we need to discard the buffers, drop all statements and also
             * drop the proof chains. If we are not doing truth maintenance and
             * this is the unisolated connection, then delete all statements and
             * also clear the lexicon. (We should really catch this optimization
             * in the BigdataSailConnection.)
             */

            sailConn.removeStatements(null/* s */, null/* p */, null/* o */);

        }

        /*
         * Note: We need to verify that the backing data structure is enabled
         * since the default semantics of CLEAR ALL and DROP ALL also imply all
         * named solution sets.
         */
        if (allSolutionSets && context.solutionSetManager != null) {
            
            // Delete all solution sets.
            context.solutionSetManager.clearAllSolutions();
            
        }
        
    }

   	private static PipelineOp convertDropEntailments(final PipelineOp left,
			final AST2BOpUpdateContext context) throws SailException {
		
		long stmtCount = 0;
		
		if (log.isDebugEnabled()) {			
			stmtCount = context.conn.getSailConnection().getTripleStore().getStatementCount(true);			
			log.info("begin drop entailments");
			
		}
		
		context.conn.getSailConnection().removeAllEntailments();
				
		if (log.isDebugEnabled()) {			
			long removedCount = stmtCount - context.conn.getSailConnection().getTripleStore().getStatementCount(true);			
            log.debug("Removed statements = " + removedCount);
            
		}
		
        return left;

    }
	
	private static PipelineOp convertDisableEntailments(PipelineOp left,
			AST2BOpUpdateContext context) {
		
		if (log.isDebugEnabled()) {
            log.debug("Going to disable truth maintenance");
		}
		
		
		if (context.conn.getSailConnection().isTruthMaintenanceConfigured()) {
			
			context.conn.getSailConnection().setTruthMaintenance(false);
			
		} else {			
			log.debug("Truth maintenance is not configured");			
		}
			
		if (log.isDebugEnabled()) {			
            log.debug("truthMaintenance = " + context.conn.getSailConnection().getTruthMaintenance());            
		}
		
		return left;
	}
	
	private static PipelineOp convertEnableEntailments(PipelineOp left,
			AST2BOpUpdateContext context) {
		
		if (log.isDebugEnabled()) {			
		    log.debug("Going to enable truth maintenance");            
		}
		
		if (context.conn.getSailConnection().isTruthMaintenanceConfigured()) {
			
			context.conn.getSailConnection().setTruthMaintenance(true);
			
		} else {			
			log.debug("Truth maintenance is not configured");			
		}		
			
		if (log.isDebugEnabled()) {			
            log.debug("truthMaintenance = " + context.conn.getSailConnection().getTruthMaintenance());            
		}
		
		return left;
	}
	
	private static PipelineOp convertCreateEntailments(PipelineOp left,
			AST2BOpUpdateContext context) throws SailException {
		
		long stmtCount = 0;
		if (log.isDebugEnabled()) {
			stmtCount = context.conn.getSailConnection().getTripleStore().getStatementCount(true);
			log.info("begin compute closure");
		}
		
		context.conn.getSailConnection().computeClosure();
		
		if (log.isDebugEnabled()) {
			long inferredCount = context.conn.getSailConnection().getTripleStore().getStatementCount(true) - stmtCount;
            log.debug("Inferred statements = " + inferredCount);
            
		}
		
		return left;
	}

	
	/**
	 * GRAPHS : If the graph already exists (context has at least one
	 * statement), then this is an error (unless SILENT). Otherwise it is a NOP.
	 * <p>
	 * SOLUTIONS : If the named solution set already exists (is registered, but
	 * may be empty), then this is an error (unless SILENT). Otherwise, the
	 * named solution set is provisioned according to the optional parameters.
	 * 
	 * @param left
	 * @param op
	 * @param context
	 * @return
	 */
	private static PipelineOp convertCreateGraph(final PipelineOp left,
			final CreateGraph op, final AST2BOpUpdateContext context) {

		if (op.isTargetSolutionSet()) {

			final String solutionSet = op.getTargetSolutionSet();

			final boolean exists = context.solutionSetManager
					.existsSolutions(solutionSet);

			if (!op.isSilent() && exists) {

                // Named solution set exists, but should not.
                throw new SolutionSetExistsException(solutionSet);

			}

			if (!exists) {

				context.solutionSetManager
						.createSolutions(solutionSet, op.getParams());

            }
            
        } else {

            final BigdataURI c = (BigdataURI) ((CreateGraph) op)
                    .getTargetGraph().getValue();

            if (log.isDebugEnabled())
                log.debug("targetGraph=" + c);

            if (!op.isSilent()) {

                assertGraphExists(context, c);

            }
            
        }

        return left;

    }

    /**
     * <pre>
     * INSERT DATA -or- DELETE DATA
     * </pre>
     * 
     * @param left
     * @param op
     * @param context
     * @return
     * @throws Exception
     */
    private static PipelineOp convertInsertOrDeleteData(PipelineOp left,
            final AbstractGraphDataUpdate op, final AST2BOpUpdateContext context)
            throws Exception {

        final boolean insert;
        switch (op.getUpdateType()) {
        case InsertData:
            insert = true;
            break;
        case DeleteData:
            insert = false;
            break;
        default:
            throw new UnsupportedOperationException(op.getUpdateType().name());
        }

        if (!runOnQueryEngine) {

            final BigdataStatement[] stmts = op.getData();

            if (log.isDebugEnabled())
                log.debug((insert ? "INSERT" : "DELETE") + " DATA: #stmts="
                        + stmts.length);

            final BigdataSailConnection conn = context.conn.getSailConnection();
            
            for (BigdataStatement s : stmts) {
 
                addOrRemoveStatementData(conn, s, insert);
                
            }

            return null;
            
        }
        
        /*
         * Convert the statements to be asserted or retracted into an
         * IBindingSet[].
         */
        final IBindingSet[] bindingSets;
        {

            // Note: getTargetGraph() is not defined for INSERT/DELETE DATA.
            final ConstantNode c = null;// op.getTargetGraph();

            @SuppressWarnings("rawtypes")
            IV targetGraphIV = null;

            if (c != null) {

                targetGraphIV = c.getValue().getIV();

            }

            if (targetGraphIV == null && context.isQuads()) {

                targetGraphIV = context.getNullGraph().getIV();

            }

            bindingSets = getData(op.getData(), targetGraphIV,
                    context.isQuads());

        }

        /*
         * Append the pipeline operations to add/resolve IVs against the lexicon
         * and insert/delete statemetns.
         */

        left = addInsertOrDeleteDataPipeline(left, insert, context);

        /*
         * Execute the update.
         */
        executeUpdate(left, bindingSets, context);
        
        // Return null since pipeline was evaluated.
        return null;

    }

    /**
     * Insert or remove a statement.
     * 
     * @param conn
     *            The connection on which to write the mutation.
     * @param spo
     *            The statement.
     * @param insert
     *            <code>true</code> iff the statement is to be inserted and
     *            <code>false</code> iff the statement is to be removed.
     * @throws SailException
     */
    private static void addOrRemoveStatement(final BigdataSailConnection conn,
            final BigdataStatement spo, final boolean insert) throws SailException {

        final Resource s = (Resource) spo.getSubject();

        final URI p = (URI) spo.getPredicate();
        
        final Value o = (Value) spo.getObject();
        
        /*
         * If [c] is not bound, then using an empty Resource[] for the contexts.
         * 
         * On insert, this will cause the data to be added to the null graph.
         * 
         * On remove, this will cause the statements to be removed from all
         * contexts (on remove it is interpreted as a wildcard).
         */
        
        final Resource c = (Resource) (spo.getContext() == null ? null : spo
                .getContext());
        
        final Resource[] contexts = (Resource[]) (c == null ? NO_CONTEXTS
                : new Resource[] { c });
        
        if(log.isTraceEnabled())
            log.trace((insert ? "INSERT" : "DELETE") + ": <" + s + "," + p
                    + "," + o + "," + Arrays.toString(contexts));
        
        if (insert) {
        
            conn.addStatement(s, p, o, contexts);
            
        } else {

//            /*
//             * We need to handle blank nodes (which can appear in the subject or
//             * object position) as unbound variables.
//             */
//            
//            final Resource s1 = s instanceof BNode ? null : s;
//
//            final Value o1 = o instanceof BNode ? null : o;
//            
//            conn.removeStatements(s1, p, o1, contexts);

            /**
             * 
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/571">
             *      DELETE/INSERT WHERE handling of blank nodes </a>
             */
            conn.removeStatements(s, p, o, contexts);

        }

    }

    /**
     * Insert or remove a statement (INSERT DATA or DELETE DATA).
     * 
     * @param conn
     *            The connection on which to write the mutation.
     * @param spo
     *            The statement.
     * @param insert
     *            <code>true</code> iff the statement is to be inserted and
     *            <code>false</code> iff the statement is to be removed.
     * @throws SailException
     */
    private static void addOrRemoveStatementData(final BigdataSailConnection conn,
            final BigdataStatement stmt, final boolean insert) throws SailException {

//        final Resource s = (Resource) spo.s().getValue();
//
//        final URI p = (URI) spo.p().getValue();
//        
//        final Value o = (Value) spo.o().getValue();

        final Resource s = stmt.getSubject();
        
        final URI p = stmt.getPredicate();
        
        final Value o = stmt.getObject();
        
        /*
         * If [c] is not bound, then using an empty Resource[] for the contexts.
         * 
         * On insert, this will cause the data to be added to the null graph.
         * 
         * On remove, this will cause the statements to be removed from all
         * contexts (on remove it is interpreted as a wildcard).
         */
        
        final Resource c = (Resource) (stmt.getContext() == null ? null : stmt
                .getContext());

        final Resource[] contexts = (Resource[]) (c == null ? NO_CONTEXTS
                : new Resource[] { c });
        
        if(log.isTraceEnabled())
            log.trace((insert ? "INSERT" : "DELETE") + ": <" + s + "," + p
                    + "," + o + "," + Arrays.toString(contexts));
        
        if (insert) {
        
            conn.addStatement(s, p, o, contexts);
            
        } else {
         
            conn.removeStatements(s, p, o, contexts);
            
        }

    }

    /**
     * @param left
     * @param b
     * @param context
     * @return
     */
    private static PipelineOp addInsertOrDeleteDataPipeline(PipelineOp left,
            final boolean insert, final AST2BOpUpdateContext context) {
        
        /*
         * Resolve/add terms against the lexicon.
         * 
         * TODO Must do SIDs support. Probably pass the database mode in as an
         * annotation. See StatementBuffer.
         */
        left = new ChunkedResolutionOp(leftOrEmpty(left), NV.asMap(
                //
                new NV(BOp.Annotations.BOP_ID, context.nextId()),//
                new NV(ChunkedResolutionOp.Annotations.TIMESTAMP, context
                        .getTimestamp()),//
                new NV(ChunkedResolutionOp.Annotations.RELATION_NAME,
                        new String[] { context.getLexiconNamespace() })//
                ));

        /*
         * Insert / remove statements.
         * 
         * Note: namespace is the triple store, not the spo relation. This is
         * because insert is currently on the triple store for historical SIDs
         * support.
         * 
         * Note: This already does TM for SIDs mode.
         * 
         * TODO This must to TM for the subject-centric text index.
         * 
         * TODO This must be able to do TM for triples+inference.
         */
        if (insert) {
            left = new InsertStatementsOp(leftOrEmpty(left), NV.asMap(
                    new NV(BOp.Annotations.BOP_ID, context.nextId()),//
                    new NV(ChunkedResolutionOp.Annotations.TIMESTAMP, context
                            .getTimestamp()),//
                    new NV(ChunkedResolutionOp.Annotations.RELATION_NAME,
                            new String[] { context.getNamespace() })//
                    ));
        } else {
            left = new RemoveStatementsOp(leftOrEmpty(left), NV.asMap(
                    new NV(BOp.Annotations.BOP_ID, context.nextId()),//
                    new NV(ChunkedResolutionOp.Annotations.TIMESTAMP, context
                            .getTimestamp()),//
                    new NV(ChunkedResolutionOp.Annotations.RELATION_NAME,
                            new String[] { context.getNamespace() })//
                    ));
        }

        return left;
    }

    /**
     * Convert an {@link ISPO}[] into an {@link IBindingSet}[].
     * 
     * @param data
     *            The {@link ISPO}[].
     * @param targetGraph
     *            The target graph (optional, but required if quads).
     * @param quads
     *            <code>true</code> iff the target {@link AbstractTripleStore}
     *            is in quads mode.
     * 
     * @return The {@link IBindingSet}[].
     * 
     *         TODO Either we need to evaluate this NOW (rather than deferring
     *         it to pipelined evaluation later) or this needs to be pumped into
     *         a hash index associated with the query plan in order to be
     *         available when there is more than one INSERT DATA or REMOVE DATA
     *         operation (or simply more than one UPDATE operation).
     *         <p>
     *         That hash index could be joined into the solutions immediate
     *         before we undertake the chunked resolution operation which then
     *         flows into the add/remove statements operation.
     *         <p>
     *         Variables in the query can not be projected into this operation
     *         without causing us to insert/delete the cross product of those
     *         variables, which has no interesting effect.
     *         <p>
     *         The advantage of running one plan per {@link Update} is that the
     *         data can be flowed naturally into the {@link IRunningQuery}.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static IBindingSet[] getData(final ISPO[] data,
            final IV<?, ?> targetGraph, final boolean quads) {

        final IBindingSet[] bsets = new IBindingSet[data.length];

        for (int i = 0; i < data.length; i++) {

            final ISPO spo = data[i];

            final IBindingSet bset = bsets[i] = new ListBindingSet();

            bset.set(s, new Constant(spo.s()));

            bset.set(p, new Constant(spo.p()));

            bset.set(o, new Constant(spo.o()));

            Constant g = null;

            if (spo.c() != null)
                g = new Constant(spo.c());

            if (quads && g == null) {

                g = new Constant(targetGraph);

            }

            if (g != null) {

                bset.set(c, g);

            }

        }

        return bsets;
    
    }

    /**
     * Execute the update plan.
     * 
     * @param left
     * @param bindingSets
     *            The source solutions.
     * @param context
     * 
     * @throws UpdateExecutionException
     */
    static private void executeUpdate(final PipelineOp left,
            IBindingSet[] bindingSets, final AST2BOpUpdateContext context)
            throws Exception {

        if (!runOnQueryEngine)
            throw new UnsupportedOperationException();
        
        if (left == null)
            throw new IllegalArgumentException();

        if(bindingSets == null) {
            bindingSets = EMPTY_BINDING_SETS;
        }
        
        if (context == null)
            throw new IllegalArgumentException();

        IRunningQuery runningQuery = null;
        try {

            // Submit update plan for evaluation.
            runningQuery = context.queryEngine.eval(left, bindingSets);

            // Wait for the update plan to complete.
            runningQuery.get();

        } finally {

            if (runningQuery != null) {
            
                // ensure query is halted.
                runningQuery.cancel(true/* mayInterruptIfRunning */);
            }
            
        }

    }

    /**
     * Throw an exception if the graph is empty.
     * 
     * @throws GraphEmptyException
     *             if the graph does not exist and/or is empty.
     */
    static private void assertGraphNotEmpty(final AST2BOpUpdateContext context,
            final BigdataURI sourceGraph) {

        if (sourceGraph == null || sourceGraph.equals(BD.NULL_GRAPH)) {

            /*
             * The DEFAULT graph is considered non-empty.
             * 
             * Note: [null] for the sourceGraph indicates the default graph. But
             * the nullGraph can also indicate the default graph (empirically
             * observed).
             */
            
            return;

        }

        if (sourceGraph.getIV() == null) {

            // Proof that the graph does not exist.
            throw new GraphEmptyException(sourceGraph);
            
        }

        if (context.conn
                .getTripleStore()
                .getAccessPath(null/* s */, null/* p */, null/* o */,
                        sourceGraph).isEmpty()) {

            // Proof that the graph is empty.
            throw new GraphEmptyException(sourceGraph);
            
        }
        
    }
    
    /**
     * Throw an exception unless the graph is non-empty.
     * <p>
     * Note: This *MUST* use the view of the tripleStore which is associated
     * with the SailConnection in case the view is isolated by a transaction.
     * <P>
     * Note: If the IV could not be resolved, then that is proof that there is
     * no named graph for that RDF Resource.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/569
     *      (LOAD-CREATE-LOAD using virgin journal fails with "Graph exists"
     *      exception)
     */
    static private void assertGraphExists(final AST2BOpUpdateContext context,
            final BigdataURI c) {

        if (c.getIV() != null
                && context.conn
                        .getTripleStore()
                        .getAccessPath(null/* s */, null/* p */, null/* o */,
                                c.getIV()).rangeCount(false/* exact */) != 0) {

            throw new GraphExistsException(c);

        }
        
    }
    
    private static final IBindingSet[] EMPTY_BINDING_SETS = new IBindingSet[0];
    private static final Resource[] NO_CONTEXTS = new Resource[0];

}
