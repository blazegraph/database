package com.bigdata.rdf.sparql.ast.eval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.constraints.IVValueExpression;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryHeadlessImpl;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.sail.sparql.ast.ASTDatasetClause;
import com.bigdata.rdf.sail.sparql.ast.ASTIRI;
import com.bigdata.rdf.sail.sparql.ast.ASTQueryContainer;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractGraphDataUpdate;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.CreateGraph;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.DeleteInsertGraph;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.HavingNode;
import com.bigdata.rdf.sparql.ast.IDataSetNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.PathNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QuadsDataOrNamedSolutionSet;
import com.bigdata.rdf.sparql.ast.QuadsOperationInTriplesModeException;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryNodeBase;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryFunctionNodeBase;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.Update;
import com.bigdata.rdf.sparql.ast.UpdateRoot;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.ASTContainer.Annotations;
import com.bigdata.rdf.sparql.ast.PathNode.PathAlternative;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSetValueExpressionsOptimizer;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.relation.accesspath.IAccessPath;

/**
 * This class provides batch resolution of internal values, which were left
 * unresolved during query/update preparation. Values, which are processed: any
 * BOp arguments, ValueExpressions, and specific values stored in annotations
 * (for example, SERVICE_REF in ServiceNode).
 * <p>
 * Class performs efficient batch resolution of RDF Values against the database
 * during query preparation. This efficiency is important on a cluster and when
 * a SPARQL query or update contains a large number of RDF Values.
 * 
 * @author igor.kim
 * 
 * @see https://jira.blazegraph.com/browse/BLZG-1176
 */
@SuppressWarnings( { "rawtypes", "unchecked" } )
public class ASTDeferredIVResolution {
    
    private final static Logger log = Logger
            .getLogger(ASTDeferredIVResolution.class);

    /**
     * Anonymous instances of Handler interface are used as a deferred code,
     * which should be run after particular IV is available in batch resolution results 
     */
    private interface Handler {
        void handle(IV newIV);
    }
    
    /**
     * Deferred handlers, linked to particular BigdataValue resolution
     */
    private final Map<BigdataValue, List<Handler>> deferred = new LinkedHashMap<>();
    /**
	 * Deferred handlers, NOT linked to particular BigdataValue resolution, used
	 * to provide sets of resolved default and named graphs into DataSetSummary
	 * constructor
	 */
    private final List<Runnable> deferredRunnables = new ArrayList<>();
    /**
	 * IVs of graphs, which will be later passed to
	 * {@link #resolveDataset(AST2BOpContext, Map)}.
	 * 
	 * No need for synchronization or keeping order of values.
	 */
    private final Map<Value, IV> resolvedValues = new HashMap<>();
    

   /**
     * Do deferred resolution of IVs, which were left unresolved while preparing the query
     * @param store - triple store, which will be used for values resolution
     * @param ast - AST model of the query, which should be resolved
     * @throws MalformedQueryException
     */
    public static void resolveQuery(final AbstractTripleStore store, final ASTContainer ast) throws MalformedQueryException {

        /*
         * Prevent running IV resolution more than once.
         * Property RESOLVED is set after resolution completed,
         * so subsequent repetitive calls to query evaluate
         * (for example with different bindings) would not result
         * in running resolution again.
         */
        if (Boolean.TRUE.equals(ast.getProperty(Annotations.RESOLVED))) {
        	return;
        }
        
    	final long beginNanos = System.nanoTime();
    	
        final QueryRoot queryRoot = (QueryRoot)ast.getProperty(Annotations.ORIGINAL_AST);

        final AST2BOpContext context = new AST2BOpContext(ast, store);

        /*
         * Handle dataset declaration
         * 
         * Note: Filters can be attached in order to impose ACLs on the
         * query. This has to be done at the application layer at this
         * point, but it might be possible to extend the grammar for this.
         * The SPARQL end point would have to be protected from external
         * access if this were done. Perhaps the better way to do this is to
         * have the NanoSparqlServer impose the ACL filters. There also
         * needs to be an authenticated identity to make this work and that
         * could be done via an integration within the NanoSparqlServer web
         * application container.
         * 
         * Note: This handles VIRTUAL GRAPH resolution.
         */
        final Map<IDataSetNode, List<ASTDatasetClause>> dcLists = new LinkedHashMap<>();
        {
            final ASTQueryContainer qc = (ASTQueryContainer) ast.getProperty(Annotations.PARSE_TREE);
            if (qc != null && qc.getOperation() != null) {
                final List<ASTDatasetClause> dcList = new ArrayList<>();
                dcList.addAll(qc.getOperation().getDatasetClauseList());
                dcLists.put(queryRoot, dcList);
            }
            
        }
        
        /*
         * I think here we could set the value expressions and do last- minute
         * validation.
         */
        final ASTSetValueExpressionsOptimizer opt = new ASTSetValueExpressionsOptimizer();

        final QueryRoot queryRoot2 = (QueryRoot) opt.optimize(context, new QueryNodeWithBindingSet(queryRoot, null)).getQueryNode();

        final ASTDeferredIVResolution termsResolver = new ASTDeferredIVResolution();
        
        termsResolver.resolve(context, queryRoot2, dcLists);
        
        queryRoot2.setPrefixDecls(ast.getOriginalAST().getPrefixDecls());
        
        ast.setOriginalAST(queryRoot2);

        ast.setResolveValuesTime(System.nanoTime() - beginNanos);

        ast.setProperty(Annotations.RESOLVED, Boolean.TRUE);

    }

    /**
     * Do deferred resolution of IVs, which were left unresolved while preparing the update
     * @param store - triple store, which will be used for values resolution
     * @param ast - AST model of the update, which should be resolved
     * @throws MalformedQueryException
     */
    public static void resolveUpdate(final AbstractTripleStore store, final ASTContainer ast) throws MalformedQueryException {

        /*
         * Prevent running IV resolution more than once.
         * Property RESOLVED is set after resolution completed,
         * so subsequent repetitive calls to update execute
         * (for example with different bindings) would not result
         * in running resolution again.
         */
        if (Boolean.TRUE.equals(ast.getProperty(Annotations.RESOLVED))) {
        	return;
        }
        
    	final long beginNanos = System.nanoTime();
    	
        final UpdateRoot qc = (UpdateRoot)ast.getProperty(Annotations.ORIGINAL_AST);
        
        /*
         * Handle dataset declaration. It only appears for DELETE/INSERT
         * (aka ASTModify). It is attached to each DeleteInsertNode for
         * which it is given.
         */
        final Map<IDataSetNode, List<ASTDatasetClause>> dcLists = new LinkedHashMap<>();
        for (final Update update: qc.getChildren()) {
            if (update instanceof IDataSetNode) {
                final List<ASTDatasetClause> dcList = new ArrayList();
                dcList.addAll(update.getDatasetClauses());
                dcLists.put((IDataSetNode)update, dcList);
            }
        }
        
        final AST2BOpContext context2 = new AST2BOpContext(ast, store);

        final ASTDeferredIVResolution termsResolver = new ASTDeferredIVResolution();

        termsResolver.resolve(context2, qc, dcLists);

        if (ast.getOriginalUpdateAST().getPrefixDecls()!=null && !ast.getOriginalUpdateAST().getPrefixDecls().isEmpty()) {

            qc.setPrefixDecls(ast.getOriginalUpdateAST().getPrefixDecls());

        }

        ast.setOriginalUpdateAST(qc);

        ast.setResolveValuesTime(System.nanoTime() - beginNanos);

        ast.setProperty(Annotations.RESOLVED, Boolean.TRUE);

    }

    /**
     * Schedule resolution if IV in provided value, provided handler
     * will be used to process resolved IV. If provided BigdataValue was created
     * using BigdataValueFactory bound to triple store, and has real IV already
     * resolved (for example dataset definition), handler will be fired immediately
     * and no value will be queued for resolution. 
     * @param value
     * @param handler
     */
    private void defer(final BigdataValue value, final Handler handler) {
        if (value!=null) {
            if (value.getValueFactory() instanceof BigdataValueFactoryImpl && value.isRealIV()) {
                handler.handle(value.getIV());
                return;
            }
            List<Handler> handlers = deferred.get(value);
            if (handlers==null) {
                handlers = new ArrayList<>();
                deferred.put(value, handlers);
            }
            handlers.add(handler);
        }
    }

    /**
     * Schedule execution of runnable after batch resulution if IVs
     * (to handle creation of DataSetSummary instances for default
     * and named graphs collections)
     * @param runnable
     */
    private void deferRunnable(final Runnable runnable) {
        deferredRunnables.add(runnable);
    }

    /**
     * Prepare and execute batch resolution of IVs in provided queryNode
     * @param dcList 
     * @throws MalformedQueryException 
     */
    private void resolve(final AST2BOpContext context, final QueryNodeBase queryNode, final Map<IDataSetNode, List<ASTDatasetClause>> dcLists) throws MalformedQueryException {

        // prepare deferred handlers for batch IVs resolution
        prepare(context, queryNode);

		// Assign DataSetNode to each IDataSetNode (sets up handlers that are
		// then invoked by call backs).
		resolveDataset(context, dcLists);

        // execute batch resolution and run all deferred handlers,
        // which will update unresolved values across AST model 
        resolveIVs(context);
        
    }

    /**
	 * Method is invoked after IV resolution and sets the
	 * {@link IDataSetNode#setDataset(DatasetNode) data set} on each
	 * {@link IDataSetNode} to a newly constructed {@link DatasetNode}
	 * object.
	 * 
	 * @param context
	 * @param dcLists
	 *            A mapping from the {@link IDataSetNode} objects to the set of
	 *            {@link ASTDatasetClause data set declarations clauses} (there
	 *            can be more than one for SPARQL UPDATE).
	 * 
	 * @throws MalformedQueryException
	 */
    private void resolveDataset(final AST2BOpContext context, final Map<IDataSetNode, List<ASTDatasetClause>> dcLists) throws MalformedQueryException {
       
    	for (final Entry<IDataSetNode, List<ASTDatasetClause>> dcList: dcLists.entrySet()) {
        
    		final boolean update = dcList.getKey() instanceof Update;
            
    		final List<ASTDatasetClause> datasetClauses = dcList.getValue();

			if (datasetClauses != null && !datasetClauses.isEmpty()) {

				if (!context.getAbstractTripleStore().isQuads()) {
					throw new QuadsOperationInTriplesModeException(
							"NAMED clauses in queries are not supported in" + " triples mode.");
				}

			    /*
			     * Lazily instantiated sets for the default and named graphs.
			     */

				final Set<IV<?,?>> defaultGraphs = new LinkedHashSet<>();

				final Set<IV<?,?>> namedGraphs = new LinkedHashSet<>();

                for (final ASTDatasetClause dc : datasetClauses) {
                
                    final ASTIRI astIri = dc.jjtGetChild(ASTIRI.class);
    
                    // Setup the callback handler : TODO Pull handler into a private class? Questions about scope for defaultGraphs and namedGraphs.
					defer((BigdataURI) astIri.getRDFValue(), new Handler() {

						@Override
                        public void handle(final IV newIV) {
                        
							final BigdataValue uri = newIV.getValue();
                            
							if (dc.isVirtual()) {

                                if (uri.getIV().isNullIV()) {
                                    /*
                                     * A virtual graph was referenced which is not
                                     * declared in the database. This virtual graph will
                                     * not have any members.
                                     */
                                    throw new RuntimeException("Not declared: " + uri);
                                }
                                
                                final IV virtualGraph = resolvedValues.get(BD.VIRTUAL_GRAPH);

                                if (virtualGraph == null) {

                                    throw new RuntimeException("Not declared: "
                                            + BD.VIRTUAL_GRAPH);
                                    
                                }

                                final IAccessPath<ISPO> ap = context.getAbstractTripleStore()
                                        .getSPORelation()
                                        .getAccessPath(//
                                                uri.getIV(),// the virtual graph "name"
                                                virtualGraph, null/* o */, null/* c */);
                                
                                final Iterator<ISPO> itr = ap.iterator();
                                
                                while(itr.hasNext()) {

                                    final IV memberGraph = itr.next().o();
                                    final BigdataValue value = context.getAbstractTripleStore().getLexiconRelation().getTerm(memberGraph);
                                    memberGraph.setValue(value);

                                    if (dc.isNamed()) {

                                        namedGraphs.add(memberGraph);

                                    } else {

                                        defaultGraphs.add(memberGraph);

                                    }

                                }
                                
                            } else {

                                if (uri.getIV() != null) {

                                    if (dc.isNamed()) {

                                        namedGraphs.add(uri.getIV());

                                    } else {

                                        defaultGraphs.add(uri.getIV());

                                    }

                                }

                            }
						}
					});
					deferRunnable(new Runnable(){
					    public void run() {
							if (!defaultGraphs.isEmpty() || !namedGraphs.isEmpty()) {

								// Note: Cast required to shut up the compiler.
								final DatasetNode datasetNode = new DatasetNode((Set) defaultGraphs, (Set) namedGraphs,
										update);

								/*
								 * Set the data set on the QueryRoot or
								 * DeleteInsertGraph node.
								 */
								dcList.getKey().setDataset(datasetNode);

							}
							
						}
                    });

                }
            }
        }
    }
    
    /**
     * prepare deferred handlers for batch IVs resolution. the handlers will go back
     * and put the IVs into place afterwards.
	 *
     * @param context
     * @param queryNode
     * @param dcList 
     */
    private void prepare(
        final AST2BOpContext context, final QueryNodeBase queryNode) {

        if (queryNode instanceof QueryRoot) {

        	fillInIV(context, ((QueryRoot)queryNode).getDataset());
            
        }

        if (queryNode instanceof UpdateRoot) {
            
        	final UpdateRoot updateRoot = (UpdateRoot) queryNode;
            
        	fillInIV(context, updateRoot);
            
        } else if (queryNode instanceof QueryBase) {
    
            final QueryBase queryRoot = (QueryBase) queryNode;
            
            // SELECT clause
            {
                final ProjectionNode projection = queryRoot.getProjection();
                if (projection!=null) {
                    fillInIV(context, projection);
                }
            }
    
            // Main CONSTRUCT clause
            {
    
                final GroupNodeBase constructClause = queryRoot.getConstruct();
    
                if (constructClause != null) {
    
                    fillInIV(context, constructClause);
                    
                }
    
            }
    
            // Main WHERE clause
            {
    
                final GroupNodeBase<IGroupMemberNode> whereClause = queryRoot
                        .getWhereClause();
    
                if (whereClause != null) {
    
                    fillInIV(context, whereClause);
                    
                }
    
            }
            
            // HAVING clause
            {
                final HavingNode having = queryRoot.getHaving();
                if (having!=null) {
                    for (final IConstraint c: having.getConstraints()) {
                        for (final BOp bop: c.args()) {
                            fillInIV(context, bop);
                        }
                    }
                }
            }
    
            
            // BINDINGS clause
            {
                final BindingsClause bc = queryRoot.getBindingsClause();
                if (bc!=null) {
                    for (final IBindingSet bs: bc.getBindingSets()) {
                        handleBindingSet(context, bs);
                    }
                }
            }
    
            // Named subqueries
            if (queryRoot instanceof QueryRoot && ((QueryRoot)queryRoot).getNamedSubqueries() != null) {
    
                final NamedSubqueriesNode namedSubqueries = ((QueryRoot)queryRoot)
                        .getNamedSubqueries();
    
                /*
                 * Note: This loop uses the current size() and get(i) to avoid
                 * problems with concurrent modification during visitation.
                 */
                for (int i = 0; i < namedSubqueries.size(); i++) {
    
                    final NamedSubqueryRoot namedSubquery = (NamedSubqueryRoot) namedSubqueries
                            .get(i);
    
                    final GroupNodeBase<IGroupMemberNode> whereClause = namedSubquery
                            .getWhereClause();
    
                    if (whereClause != null) {
    
                        fillInIV(context, whereClause);
                        
                    }
    
                }
    
            }
    
        }
        
    }

    private void handleBindingSet(final AST2BOpContext context, final IBindingSet s) {
        final Iterator<Entry<IVariable, IConstant>> itr = s.iterator();
        while (itr.hasNext()) {
            final Entry<IVariable, IConstant> entry = itr.next();
            final Object value = entry.getValue().get();
            if (value instanceof BigdataValue) {
                defer((BigdataValue)value, new Handler(){
                    @Override
                    public void handle(final IV newIV) {
                        entry.setValue(new Constant(newIV));
                    }
                });
            } else if (value instanceof TermId) {
                defer(((TermId)value).getValue(), new Handler(){
                    @Override
                    public void handle(final IV newIV) {
                        entry.setValue(new Constant(newIV));
                    }
                });
            }
        }
    }

    /**
     * Transitively handles all unresolved IVs inside provided bop
     */
    private void fillInIV(final AST2BOpContext context, final BOp bop) {
        if (bop==null) {
            return;
        }
        
        if (bop instanceof ConstantNode) {
            
            final BigdataValue value = ((ConstantNode) bop).getValue();
            if (value!=null) {
                // even if iv is already filled in we should try to resolve it against triplestore,
                // as previously resolved IV may be inlined, but expected to be term from lexicon relation on evaluation
                defer(value, new Handler(){
                    @Override
                    public void handle(final IV newIV) {
                        ((ConstantNode) bop).setArg(0, new Constant(newIV));
                    }
                });
            }
            return;
        }

        for (int k = 0; k < bop.arity(); k++) {
            final BOp pathBop = bop.get(k);
            if (pathBop instanceof Constant) {
                final Object v = ((Constant)pathBop).get();
                final int fk = k;
                if (v instanceof BigdataValue) {
                    defer((BigdataValue)v, new Handler(){
                        @Override
                        public void handle(final IV newIV) {
                            bop.args().set(fk, new Constant(newIV));
                        }
                    });
                } else if (v instanceof TermId) {
                    defer(((TermId)v).getValue(), new Handler(){
                        @Override
                        public void handle(final IV newIV) {
                            if (bop.args() instanceof ArrayList) {
                                bop.args().set(fk, new Constant(newIV));
                            }
                        }
                    });
                }
            } else {
                fillInIV(context,pathBop);
            }
        }

        if (bop instanceof CreateGraph) {
            fillInIV(context,((CreateGraph)bop).getTargetGraph());
        } if (bop instanceof AbstractGraphDataUpdate) {
            final AbstractGraphDataUpdate update = ((AbstractGraphDataUpdate)bop);
            // @see https://jira.blazegraph.com/browse/BLZG-1176
            // Check for using context value in DATA block with triple store not supporting quads
            // Moved from com.bigdata.rdf.sail.sparql.UpdateExprBuilder.doUnparsedQuadsDataBlock(ASTUpdate, Object, boolean, boolean)
            if (!context.isQuads()) {
                for (final BigdataStatement sp: update.getData()) {
                    if (sp.getContext()!=null) {
                        throw new QuadsOperationInTriplesModeException(
                                "Quads in SPARQL update data block are not supported " +
                                "in triples mode.");
                    }
                }
            }
        } if (bop instanceof DeleteInsertGraph) {
            // @see https://jira.blazegraph.com/browse/BLZG-1176
            // Check for using WITH keyword with triple store not supporting quads
            // Moved from com.bigdata.rdf.sail.sparql.UpdateExprBuilder.visit(ASTModify, Object)
            // TODO: needs additional unit tests, see https://jira.blazegraph.com/browse/BLZG-1518
            if (!context.isQuads() && ((DeleteInsertGraph)bop).getContext()!=null) {
                throw new QuadsOperationInTriplesModeException(
                    "Using named graph referenced through WITH clause " +
                    "is not supported in triples mode.");
            }
            fillInIV(context, ((DeleteInsertGraph)bop).getDataset());
            fillInIV(context, ((DeleteInsertGraph)bop).getDeleteClause());
            fillInIV(context, ((DeleteInsertGraph)bop).getInsertClause());
            fillInIV(context, ((DeleteInsertGraph)bop).getWhereClause());

        } else if (bop instanceof QuadsDataOrNamedSolutionSet) {
            fillInIV(context, ((QuadsDataOrNamedSolutionSet)bop).getQuadData());
        } else if (bop instanceof DatasetNode) {
            final DatasetNode dataset = ((DatasetNode) bop);
            final Set<IV> newDefaultGraphs = new LinkedHashSet<IV>();
            for(final IV iv: dataset.getDefaultGraphs().getGraphs()) {
                defer(iv.getValue(), new Handler(){
                    @Override
                    public void handle(final IV newIV) {
                        newDefaultGraphs.add(newIV);
                    }
                });
            }
            deferRunnable(new Runnable(){
                @Override
                public void run() {
                    dataset.setDefaultGraphs(new DataSetSummary(newDefaultGraphs, true));
                }
            });
            
            final Set<IV> newNamedGraphs = new LinkedHashSet<IV>();
            final Iterator<IV> namedGraphs = dataset.getNamedGraphs().getGraphs().iterator();
            while(namedGraphs.hasNext()) {
                final IV iv = namedGraphs.next();
                defer(iv.getValue(), new Handler(){
                    @Override
                    public void handle(final IV newIV) {
                        newNamedGraphs.add(newIV);
                    }
                });
            }
            deferRunnable(new Runnable(){
                @Override
                public void run() {
                    dataset.setNamedGraphs(new DataSetSummary(newNamedGraphs, true));
                }
            });
        } else if (bop instanceof PathNode) {
            final PathAlternative path = ((PathNode) bop).getPathAlternative();
            for (int k = 0; k < path.arity(); k++) {
                final BOp pathBop = path.get(k);
                fillInIV(context,pathBop);
            }
        } else if (bop instanceof FunctionNode) {
            if (bop instanceof SubqueryFunctionNodeBase) {
                fillInIV(context, ((SubqueryFunctionNodeBase)bop).getGraphPattern());
            }
            final IValueExpression<? extends IV> fve = ((FunctionNode)bop).getValueExpression();
            if (fve instanceof IVValueExpression) {
                for (int k = 0; k < fve.arity(); k++) {
                    final IValueExpression<? extends IV> ve = ((FunctionNode)bop).getValueExpression();
                    final BOp pathBop = ve.get(k);
                    if (pathBop instanceof Constant && ((Constant)pathBop).get() instanceof TermId) {
                        final BigdataValue v = ((TermId) ((Constant)pathBop).get()).getValue();
                        final int fk = k;
                        defer(v, new Handler(){
                            @Override
                            public void handle(final IV newIV) {
                                final BigdataValue resolved = context.getAbstractTripleStore().getValueFactory().asValue(v);
                                if (resolved.getIV() == null && newIV!=null) {
                                    resolved.setIV(newIV);
                                    newIV.setValue(resolved);
                                    final Constant newConstant = new Constant(newIV);
                                    ((FunctionNode)bop).setValueExpression((IValueExpression<? extends IV>) ((IVValueExpression) ve).setArg(fk, newConstant));
                                    ((FunctionNode) bop).setArg(fk, new ConstantNode(newConstant));
                                }
                            }
                        });
                    }
                }
            } else if (fve instanceof Constant) {
                final Object value = ((Constant)fve).get();
                if (value instanceof BigdataValue) {
                    defer((BigdataValue)value, new Handler(){
                        @Override
                        public void handle(final IV newIV) {
                            ((FunctionNode)bop).setValueExpression(new Constant(newIV));
                        }
                    });
                }
            }
        } else if (bop instanceof ValueExpressionNode) {
            final IValueExpression<? extends IV> ve = ((ValueExpressionNode)bop).getValueExpression();
            for (int k = 0; k < ve.arity(); k++) {
                final BOp pathBop = ve.get(k);
                fillInIV(context,pathBop);
            }
        } else if (bop instanceof GroupNodeBase) {
            fillInIV(context, ((GroupNodeBase) bop).getContext());
        } else if (bop instanceof StatementPatternNode) {
            final StatementPatternNode sp = (StatementPatternNode)bop;
            // @see https://jira.blazegraph.com/browse/BLZG-1176
            // Check for using GRAPH keyword with triple store not supporting quads
            // Moved from GroupGraphPatternBuilder.visit(final ASTGraphGraphPattern node, Object data)
            if (!context.isQuads() && Scope.NAMED_CONTEXTS.equals(sp.getScope())) {
                throw new QuadsOperationInTriplesModeException(
                        "Use of GRAPH construct in query body is not supported " +
                        "in triples mode.");
            }
        } else if(bop instanceof ServiceNode) {
            final ServiceNode node = (ServiceNode) bop;
            final TermNode serviceRef = node.getServiceRef();
            defer(serviceRef.getValue(), new Handler(){
                @Override
                public void handle(final IV newIV) {
                    node.setServiceRef(new ConstantNode(new Constant(newIV)));
                }
            });
            fillInIV(context, node.getGraphPattern());

        } else if (bop instanceof QueryBase) {

            final QueryBase subquery = (QueryBase) bop;

            prepare(context, subquery);

        } else if (bop instanceof BindingsClause) {

            final List<IBindingSet> bsList = ((BindingsClause)bop).getBindingSets();

            if (bsList!=null) {

                for (final IBindingSet bs: bsList) {

                    handleBindingSet(context, bs);

                }

            }

        } else if(bop instanceof FilterNode) {

            final FilterNode node = (FilterNode)bop;

            fillInIV(context,node.getValueExpression());

        }

    }

	/**
	 * Does the batch resolution of IVs against the database and applies the
	 * handlers to set those IVs on the AST.
	 * <p>
	 * Note: This is also adding some RDF vocabulary items for RDF syntactic
	 * sugar (rdf:first, rdf:rest, etc.).
	 * 
	 * @param context
	 */
    private void resolveIVs(final AST2BOpContext context) {

        /*
         * Build up the vocabulary (some key vocabulary items which correspond to syntactic sugar in SPARQL.)
         */
        final BigdataValueFactory f = context.getAbstractTripleStore().getValueFactory();
        final List<BigdataValue> vocab = new LinkedList<>();
        {
            
            // RDF Collection syntactic sugar vocabulary items.
            vocab.add(f.asValue(RDF.FIRST));
            vocab.add(f.asValue(RDF.REST));
            vocab.add(f.asValue(RDF.NIL));
            vocab.add(f.asValue(BD.VIRTUAL_GRAPH));

        }

        final IV[] ivs = new IV[deferred.size()+vocab.size()];
        final BigdataValue[] values = new BigdataValue[deferred.size()+vocab.size()];
        {
        
            /*
             * First, build up the set of unknown terms.
             */
            int i = 0;
            
            /*
             * Adding vocab values. Note, that order of values in array is important
             * as vocab values should be available and resolved while running handlers
             * for values in deferred list
             */
            
            for (final BigdataValue v: vocab) {

                final BigdataValue toBeResolved = f.asValue(v);
                ivs[i] = TermId.mockIV(VTE.valueOf(v));
                toBeResolved.clearInternalValue();
                values[i++] = toBeResolved;
                
            }

            for (final BigdataValue v: deferred.keySet()) {

                /*
                 * Ensure that BigdataValue objects belong to the headless ValueFactory
                 */
                if (v!=null) {
                    BigdataValueFactory vf = v.getValueFactory();
                    if (vf == null) {
                        throw new RuntimeException("Invalid null valueFactory for value="+v);
                    } else if (vf instanceof BigdataValueFactoryImpl) {
                        throw new RuntimeException("Invalid valueFactory class "+vf.getClass().getName()+" namespace="+((BigdataValueFactoryImpl)vf).getNamespace()+" != " + ((BigdataValueFactoryImpl)f).getNamespace() + " for value="+v);
                    } else if (!(vf instanceof BigdataValueFactoryHeadlessImpl)) {
                        throw new RuntimeException("Invalid valueFactory class "+vf.getClass().getName()+" for value="+v);
                    }
                }

                BigdataValue toBeResolved = f.asValue(v);
                ivs[i] = v.getIV();
                toBeResolved.clearInternalValue();
                values[i++] = toBeResolved;
                
            }

            /*
             * Batch resolution.
             */
            
            if (log.isDebugEnabled())
                log.debug("UNKNOWNS: " + Arrays.toString(values));

            context.getAbstractTripleStore().getLexiconRelation()
                    .addTerms(values, values.length, true/* readOnly */);

        }

        /*
         * Replace the value expression with one which uses the resolved IV.
         */

        {
            for (int i = 0; i<values.length; i++) {

                final BigdataValue v = values[i];
                final IV iv;
                if (v.isRealIV()) {

                    if (log.isInfoEnabled())
                        log.info("RESOLVED: " + v + " => " + v.getIV());

                    /*
                     * Note: If the constant is an effective constant
                     * because it was given in the binding sets then we also
                     * need to capture the variable name associated with
                     * that constant.
                     */

                    iv = v.getIV();
                } else {
                    if (v instanceof Literal) {
                        final String label = ((Literal)v).getLabel();
                        final URI dataType = ((Literal)v).getDatatype();
                        final String language = ((Literal)v).getLanguage();
                        final BigdataValue resolved;
                        if (language!=null) {
                            resolved = context.getAbstractTripleStore().getValueFactory().createLiteral(label, language);
                        } else {
                            resolved = context.getAbstractTripleStore().getValueFactory().createLiteral(label, dataType);
                        }
                        final DTE dte = DTE.valueOf(dataType);
                        if (dte!=null) {
                            iv = IVUtility.decode(label, dte.name());
                        } else {
                            iv = TermId.mockIV(VTE.valueOf(v));
                        }
                        iv.setValue(resolved);
                        resolved.setIV(iv);
                    } else {
                        iv = ivs[i];
                    }
                }
                if (iv!=null) {
                    iv.setValue(v);
                    v.setIV(iv);
                    
                    final List<Handler> deferredHandlers = deferred.get(v);
                    if (deferredHandlers!=null) { // No handlers are usually defined for vocab values (see above)
                        for(final Handler handler: deferredHandlers) {
                            handler.handle(iv);
                        }
                    }
                    // overwrite entry with unresolved key by entry with resolved key
                    resolvedValues.put(v, iv);
                }

            }

        }
        
        for(final Runnable r: deferredRunnables) {
            r.run();
        }

        /*
         * Ensure that BigdataValue objects belong to the headless ValueFactory
         */
        {
            for (IV iv: resolvedValues.values()) {
            	BigdataValueFactory vf = iv.getValue().getValueFactory();
                if (vf == null) {
                	throw new RuntimeException("Invalid null valueFactory for value="+iv);
                } else if (!(vf instanceof BigdataValueFactoryImpl)) {
                	throw new RuntimeException("Invalid valueFactory class "+vf.getClass().getName()+" for value="+iv);
                } else if (vf!=f) {
                	throw new RuntimeException("Invalid valueFactory instance "+vf+"!="+f+" for value="+iv);
                }
            }
        }
        
    }
}
