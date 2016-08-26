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
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.query.impl.MapBindingSet;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.constraints.IVValueExpression;
import com.bigdata.rdf.internal.impl.AbstractIV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.sparql.ast.ASTDatasetClause;
import com.bigdata.rdf.sail.sparql.ast.ASTIRI;
import com.bigdata.rdf.sail.sparql.ast.ASTQueryContainer;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.ASTContainer.Annotations;
import com.bigdata.rdf.sparql.ast.AbstractFromToGraphManagement;
import com.bigdata.rdf.sparql.ast.AbstractGraphDataUpdate;
import com.bigdata.rdf.sparql.ast.AbstractOneGraphManagement;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.DeleteInsertGraph;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.GroupByNode;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.HavingNode;
import com.bigdata.rdf.sparql.ast.IDataSetNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.PathNode;
import com.bigdata.rdf.sparql.ast.PathNode.PathAlternative;
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

//    /**
//     * The target triple store.
//     */
//    private final AbstractTripleStore store;
    
    /**
     * The value factory for that target triple store.
     */
    private final BigdataValueFactory vf;
    
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
    
    private ASTDeferredIVResolution(AbstractTripleStore store) {

        if (store == null)
            throw new IllegalArgumentException();

//        this.store = store;
        
        this.vf = store.getValueFactory();
        
    }
    
    static class DeferredResolutionResult {

        BindingSet bindingSet;
        Dataset dataset;

        public DeferredResolutionResult(final BindingSet bindingSet, final Dataset dataset) {
            this.bindingSet = bindingSet;
            this.dataset = dataset;
        }

    }

    /**
     * Do deferred resolution of IVs, which were left unresolved while preparing
     * the query
     * 
     * @param store
     *            - triple store, which will be used for values resolution
     * @param ast
     *            - AST model of the query, which should be resolved
     * @throws MalformedQueryException
     */
    public static DeferredResolutionResult resolveQuery(final AbstractTripleStore store, final ASTContainer ast) throws MalformedQueryException {
        return resolveQuery(store, ast, null, null, null /* context unknown */);
    }
    
    /**
     * Do deferred resolution of IVs, which were left unresolved while preparing
     * the query
     * 
     * @param store
     *            - triple store, which will be used for values resolution
     * @param ast
     *            - AST model of the query, which should be resolved
     * @param bs
     *            - binding set, which should be resolved 
     * @param dataset 
     * @throws MalformedQueryException
     */
    public static DeferredResolutionResult resolveQuery(
        final AbstractTripleStore store, final ASTContainer ast, 
        final BindingSet bs, final Dataset dataset, final AST2BOpContext ctxIn) throws MalformedQueryException {

        // whenever a context is provided, use that one, otherwise construct it
        final AST2BOpContext ctx = ctxIn==null ? new AST2BOpContext(ast, store) : ctxIn;
        
        final ASTDeferredIVResolution termsResolver = new ASTDeferredIVResolution(store);

        // process provided binding set
        final BindingSet resolvedBindingset =  termsResolver.handleBindingSet(store, bs);

        // process provided dataset
        final Dataset resolvedDataset = termsResolver.handleDataset(store, dataset);

        /*
         * Prevent running IV resolution more than once.
         * Property RESOLVED is set after resolution completed,
         * so subsequent repetitive calls to query evaluate
         * (for example with different bindings) would not result
         * in running resolution again.
         */
        if (Boolean.TRUE.equals(ast.getProperty(Annotations.RESOLVED))) {
            /*
             * Resolve binding set or dataset if there are any values to be processed
             */
            if (!termsResolver.deferred.isEmpty()) {
                termsResolver.resolveIVs(store);
            }
            // fast path for pre-resolved query.
        	return new DeferredResolutionResult(resolvedBindingset, resolvedDataset);
        }
        
    	final long beginNanos = System.nanoTime();
    	
        final QueryRoot queryRoot = (QueryRoot)ast.getProperty(Annotations.ORIGINAL_AST);

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

        final QueryRoot queryRoot2 = (QueryRoot) opt.optimize(ctx, new QueryNodeWithBindingSet(queryRoot, null)).getQueryNode();

        termsResolver.resolve(store, queryRoot2, dcLists, bs);
        
        queryRoot2.setPrefixDecls(ast.getOriginalAST().getPrefixDecls());
        
        ast.setOriginalAST(queryRoot2);

        ast.setResolveValuesTime(System.nanoTime() - beginNanos);

        ast.setProperty(Annotations.RESOLVED, Boolean.TRUE);

        // Note: Full recursive traversal can be used to inspect post-condition of all bops.
//        if(true) {
//            final Iterator<BOp> itr = BOpUtility.preOrderIteratorWithAnnotations(ast);
//            while(itr.hasNext()) {
//                final BOp bop = itr.next();
//                log.error("bop="+bop);
//            }
//        }
        
        return new DeferredResolutionResult(resolvedBindingset, resolvedDataset);
    }

    /**
     * Do deferred resolution of IVs, which were left unresolved while preparing the update
     * @param store - triple store, which will be used for values resolution
     * @param ast - AST model of the update, which should be resolved
     * @throws MalformedQueryException
     */
    public static DeferredResolutionResult resolveUpdate(final AbstractTripleStore store, final ASTContainer ast) throws MalformedQueryException {
        return resolveUpdate(store, ast, null, null);
    }

    /**
     * Do deferred resolution of IVs, which were left unresolved while preparing the update
     * @param store - triple store, which will be used for values resolution
     * @param ast - AST model of the update, which should be resolved
     * @param bs - binding set, which should be resolved
     * @param dataset 
     * @return 
     * @throws MalformedQueryException
     */
    public static DeferredResolutionResult resolveUpdate(final AbstractTripleStore store, final ASTContainer ast, final BindingSet bs, final Dataset dataset) throws MalformedQueryException {

        final ASTDeferredIVResolution termsResolver = new ASTDeferredIVResolution(store);

        // process provided binding set
        BindingSet resolvedBindingSet = termsResolver.handleBindingSet(store, bs);

        // process provided dataset
        final Dataset resolvedDataset = termsResolver.handleDataset(store, dataset);

        /*
         * Prevent running IV resolution more than once.
         * Property RESOLVED is set after resolution completed,
         * so subsequent repetitive calls to update execute
         * (for example with different bindings) would not result
         * in running resolution again.
         */
        if (Boolean.TRUE.equals(ast.getProperty(Annotations.RESOLVED))) {
            /*
             * Resolve binding set or dataset if there are any values to be processed
             */
            if (!termsResolver.deferred.isEmpty()) {
                termsResolver.resolveIVs(store);
            }
            return new DeferredResolutionResult(resolvedBindingSet, resolvedDataset);
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
        

        termsResolver.resolve(store, qc, dcLists, bs);

        if (ast.getOriginalUpdateAST().getPrefixDecls()!=null && !ast.getOriginalUpdateAST().getPrefixDecls().isEmpty()) {

            qc.setPrefixDecls(ast.getOriginalUpdateAST().getPrefixDecls());

        }

        ast.setOriginalUpdateAST(qc);

        ast.setResolveValuesTime(System.nanoTime() - beginNanos);

        ast.setProperty(Annotations.RESOLVED, Boolean.TRUE);

        return new DeferredResolutionResult(resolvedBindingSet, resolvedDataset);

    }

    /**
     * Do deferred resolution of IVs, which were left unresolved after execution of each Update in UpdateRoot
     * @param store - triple store, which will be used for values resolution
     * @param ast - AST model of the update, which should be resolved
     * @param bs - binding set, which should be resolved
     * @param dataset 
     * @return 
     * @throws MalformedQueryException
     */
    public static DeferredResolutionResult resolveUpdate(final AbstractTripleStore store, final Update update, final BindingSet bs, final Dataset dataset) throws MalformedQueryException {

        final ASTDeferredIVResolution termsResolver = new ASTDeferredIVResolution(store);

        // process provided binding set
        BindingSet resolvedBindingSet = termsResolver.handleBindingSet(store, bs);

        // process provided dataset
        final Dataset resolvedDataset = termsResolver.handleDataset(store, dataset);

//    	final long beginNanos = System.nanoTime();
    	
        termsResolver.resolve(store, update, null/*datasetClauseLists*/, bs);

//        ast.setResolveValuesTime(System.nanoTime() - beginNanos);

        return new DeferredResolutionResult(resolvedBindingSet, resolvedDataset);

    }

    /**
     * Schedule resolution if IV in provided value, provided handler will be
     * used to process resolved IV. If provided BigdataValue was created using
     * BigdataValueFactory bound to triple store, and has real IV already
     * resolved (for example dataset definition), handler will be fired
     * immediately and no value will be queued for resolution.
     * 
     * @param value
     * @param handler
     */
    private void defer(final BigdataValue value, final Handler handler) {
        if (value == null)
            return;
        if (value.getValueFactory() == vf && value.isRealIV()) {
            /*
             * We have a BigdataValue that belongs to the correct namespace and
             * which has already been resolved to a real IV.
             */
            if (value.getIV().needsMaterialization()) {
                value.getIV().setValue(value);
            }
            handler.handle(value.getIV());
            return;
        }
        List<Handler> handlers = deferred.get(value);
        if (handlers == null) {
            handlers = new ArrayList<>();
            deferred.put(value, handlers);
        }
        handlers.add(handler);
    }

    /**
     * Schedule execution of runnable after batch resolution if IVs (to handle
     * creation of DataSetSummary instances for default and named graphs
     * collections)
     * 
     * @param runnable
     */
    private void deferRunnable(final Runnable runnable) {
        deferredRunnables.add(runnable);
    }

    /**
     * Prepare and execute batch resolution of IVs in provided queryNode
     * @param bs 
     * @param dcList 
     * @throws MalformedQueryException 
     */
    private void resolve(final AbstractTripleStore store, final QueryNodeBase queryNode, final Map<IDataSetNode, List<ASTDatasetClause>> dcLists, final BindingSet bs) throws MalformedQueryException {

        // prepare deferred handlers for batch IVs resolution
        prepare(store, queryNode);
        
		// Assign DataSetNode to each IDataSetNode (sets up handlers that are
		// then invoked by call backs).
		resolveDataset(store, dcLists);

        // execute batch resolution and run all deferred handlers,
        // which will update unresolved values across AST model 
        resolveIVs(store);
        
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
    private void resolveDataset(final AbstractTripleStore store, final Map<IDataSetNode, List<ASTDatasetClause>> dcLists) throws MalformedQueryException {
       
        if (dcLists == null) {
            return;
        }
        
    	for (final Entry<IDataSetNode, List<ASTDatasetClause>> dcList: dcLists.entrySet()) {
        
    		final boolean update = dcList.getKey() instanceof Update;
            
    		final List<ASTDatasetClause> datasetClauses = dcList.getValue();

			if (datasetClauses != null && !datasetClauses.isEmpty()) {

				if (!store.isQuads()) {
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

                                final IAccessPath<ISPO> ap = store
                                        .getSPORelation()
                                        .getAccessPath(//
                                                uri.getIV(),// the virtual graph "name"
                                                virtualGraph, null/* o */, null/* c */);
                                
                                final Iterator<ISPO> itr = ap.iterator();
                                
                                while(itr.hasNext()) {

                                    final IV memberGraph = itr.next().o();
                                    final BigdataValue value = store.getLexiconRelation().getTerm(memberGraph);
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
    private void prepare(final AbstractTripleStore store, final QueryNodeBase queryNode) {

        if (queryNode instanceof QueryRoot) {

        	fillInIV(store, ((QueryRoot)queryNode).getDataset());
            
        }

        if (queryNode instanceof UpdateRoot) {
            
        	final UpdateRoot updateRoot = (UpdateRoot) queryNode;
            
        	fillInIV(store, updateRoot);
            
        } else if (queryNode instanceof Update) {
            
            final Update update = (Update) queryNode;
            
            fillInIV(store, update);
            
        } else if (queryNode instanceof QueryBase) {
    
            final QueryBase queryRoot = (QueryBase) queryNode;
            
            // SELECT clause
            {
                final ProjectionNode projection = queryRoot.getProjection();
                if (projection!=null) {
                    fillInIV(store, projection);
                }
            }
    
            // Main CONSTRUCT clause
            {
    
                final GroupNodeBase constructClause = queryRoot.getConstruct();
    
                if (constructClause != null) {
    
                    fillInIV(store, constructClause);
                    
                }
    
            }
    
            // Main WHERE clause
            {
    
                final GroupNodeBase<IGroupMemberNode> whereClause = queryRoot
                        .getWhereClause();
    
                if (whereClause != null) {
    
                    fillInIV(store, whereClause);
                    
                }
    
            }
            
            // GROUP BY clause
            {
                final GroupByNode groupBy = queryRoot.getGroupBy();
                
                if (groupBy != null) {
                	
                	fillInIV(store, groupBy);
                	
                }
            
            }
            // HAVING clause
            {
                final HavingNode having = queryRoot.getHaving();
                
                if (having != null) {
                	
                	fillInIV(store, having);
                	
                }
            }
    
            
            // BINDINGS clause
            {
                final BindingsClause bc = queryRoot.getBindingsClause();
                if (bc!=null) {
                    for (final IBindingSet bs: bc.getBindingSets()) {
                        handleBindingSet(store, bs);
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

                    // process subquery recursively, handling all the clauses of the subquery
                    // @see https://jira.blazegraph.com/browse/BLZG-1682
                    
                    prepare(store, namedSubquery);

                }
    
            }
    
        }
        
    }

    private void handleBindingSet(final AbstractTripleStore store, final IBindingSet s) {
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
            } else if (value instanceof AbstractIV) {
            	// See BLZG-1788 (Typed literals in VALUES clause not matching data)
            	// Changed from TermId to AbstractIV, as there are other types of IVs,
            	// which could require resolution against the store
            	// (for ex. FullyInlineTypedLiteralIV which represents typed literal)
                defer(((AbstractIV)value).getValue(), new Handler(){
                    @Override
                    public void handle(final IV newIV) {
                        entry.setValue(new Constant(newIV));
                    }
                });
            }
        }
    }

    private BindingSet handleBindingSet(final AbstractTripleStore store, final BindingSet bs) {
        if (bs != null) {

            MapBindingSet newBs = new MapBindingSet();
            
            for (Binding entry: bs) {
                Value value = entry.getValue();
                if (!(value instanceof BigdataValue)) {
                    if (bs instanceof QueryBindingSet) {
                        BigdataValue bValue = store.getValueFactory().asValue(value);
//                        bValue.setIV(TermId.mockIV(VTE.valueOf(bValue)));
//                        bValue.getIV().setValue(bValue);
                        value = bValue;
                    }
                }

                if (value instanceof BigdataValue && !((BigdataValue) value).isRealIV()) {
                    final BigdataValue bValue = (BigdataValue) value;
                    defer((BigdataValue)value, new Handler(){
                        @Override
                        public void handle(final IV newIV) {
                            bValue.setIV(newIV);
                        }
                    });
                }
                newBs.addBinding(entry.getName(), value);
            }
            return newBs;
        }
        return bs;
    }

    private Dataset handleDataset(final AbstractTripleStore store, final Dataset dataset) {
        if (dataset != null) {

            DatasetImpl newDataset = new DatasetImpl();
            
            for (final URI uri: dataset.getDefaultGraphs()) {
                URI value = handleDatasetGraph(store, uri);
                newDataset.addDefaultGraph(value);
            }
            for (final URI uri: dataset.getDefaultRemoveGraphs()) {
                URI value = handleDatasetGraph(store, uri);
                newDataset.addDefaultRemoveGraph(value);
            }
            for (final URI uri: dataset.getNamedGraphs()) {
                URI value = handleDatasetGraph(store, uri);
                newDataset.addNamedGraph(value);
            }
            URI value = handleDatasetGraph(store, dataset.getDefaultInsertGraph());
            newDataset.setDefaultInsertGraph(value);
            return newDataset;
        }
        return dataset;
    }

    private URI handleDatasetGraph(final AbstractTripleStore store, final URI uri) {
        URI value = uri;
        if (value!= null && !(value instanceof BigdataValue)) {
            value = store.getValueFactory().asValue(value);
        }

        if (value instanceof BigdataValue) {
            final BigdataValue bValue = (BigdataValue) value;
            defer((BigdataValue)value, new Handler(){
                @Override
                public void handle(final IV newIV) {
                    bValue.setIV(newIV);
                }
            });
        }
        return value;
    }

    /**
     * Transitively handles all unresolved IVs inside provided bop
     */
    private void fillInIV(final AbstractTripleStore store, final BOp bop) {
   
        if (bop == null) {
            return;
        }

        if (bop instanceof ConstantNode) {

            final BigdataValue value = ((ConstantNode) bop).getValue();
            if (value != null) {
                /*
                 * Even if iv is already filled in we should try to resolve it
                 * against triplestore, as previously resolved IV may be
                 * inlined, but expected to be term from lexicon relation on
                 * evaluation.
                 */
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
                        	if (bop instanceof BOpBase) {
                        		((BOpBase)bop).__replaceArg(fk, new Constant(newIV));
                        	} else {
	                            List<BOp> args = bop.args();
								if (args instanceof ArrayList) {
	                                args.set(fk, new Constant(newIV));
	                            } else {
	                            	log.warn("bop.args() class " + args.getClass() + " or " + bop.getClass() + " does not allow updates");
	                            }
                        	}
                        }
                    });
                }
            } else {
                fillInIV(store,pathBop);
            }
        }

        if (bop instanceof AbstractOneGraphManagement) {
            
            fillInIV(store,((AbstractOneGraphManagement)bop).getTargetGraph());
            
        } if (bop instanceof AbstractFromToGraphManagement) {

            fillInIV(store, ((AbstractFromToGraphManagement)bop).getSourceGraph());
            fillInIV(store, ((AbstractFromToGraphManagement)bop).getTargetGraph());

        } if (bop instanceof AbstractGraphDataUpdate) {
            final AbstractGraphDataUpdate update = ((AbstractGraphDataUpdate)bop);
            // @see https://jira.blazegraph.com/browse/BLZG-1176
            // Check for using context value in DATA block with triple store not supporting quads
            // Moved from com.bigdata.rdf.sail.sparql.UpdateExprBuilder.doUnparsedQuadsDataBlock(ASTUpdate, Object, boolean, boolean)
            if (!store.isQuads()) {
                for (final BigdataStatement sp: update.getData()) {
                    if (sp.getContext()!=null) {
                        throw new QuadsOperationInTriplesModeException(
                                "Quads in SPARQL update data block are not supported " +
                                "in triples mode.");
                    }
                }
            }
        } if (bop instanceof DeleteInsertGraph) {
            fillInIV(store, ((DeleteInsertGraph)bop).getDataset());
            fillInIV(store, ((DeleteInsertGraph)bop).getDeleteClause());
            fillInIV(store, ((DeleteInsertGraph)bop).getInsertClause());
            fillInIV(store, ((DeleteInsertGraph)bop).getWhereClause());

        } else if (bop instanceof QuadsDataOrNamedSolutionSet) {
            fillInIV(store, ((QuadsDataOrNamedSolutionSet)bop).getQuadData());
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
                fillInIV(store,pathBop);
            }
        } else if (bop instanceof FunctionNode) {
            if (bop instanceof SubqueryFunctionNodeBase) {
                fillInIV(store, ((SubqueryFunctionNodeBase)bop).getGraphPattern());
            }
            final IValueExpression<? extends IV> fve = ((FunctionNode)bop).getValueExpression();
            if (fve instanceof IVValueExpression) {
        		for (int k = 0; k < fve.arity(); k++) {
        		    final BOp veBop = fve.get(k);
        		    if (veBop instanceof Constant && ((Constant)veBop).get() instanceof TermId) {
        		        final BigdataValue v = ((TermId) ((Constant)veBop).get()).getValue();
        		        final int fk = k;
        		        defer(v, new Handler(){
        		            @Override
        		            public void handle(final IV newIV) {
        		                final BigdataValue resolved = vf.asValue(v);
        		                if (resolved.getIV() == null && newIV!=null) {
        		                    resolved.setIV(newIV);
        		                    newIV.setValue(resolved);
        		                    final Constant newConstant = new Constant(newIV);
        		                    // we need to reread value expression from the node, as it might get changed by sibling nodes resolution
        		                    // @see https://jira.blazegraph.com/browse/BLZG-1682
        		                    final IValueExpression<? extends IV> fve = ((ValueExpressionNode)bop).getValueExpression();
        		                    IValueExpression<? extends IV> newVe = (IValueExpression<? extends IV>) ((IVValueExpression) fve).setArg(fk, newConstant);
        		                    ((ValueExpressionNode)bop).setValueExpression(newVe);
        		                    ((ValueExpressionNode)bop).setArg(fk, new ConstantNode(newConstant));
        		                }
        		            }
        		        });
        		    } else if (veBop instanceof IVValueExpression) {
        		    	fillInIV(store, veBop);
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
                fillInIV(store,pathBop);
            }
        } else if (bop instanceof GroupNodeBase) {
            fillInIV(store, ((GroupNodeBase) bop).getContext());
        } else if (bop instanceof StatementPatternNode) {
            final StatementPatternNode sp = (StatementPatternNode)bop;
            // @see https://jira.blazegraph.com/browse/BLZG-1176
            // Check for using WITH keyword with triple store not supporting quads
            // Moved from com.bigdata.rdf.sail.sparql.UpdateExprBuilder.visit(ASTModify, Object)
            // Check for using GRAPH keyword with triple store not supporting quads
            // Moved from GroupGraphPatternBuilder.visit(final ASTGraphGraphPattern node, Object data)
            // At this point it is not possible to distinguish using WITH keyword from GRAPH construct,
            // as WITH scope was propagated into statement pattern
            if (!store.isQuads() && (sp.getScope()!=null && !(Scope.DEFAULT_CONTEXTS.equals(sp.getScope())))) {
                throw new QuadsOperationInTriplesModeException(
                        "Use of WITH and GRAPH constructs in query body is not supported " +
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
            fillInIV(store, node.getGraphPattern());

        } else if (bop instanceof QueryBase) {

            final QueryBase subquery = (QueryBase) bop;

            prepare(store, subquery);

        } else if (bop instanceof BindingsClause) {

            final List<IBindingSet> bsList = ((BindingsClause)bop).getBindingSets();

            if (bsList!=null) {

                for (final IBindingSet bs: bsList) {

                    handleBindingSet(store, bs);

                }

            }

        } else if(bop instanceof FilterNode) {

            final FilterNode node = (FilterNode)bop;

            fillInIV(store,node.getValueExpression());

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
    private void resolveIVs(final AbstractTripleStore store) {

        /*
         * Build up the vocabulary (some key vocabulary items which correspond to syntactic sugar in SPARQL.)
         */
        final List<BigdataValue> vocab = new LinkedList<>();
        {
            
            // RDF Collection syntactic sugar vocabulary items.
            vocab.add(vf.asValue(RDF.FIRST));
            vocab.add(vf.asValue(RDF.REST));
            vocab.add(vf.asValue(RDF.NIL));
            vocab.add(vf.asValue(BD.VIRTUAL_GRAPH));

        }

        final int nvalues = deferred.size() + vocab.size();
        final IV[] ivs = new IV[nvalues]; // FIXME REMOVE.
        final BigdataValue[] values = new BigdataValue[nvalues];
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

                final BigdataValue toBeResolved = v; // asValue() invoked above. // f.asValue(v);
                ivs[i] = TermId.mockIV(VTE.valueOf(v));
                if (!toBeResolved.isRealIV()) {
                	toBeResolved.clearInternalValue();
                }
                values[i++] = toBeResolved;
                
            }

            for (final BigdataValue v: deferred.keySet()) {
                
                final BigdataValue toBeResolved = vf.asValue(v);
                ivs[i] = v.getIV();
                if (!toBeResolved.isRealIV()) {
                	toBeResolved.clearInternalValue();
                }
                values[i++] = toBeResolved;
                
            }

            /*
             * Batch resolution.
             * 
             * Note: At this point all BigdataValue objects belong to the target
             * namespace.
             */
            
            if (log.isDebugEnabled())
                log.debug("UNKNOWNS: " + Arrays.toString(values));

            store.getLexiconRelation()
                    .addTerms(values, values.length, true/* readOnly */);

        }

        /*
         * Replace the value expression with one which uses the resolved IV.
         */

        {
            for (int i = 0; i < values.length; i++) {

                final BigdataValue v = values[i];

                final IV iv;
                if (v.isRealIV()) {

                    if (log.isDebugEnabled())
                        log.debug("RESOLVED: " + v + " => " + v.getIV());

                    /*
                     * Note: If the constant is an effective constant
                     * because it was given in the binding sets then we also
                     * need to capture the variable name associated with
                     * that constant.
                     */

                    iv = v.getIV();
                } else {
                    if (v instanceof Literal) {
                        /*
                         * This code path handles IVs not resolved by
                         * ASTDeferredIVResolutionInitializer, for example
                         * bindings, nor resolved by LexiconRelation, so we
                         * could not provide Term IV for a literal from a triple
                         * store, which configured to not use inlined values,
                         * due to that this value is not available in lexicon,
                         * thus there is no other was as to create it as inlined
                         * IV.
                         * 
                         * BBT: This appears to be related to the need to
                         * represent in the query constants that are not in the
                         * database but which nevertheless need to be captured
                         * in the query. For example, a constant might appear in
                         * VALUES () which is then used in a FILTER or a BIND().
                         * Or a constant could appear in a BIND(?x as 12). For
                         * those cases the Literal is being represented as a
                         * fully inline value. A number of SPARQL tests will
                         * fail if this code path is disabled, but note that the
                         * test will only fail if openrdf Value objects are
                         * being provided rather than BigdataValue objects, so
                         * the issue only shows up with embedded SPARQL query
                         * use.
                         * 
                         * @see com.bigdata.rdf.sail.TestBigdataValueReplacer.test_dropUnusedBindings()
                         * @see TestRollbacks
                         */
                        final String label = ((Literal) v).getLabel();
                        final URI dataType = ((Literal) v).getDatatype();
                        final String language = ((Literal) v).getLanguage();
                        final BigdataValue resolved;
                        if (language != null) {
                            resolved = vf.createLiteral(label, language);
                        } else {
                            resolved = vf.createLiteral(label, dataType);
                        }
                        final DTE dte = DTE.valueOf(dataType);
                        if (dte != null) {
                        	// Check if lexical form is empty, and keep FullyInlineTypedLiteralIV
                        	// holding corresponding data type as iv for the new value
                        	// @see https://jira.blazegraph.com/browse/BLZG-1716 (SPARQL Update parser fails on invalid numeric literals)
                        	if (label.isEmpty()) {
                        		iv = ivs[i];
                        	} else {
                        		// @see https://jira.blazegraph.com/browse/BLZG-2043 (xsd:integer IV not properly resolved when inlining disabled)
                        		// At this point null IV means, that LexiconRelation has not resolved it against triplestore, nor it could be inlined
                        		// according to inlining configuration, so we should use mockIV, as it was behavior of 1.5.1 version and prior
                        		// (before BLZG-1176 SPARQL Parsers should not be db mode aware)
//                      		iv = ASTDeferredIVResolutionInitializer.decode(label, dte.name());
                                iv = TermId.mockIV(VTE.valueOf(v));
                        	}
                        } else {
                            iv = TermId.mockIV(VTE.valueOf(v));
                        }
                        iv.setValue(resolved);
                        resolved.setIV(iv);
                    } else if (ivs[i] != null) {
                        iv = ivs[i];
                    } else {
                        iv = TermId.mockIV(VTE.valueOf(v)); // to support bindings, which were not resolved to IVs
                    }
                }

                if (iv != null) {
                    iv.setValue(v);
                    v.setIV(iv);

                    final List<Handler> deferredHandlers = deferred.get(v);
                    if (deferredHandlers != null) {
                        /*
                         * No handlers are usually defined for vocab values
                         * (see above).
                         */
                        for (final Handler handler : deferredHandlers) {
                            handler.handle(iv);
                        }
                    }
                    // overwrite entry with unresolved key by entry with
                    // resolved key
                    resolvedValues.put(v, iv);
                }

            }

        }
        
        for(final Runnable r: deferredRunnables) {
            r.run();
        }

    }
    
}
