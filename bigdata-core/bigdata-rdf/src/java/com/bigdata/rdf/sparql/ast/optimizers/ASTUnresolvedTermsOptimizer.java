/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Sep 10, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;

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
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.CreateGraph;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.DeleteInsertGraph;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.HavingNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.PathNode;
import com.bigdata.rdf.sparql.ast.PathNode.PathAlternative;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QuadsDataOrNamedSolutionSet;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryNodeBase;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.SubqueryFunctionNodeBase;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.UpdateRoot;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.DataSetSummary;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

@SuppressWarnings({"unchecked","rawtypes"})
public class ASTUnresolvedTermsOptimizer implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTUnresolvedTermsOptimizer.class);

    private interface Handler {
        void handle(IV newIV);
    }
    
    private final Map<BigdataValue, List<Handler>> deferred = new HashMap<>();
    private final List<Runnable> deferredRunnables = new ArrayList<>();
    
    private void defer(final BigdataValue value, final Handler handler) {
        if (value!=null) {
            List<Handler> handlers = deferred.get(value);
            if (handlers==null) {
                handlers = new ArrayList<>();
                deferred.put(value, handlers);
            }
            handlers.add(handler);
        }
    }

    private void deferRunnable(final Runnable runnable) {
        deferredRunnables.add(runnable);
    }

    @Override
    public QueryNodeWithBindingSet optimize(
        final AST2BOpContext context, final QueryNodeWithBindingSet input) {

        final IQueryNode queryNode = input.getQueryNode();
        final IBindingSet[] bindingSets = input.getBindingSets();     

        if (queryNode instanceof QueryRoot) {
            fillInIV(context, ((QueryRoot)queryNode).getDataset());
            
        }

        if (queryNode instanceof UpdateRoot) {
            final UpdateRoot updateRoot = (UpdateRoot) queryNode;
            resolveGroupsWithUnknownTerms(context, updateRoot);
            
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
    
                    resolveGroupsWithUnknownTerms(context, constructClause);
                    
                }
    
            }
    
            // Main WHERE clause
            {
    
                final GroupNodeBase<IGroupMemberNode> whereClause = queryRoot
                        .getWhereClause();
    
                if (whereClause != null) {
    
                    resolveGroupsWithUnknownTerms(context, whereClause);
                    
                }
    
            }
            
            // HAVING clause
            {
                final HavingNode having = queryRoot.getHaving();
                if (having!=null) {
                    for (final IConstraint c: having.getConstraints()) {
                        handleHaving(context, c);
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
    
                        resolveGroupsWithUnknownTerms(context, whereClause);
                        
                    }
    
                }
    
            }
    
        }
        
        resolveIVs(context);
        
        return new QueryNodeWithBindingSet(queryNode, bindingSets);
    }

    /**
     * If the group has an unknown term, resolve it.
     * @param context 
     * 
     * @param op
     */
    private void resolveGroupsWithUnknownTerms(final AST2BOpContext context, final QueryNodeBase op) {

        if (op==null) return;
    	/*
    	 * Check the statement patterns.
    	 */
    	for (int i = 0; i < op.arity(); i++) {
    		
    		final BOp sp = op.get(i);

    		if (sp!=null) {
                
        		fillInIV(context, sp);
        		
        		for (int j = 0; j < sp.arity(); j++) {
        			
        			final BOp bop = sp.get(j);
        			
       				fillInIV(context, bop);
        			
        		}
    		}
    		
    	}
    	
        /*
         * Recursion, but only into group nodes (including within subqueries).
         */
        for (int i = 0; i < op.arity(); i++) {

            final BOp child = op.get(i);
            
            if (child!=null)

            if (child instanceof GroupNodeBase<?>) {

                final GroupNodeBase<IGroupMemberNode> childGroup = (GroupNodeBase<IGroupMemberNode>) child;

                resolveGroupsWithUnknownTerms(context, childGroup);

            } else if (child instanceof QueryBase) {

                final QueryBase subquery = (QueryBase) child;

                new ASTUnresolvedTermsOptimizer().optimize(context, new QueryNodeWithBindingSet(subquery, null));

            } else if (child instanceof BindingsClause) {

                final BindingsClause bindings = (BindingsClause) child;

                final List<IBindingSet> childs = bindings
                        .getBindingSets();
                
                for (final IBindingSet s: childs) {
                    handleBindingSet(context, s);
                }

            } else if(child instanceof FilterNode) {
                final FilterNode node = (FilterNode)child;
                fillInIV(context,node.getValueExpression());
            } else if(child instanceof ServiceNode) {
                final ServiceNode node = (ServiceNode) child;
                final TermNode serviceRef = node.getServiceRef();
                defer(serviceRef.getValue(), new Handler(){
                    @Override
                    public void handle(final IV newIV) {
                        node.setServiceRef(new ConstantNode(new Constant(newIV)));
                    }
                });
            } else if(child instanceof FunctionNode) {
                fillInIV(context, child);
            } else if (child instanceof BindingsClause) {
                final List<IBindingSet> bsList = ((BindingsClause)child).getBindingSets();
                if (bsList!=null) {
                    for (final IBindingSet bs: bsList) {
                        handleBindingSet(context, bs);
                    }
                }
                
            }

        }

    }

    private void handleHaving(final AST2BOpContext context, final IConstraint s) {
        final Iterator<BOp> itr = s.argIterator();
        while (itr.hasNext()) {
            final BOp bop = itr.next();
            fillInIV(context, bop);
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

    private void fillInIV(final AST2BOpContext context, final BOp bop) {
        if (bop instanceof ConstantNode) {
            
            final BigdataValue value = ((ConstantNode) bop).getValue();
            if (value!=null) {
                // even if iv is already filled in we should try to resolve it against triplestore,
                // as previously resolved IV may be inlined, but triplestore expects term from lexicon relation
                defer(value, new Handler(){
                    @Override
                    public void handle(final IV newIV) {
                        ((ConstantNode) bop).setArg(0, new Constant(newIV));
                    }
                });
            }
            return;
        }

        if (bop!=null) {
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
        }

        if (bop instanceof CreateGraph) {
            fillInIV(context,((CreateGraph)bop).getTargetGraph());
        } if (bop instanceof DeleteInsertGraph) {
            fillInIV(context, ((DeleteInsertGraph)bop).getDataset());
            fillInIV(context, ((DeleteInsertGraph)bop).getDeleteClause());
            fillInIV(context, ((DeleteInsertGraph)bop).getInsertClause());
            fillInIV(context, ((DeleteInsertGraph)bop).getWhereClause());

        } else if (bop instanceof QuadsDataOrNamedSolutionSet) {
            resolveGroupsWithUnknownTerms(context, ((QuadsDataOrNamedSolutionSet)bop).getQuadData());
        } else if (bop instanceof DatasetNode) {
            final DatasetNode dataset = ((DatasetNode) bop);
            final Set<IV> newDefaultGraphs = new HashSet<IV>();
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
            
            final Set<IV> newNamedGraphs = new HashSet<IV>();
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
                resolveGroupsWithUnknownTerms(context, ((SubqueryFunctionNodeBase)bop).getGraphPattern());
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
            resolveGroupsWithUnknownTerms(context, (GroupNodeBase) bop);
        } else if (bop instanceof QueryNodeBase) {
            resolveGroupsWithUnknownTerms(context, ((QueryNodeBase)bop));
        }
    }

    private void resolveIVs(final AST2BOpContext context) {
        
        final IV[] ivs = new IV[deferred.size()];
        final BigdataValue[] values = new BigdataValue[deferred.size()];
        {
        
            /*
             * First, build up the set of unknown terms.
             */
            int i = 0;
            
            for (final BigdataValue v: deferred.keySet()) {

                final BigdataValue toBeResolved = v.getValueFactory().asValue(v);
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
                        BigdataValue resolved;
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
    
                    for(final Handler handler: deferred.get(v)) {
                        handler.handle(iv);
                    }
                }

            }

        }
        
        for(final Runnable r: deferredRunnables) {
            r.run();
        }

    }

}
