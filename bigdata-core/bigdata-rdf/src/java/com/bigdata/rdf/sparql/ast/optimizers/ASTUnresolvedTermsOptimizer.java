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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.constraints.IVValueExpression;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
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
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
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
                ProjectionNode projection = queryRoot.getProjection();
                if (projection!=null) {
                    fillInIV(context, projection);
                }
            }
    
            // Main CONSTRUCT clause
            {
    
                GroupNodeBase constructClause = queryRoot.getConstruct();
    
                if (constructClause != null) {
    
                    resolveGroupsWithUnknownTerms(context, constructClause);
                    
                }
    
            }
    
            // Main WHERE clause
            {
    
                final GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>) queryRoot
                        .getWhereClause();
    
                if (whereClause != null) {
    
                    resolveGroupsWithUnknownTerms(context, whereClause);
                    
                }
    
            }
            
            // HAVING clause
            {
                HavingNode having = queryRoot.getHaving();
                if (having!=null) {
                    for (IConstraint c: having.getConstraints()) {
                        handleHaving(context, c);
                    }
                }
            }
    
            
            // BINDINGS clause
            {
                BindingsClause bc = queryRoot.getBindingsClause();
                if (bc!=null) {
                    for (IBindingSet bs: bc.getBindingSets()) {
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
    
                    final GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>) namedSubquery
                            .getWhereClause();
    
                    if (whereClause != null) {
    
                        resolveGroupsWithUnknownTerms(context, whereClause);
                        
                    }
    
                }
    
            }
    
        }
        return new QueryNodeWithBindingSet(queryNode, bindingSets);
    }

    /**
     * If the group has an unknown term, resolve it.
     * @param context 
     * 
     * @param op
     */
    private static void resolveGroupsWithUnknownTerms(AST2BOpContext context, final QueryNodeBase op) {

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

                List<IBindingSet> childs = bindings
                        .getBindingSets();
                
                for (IBindingSet s: childs) {
                    handleBindingSet(context, s);
                }

            } else if(child instanceof FilterNode) {
                FilterNode node = (FilterNode)child;
                fillInIV(context,node.getValueExpression());
            } else if(child instanceof ServiceNode) {
                ServiceNode node = (ServiceNode) child;
                TermNode serviceRef = node.getServiceRef();
                IV newValue = resolveConstant(context, serviceRef.getValue());
                if (newValue!=null) {
                    node.setServiceRef(new ConstantNode(new Constant(newValue)));
                }
            } else if(child instanceof FunctionNode) {
                fillInIV(context, child);
            } else if (child instanceof BindingsClause) {
                List<IBindingSet> bsList = ((BindingsClause)child).getBindingSets();
                if (bsList!=null) {
                    for (IBindingSet bs: bsList) {
                        handleBindingSet(context, bs);
                    }
                }
                
            }

        }

    }

    private static void handleHaving(AST2BOpContext context, IConstraint s) {
        Iterator<BOp> itr = s.argIterator();
        while (itr.hasNext()) {
            BOp bop = itr.next();
            fillInIV(context, bop);
        }
    }

    private static void handleBindingSet(AST2BOpContext context, IBindingSet s) {
        Iterator<Entry<IVariable, IConstant>> itr = s.iterator();
        while (itr.hasNext()) {
            Entry<IVariable, IConstant> entry = itr.next();
            Object value = entry.getValue().get();
            if (value instanceof BigdataValue) {
                IV newValue = resolveConstant(context, (BigdataValue)value);
                if (newValue!=null) {
                    entry.setValue(new Constant(newValue));
                }
            } else if (value instanceof TermId) {
                IV newValue = resolveConstant(context, ((TermId)value).getValue());
                if (newValue!=null) {
                    entry.setValue(new Constant(newValue));
                }
            }
        }
    }

    private static void fillInIV(AST2BOpContext context, final BOp bop) {
        if (bop instanceof ConstantNode) {
            
            final BigdataValue value = ((ConstantNode) bop).getValue();
            if (value==null) {
                System.err.println("?");
            } else {
                // even if iv is already filled in we should try to resolve it against triplestore,
                // as previously resolved IV may be inlined, but triplestore expects term from lexicon relation
                IV newValue = resolveConstant(context, value);
                if (newValue!=null) {
                    ((ConstantNode) bop).setArg(0, new Constant(newValue));
                }
            }
            return;
        }

        if (bop!=null) {
            for (int k = 0; k < bop.arity(); k++) {
                BOp pathBop = bop.get(k);
                if (pathBop instanceof Constant) {
                    Object v = ((Constant)pathBop).get();
                    if (v instanceof BigdataValue) {
                        IV newValue = resolveConstant(context, (BigdataValue)v);
                        if (newValue!=null) {
                            bop.args().set(k, new Constant(newValue));
                        }
                    } else if (v instanceof TermId) {
                        IV newValue = resolveConstant(context, ((TermId)v).getValue());
                        if (newValue!=null) {
                            if (bop.args() instanceof ArrayList) {
                                bop.args().set(k, new Constant(newValue));
                            } else {
                                System.err.println("!");
                            }
                        }
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
            for(IV iv: dataset.getDefaultGraphs().getGraphs()) {
                final IV newValue = resolveConstant(context, iv.getValue());
                if (newValue!=null) {
                    newDefaultGraphs.add(newValue);
                } else {
                    newDefaultGraphs.add(iv);
                }
            }
            dataset.setDefaultGraphs(new DataSetSummary(newDefaultGraphs, true));
            
            final Set<IV> newNamedGraphs = new HashSet<IV>();
            final Iterator<IV> namedGraphs = dataset.getNamedGraphs().getGraphs().iterator();
            while(namedGraphs.hasNext()) {
                final IV iv = namedGraphs.next();
                final IV newValue = resolveConstant(context, iv.getValue());
                if (newValue!=null) {
                    newNamedGraphs.add(newValue);
                } else {
                    newNamedGraphs.add(iv);
                }
            }
            dataset.setNamedGraphs(new DataSetSummary(newNamedGraphs, true));
        } else if (bop instanceof PathNode) {
            PathAlternative path = ((PathNode) bop).getPathAlternative();
            for (int k = 0; k < path.arity(); k++) {
                BOp pathBop = path.get(k);
                fillInIV(context,pathBop);
            }
        } else if (bop instanceof FunctionNode) {
            if (bop instanceof SubqueryFunctionNodeBase) {
                resolveGroupsWithUnknownTerms(context, ((SubqueryFunctionNodeBase)bop).getGraphPattern());
            }
            IValueExpression<? extends IV> ve = ((FunctionNode)bop).getValueExpression();
            if (ve instanceof IVValueExpression) {
                for (int k = 0; k < ve.arity(); k++) {
                    BOp pathBop = ve.get(k);
                    if (pathBop instanceof Constant && ((Constant)pathBop).get() instanceof TermId) {
                        BigdataValue v = ((TermId) ((Constant)pathBop).get()).getValue();
                        BigdataValue resolved = context.getAbstractTripleStore().getValueFactory().asValue(v);
                        IV resolvedIV;
                        if (resolved.getIV()==null) {
                            resolvedIV = context.getAbstractTripleStore().getIV(resolved);
                            if (resolvedIV!=null) {
                                resolved.setIV(resolvedIV);
                                resolvedIV.setValue(resolved);
                                Constant newConstant = new Constant(resolvedIV);
                                ve = (IValueExpression<? extends IV>) ((IVValueExpression)ve).setArg(k, newConstant);
                                ((FunctionNode) bop).setArg(k, new ConstantNode(newConstant));
                            }
                        } else {
                            fillInIV(context,pathBop); 
                        }
                    }
                }
                ((FunctionNode)bop).setValueExpression(ve);
            } else if (ve instanceof Constant) {
                Object value = ((Constant)ve).get();
                if (value instanceof BigdataValue) {
                    IV newValue = resolveConstant(context, (BigdataValue)value);
                    if (newValue!=null) {
                        ((FunctionNode)bop).setValueExpression(new Constant(newValue));
                    }
                }
            }
        } else if (bop instanceof ValueExpressionNode) {
            IValueExpression<? extends IV> ve = ((ValueExpressionNode)bop).getValueExpression();
            for (int k = 0; k < ve.arity(); k++) {
                BOp pathBop = ve.get(k);
                fillInIV(context,pathBop);
            }
        } else if (bop instanceof GroupNodeBase) {
            resolveGroupsWithUnknownTerms(context, (GroupNodeBase) bop);
        } else if (bop instanceof QueryNodeBase) {
            resolveGroupsWithUnknownTerms(context, ((QueryNodeBase)bop));
        }
    }

    private static IV resolveConstant(AST2BOpContext context, final BigdataValue value) {
        IV resolvedIV = null;
        if (value instanceof Literal) {
            String label = ((Literal)value).getLabel();
            URI dataType = ((Literal)value).getDatatype();
            String language = ((Literal)value).getLanguage();
            BigdataValue resolved;
            if (language!=null) {
                resolved = context.getAbstractTripleStore().getValueFactory().createLiteral(label, language);
            } else {
                resolved = context.getAbstractTripleStore().getValueFactory().createLiteral(label, dataType);
            }
            if (resolved.getIV()==null) {;
                resolvedIV = context.getAbstractTripleStore().getIV(resolved);
                if (resolvedIV==null) {
                    DTE dte = DTE.valueOf(dataType);
                    if (dte!=null) {
                        resolvedIV = IVUtility.decode(label, dte.name());
                    } else {
                        resolvedIV = TermId.mockIV(VTE.valueOf(value));
                    }
                    resolvedIV.setValue(resolved);
                    resolved.setIV(resolvedIV);
                }
            } else {
                resolvedIV = resolved.getIV();
            }
            resolvedIV.setValue(resolved);
            resolved.setIV(resolvedIV);
        } else {
        
            BigdataValue resolved = context.getAbstractTripleStore().getValueFactory().asValue(value);
            if (resolved!=null && resolved.getIV()==null) {
                resolvedIV = context.getAbstractTripleStore().getIV(resolved);
                if (resolvedIV!=null) {
                    resolved.setIV(resolvedIV);
                    resolvedIV.setValue(resolved);
                }
            }
        }
        return resolvedIV;
    }

}
