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
 * Created on Aug 23, 2011
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.GroupMemberNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.hints.BasicBooleanQueryHint;
import com.bigdata.rdf.sparql.ast.hints.BasicIntQueryHint;
import com.bigdata.rdf.sparql.ast.hints.BasicStringQueryHint;
import com.bigdata.rdf.sparql.ast.hints.QueryHintRegistry;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.BD;

/**
 * This ALP SERVICE {@link IASTOptimizer} provides a rewrite of a SERVICE
 * expression that makes it possible to specify limits (minimum, maximum path
 * length), directionality of traversal, etc.
 * <p>
 * A sample query is:
 * 
 * <pre>
 * #e.g. "Go three hops out in either direction from vertex <id:v0> where the edge is of type <test:foo> and the edge has <some:prop>=someVal."
 * 
 * SELECT * WHERE {
 * SERVICE bd:alp { " +
 * <id:v0> ?edge ?to . " +
 * hint:Prior hint:alp.pathExpr true .
 * ?edge rdf:type <test:foo> .
 * ?edge <some:prop> "someVal" .
 * hint:Group hint:alp.lowerBound 1 .
 * hint:Group hint:alp.upperBound 3 .
 * hint:Group hint:alp.bidirectional true .
 * }
 * }
 * </pre>
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/1072"> Configurable ALP Service </a>
 * @see <a href="http://trac.blazegraph.com/ticket/1117"> Document the ALP Service </a>
 * 
 * @author bryan
 */
public class ASTALPServiceOptimizer extends AbstractJoinGroupOptimizer
        implements IASTOptimizer {

    private static final transient Logger log = Logger.getLogger(ASTALPServiceOptimizer.class);

    /**
     * The well-known URI of the ALP SERVICE extension {@value #ALP}.
     */
    public static final URI ALP = BD.ALP_SERVICE;

    /**
     * 
     */
    public static final String PATH_EXPR = "alp.pathExpr";
    
    public static final String LOWER_BOUND = "alp.lowerBound";
    
    public static final String UPPER_BOUND = "alp.upperBound";
    
    public static final String BIDIRECTIONAL = "alp.bidirectional";
    
    public static final String EDGE_VAR = "alp.edgeVar";
    
    static {
        QueryHintRegistry.add(new BasicBooleanQueryHint(PATH_EXPR, false));
        QueryHintRegistry.add(new BasicIntQueryHint(LOWER_BOUND, 1));
        QueryHintRegistry.add(new BasicIntQueryHint(UPPER_BOUND, Integer.MAX_VALUE));
        QueryHintRegistry.add(new BasicBooleanQueryHint(BIDIRECTIONAL, false));
        QueryHintRegistry.add(new BasicStringQueryHint(EDGE_VAR, null));
    }
     
    /**
     * Optimize the join group.
     */
    protected void optimizeJoinGroup(final AST2BOpContext ctx,
            final StaticAnalysis sa, final IBindingSet[] bSets,
            final JoinGroupNode group) {

        final GlobalAnnotations globals = new GlobalAnnotations(
                ctx.getLexiconNamespace(),
                ctx.getTimestamp()
                );

        for (ServiceNode node : group.getChildren(ServiceNode.class)) {

            if (log.isDebugEnabled()) {
                log.debug(node);
            }
            
            final TermNode serviceRef = node.getServiceRef();
            if (!serviceRef.isConstant() || !serviceRef.getValue().equals(ALP)) {
                /*
                 * Not our service.
                 */
                continue;
            }

            final JoinGroupNode subgroup = (JoinGroupNode) node.getGraphPattern();
            
            if (log.isDebugEnabled()) {
                log.debug("found an alp service to optimize:\n"+subgroup);
            }
            
            final Properties hints = subgroup.getQueryHints();
            if (!hints.containsKey(LOWER_BOUND)) {
                throw new RuntimeException("missing: " + LOWER_BOUND);
            }
            if (!hints.containsKey(UPPER_BOUND)) {
                throw new RuntimeException("missing: " + UPPER_BOUND);
            }
            
            for (IGroupMemberNode child : subgroup.getChildren()) {
                if (!(child instanceof StatementPatternNode ||
                        child instanceof FilterNode)) {
                    throw new RuntimeException("Complex groups not allowed in alp service");
                }
            }

            final Set<VarNode> dropVars = new LinkedHashSet<>();
            
            final int lowerBound = Integer.valueOf(subgroup.getQueryHint(LOWER_BOUND));
            final int upperBound = Integer.valueOf(subgroup.getQueryHint(UPPER_BOUND));
            final VarNode tVarLeft = new VarNode("-tVarLeft-"+UUID.randomUUID().toString());
            tVarLeft.setAnonymous(true);
            final VarNode tVarRight = new VarNode("-tVarRight-"+UUID.randomUUID().toString());
            tVarRight.setAnonymous(true);
            
            dropVars.add(tVarLeft);
            dropVars.add(tVarRight);

            final boolean bidirectional = 
                    subgroup.getQueryHintAsBoolean(BIDIRECTIONAL, false);
            
            final String evHint = subgroup.getQueryHint(EDGE_VAR);
            VarNode edgeVar = null;
            if (evHint != null) {
                if (!(evHint.length() > 1 && evHint.charAt(0) == '?')) {
                    throw new IllegalArgumentException("Illegal hint for "+EDGE_VAR+": " + evHint);
                }
                edgeVar = new VarNode(evHint.substring(1));
            }
            
            TermNode left = null;
            TermNode right = null;
            IGroupMemberNode pathExpr = null;
            JoinGroupNode group1 = null;
            JoinGroupNode group2 = null;
            TermNode middle = null;
            
            for (StatementPatternNode child : subgroup.getStatementPatterns()) {
                if (child.getQueryHintAsBoolean(PATH_EXPR, false)) {
                    if (pathExpr != null) {
                        throw new RuntimeException("Only one " + PATH_EXPR + " allowed");
                    }
                    left = child.s();
                    right = child.o();
                    middle = child.p();
                    
                    if (child.p() instanceof VarNode) {
                        final VarNode v = (VarNode) child.p();
                        v.setAnonymous(true);
                        dropVars.add(v);
                    }
                    
                    if (bidirectional) {
                        final StatementPatternNode forward = 
                                new StatementPatternNode(
                                tVarLeft,
                                child.p(), 
                                tVarRight, 
                                child.c(), 
                                child.getScope()
                                );
                        forward.setQueryHint(PATH_EXPR, "true");
                        group1 = new JoinGroupNode();
                        group1.addChild(forward);
                        
                        final StatementPatternNode reverse = 
                                new StatementPatternNode(
                                tVarRight,
                                child.p(), 
                                tVarLeft, 
                                child.c(), 
                                child.getScope()
                                );
                        reverse.setQueryHint(PATH_EXPR, "true");
                        group2 = new JoinGroupNode();
                        group2.addChild(reverse);
                        
                        final UnionNode union = new UnionNode();
                        union.addArg(group1);
                        union.addArg(group2);
                        
                        pathExpr = union;
                    } else {
                        final StatementPatternNode sp = new StatementPatternNode(
                                tVarLeft,
                                child.p(), 
                                tVarRight, 
                                child.c(), 
                                child.getScope()
                                );
                        sp.setQueryHint(PATH_EXPR, "true");
                        
                        pathExpr = sp;
                    }
                    subgroup.removeChild(child);
                }
            }
            
            IVariable<?> leftVar = null;
            if (left instanceof VarNode) {
                leftVar = ((VarNode) left).getValueExpression();
            }
            
            IVariable<?> rightVar = null;
            if (right instanceof VarNode) {
                rightVar = ((VarNode) right).getValueExpression();
            }
            
            /*
             * Remap any filters on the left/right vars onto the transitive
             * left/right vars instead.
             */
            for (FilterNode f : subgroup.getChildren(FilterNode.class)) {
                
                final Iterator<BOp> it = 
                        BOpUtility.preOrderIteratorWithAnnotations(f);

                boolean swap = false;
                while (it.hasNext()) {
                    final BOp bop = it.next();
                    if (!(bop instanceof VarNode)) {
                        continue;
                    }
                    
                    final VarNode v = (VarNode) bop;
                    final IVariable<?> ve = v.getValueExpression();
                    if (leftVar != null && leftVar.equals(ve)) {
                        v.setValueExpression(tVarLeft.getValueExpression());
                        swap = true;
                    } else if (rightVar != null && rightVar.equals(ve)) {
                        v.setValueExpression(tVarRight.getValueExpression());
                        swap = true;
                    }                    
                }

                /*
                 * If we've swapped any vars, also re-generate the underlying 
                 * value expression on the filter node.
                 */
                if (swap) {
                    final IValueExpressionNode veNode = f.getValueExpressionNode(); 
                    veNode.setValueExpression(null);
                    AST2BOpUtility.toVE(ctx.context, globals, veNode);
                }
                
            }
            
            final ArbitraryLengthPathNode alpNode = new ArbitraryLengthPathNode(
                    left, right,
                    tVarLeft, tVarRight,
                    lowerBound, upperBound
                    );
            
            if (edgeVar != null) {
                alpNode.setEdgeVar(edgeVar, middle);
            }
            
            alpNode.subgroup().addChild(pathExpr);
            for (@SuppressWarnings("rawtypes") GroupMemberNodeBase child : 
                    subgroup.getChildren(GroupMemberNodeBase.class)) {
                if (!child.getQueryHintAsBoolean(PATH_EXPR, false)) {
                    /*
                     * Make all variables in the ALP service (other than the
                     * left and right) anonymous, since they cannot be projected
                     * out in a meaningful fashion.
                     */
                    final Iterator<BOp> it = 
                            BOpUtility.preOrderIteratorWithAnnotations(child);
                    while (it.hasNext()) {
                        final BOp bop = it.next();
                        if (bop instanceof VarNode) {
                            final VarNode v = (VarNode) bop;
                            v.setAnonymous(true);
                            dropVars.add(v);
                        }
                    }
                    child.setQueryHints(new Properties());
                    subgroup.removeChild(child);
                    if (bidirectional) {
                        group1.addChild((IGroupMemberNode) child.clone());
                        group2.addChild((IGroupMemberNode) child.clone());
                    } else {
                        alpNode.subgroup().addChild(child);
                    }
                }
            }
            
            alpNode.setDropVars(dropVars);
            
            if (log.isDebugEnabled()) {
                log.debug("optimized alpNode:\n"+alpNode);
            }
            
            group.removeChild(node);
            group.addChild(alpNode);
            // optimize(ctx, sa, group, node);

        }

    }

}
