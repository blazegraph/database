package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.hints.BasicBooleanQueryHint;
import com.bigdata.rdf.sparql.ast.hints.BasicIntQueryHint;
import com.bigdata.rdf.sparql.ast.hints.QueryHintRegistry;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.BD;

public class ASTALPServiceOptimizer extends AbstractJoinGroupOptimizer
        implements IASTOptimizer {

    private static final transient Logger log = Logger.getLogger(ASTALPServiceOptimizer.class);

    public static final URI ALP = new URIImpl(BD.NAMESPACE + "alp");
    
    public static final String PATH_EXPR = "alp.pathExpr";
    
    public static final String LOWER_BOUND = "alp.lowerBound";
    
    public static final String UPPER_BOUND = "alp.upperBound";
    
    public static final String BIDIRECTIONAL = "alp.bidirectional";
    
    static {
        QueryHintRegistry.add(new BasicBooleanQueryHint(PATH_EXPR, false));
        QueryHintRegistry.add(new BasicIntQueryHint(LOWER_BOUND, 1));
        QueryHintRegistry.add(new BasicIntQueryHint(UPPER_BOUND, Integer.MAX_VALUE));
        QueryHintRegistry.add(new BasicBooleanQueryHint(BIDIRECTIONAL, false));
    }
     
    /**
     * Optimize the join group.
     */
    protected void optimizeJoinGroup(final AST2BOpContext ctx,
            final StaticAnalysis sa, final IBindingSet[] bSets,
            final JoinGroupNode group) {

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
                if (!(child instanceof StatementPatternNode)) {
                    throw new RuntimeException("Complex groups not allowed in alp service");
                }
            }
            
            final int lowerBound = Integer.valueOf(subgroup.getQueryHint(LOWER_BOUND));
            final int upperBound = Integer.valueOf(subgroup.getQueryHint(UPPER_BOUND));
            final VarNode tVarLeft = new VarNode("-tVarLeft-"+UUID.randomUUID().toString());
            tVarLeft.setAnonymous(true);
            final VarNode tVarRight = new VarNode("-tVarRight-"+UUID.randomUUID().toString());
            tVarRight.setAnonymous(true);

            final boolean bidirectional = 
                    subgroup.getQueryHintAsBoolean(BIDIRECTIONAL, false);
            
            TermNode left = null;
            TermNode right = null;
            IGroupMemberNode pathExpr = null;
            for (StatementPatternNode child : subgroup.getStatementPatterns()) {
                if (child.getQueryHintAsBoolean(PATH_EXPR, false)) {
                    if (pathExpr != null) {
                        throw new RuntimeException("Only one " + PATH_EXPR + " allowed");
                    }
                    left = child.s();
                    right = child.o();
                    if (bidirectional) {
                        final StatementPatternNode forward = 
                                new StatementPatternNode(
                                tVarLeft,
                                child.p(), 
                                tVarRight, 
                                child.c(), 
                                child.getScope()
                                );
                        final JoinGroupNode group1 = new JoinGroupNode();
                        group1.addChild(forward);
                        
                        final StatementPatternNode reverse = 
                                new StatementPatternNode(
                                tVarRight,
                                child.p(), 
                                tVarLeft, 
                                child.c(), 
                                child.getScope()
                                );
                        final JoinGroupNode group2 = new JoinGroupNode();
                        group2.addChild(reverse);
                        
                        final UnionNode union = new UnionNode();
                        union.addArg(group1);
                        union.addArg(group2);
                        union.setQueryHint(PATH_EXPR, "true");
                        
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
            
            final ArbitraryLengthPathNode alpNode = new ArbitraryLengthPathNode(
                    left, right,
                    tVarLeft, tVarRight,
                    lowerBound, upperBound
                    );
            
            alpNode.subgroup().addChild(pathExpr);
            for (StatementPatternNode child : subgroup.getStatementPatterns()) {
                if (!child.getQueryHintAsBoolean(PATH_EXPR, false)) {
                    child.setQueryHints(new Properties());
                    subgroup.removeChild(child);
                    alpNode.subgroup().addChild(child);
                }
            }
            
            group.removeChild(node);
            group.addChild(alpNode);
            // optimize(ctx, sa, group, node);

        }

    }

}