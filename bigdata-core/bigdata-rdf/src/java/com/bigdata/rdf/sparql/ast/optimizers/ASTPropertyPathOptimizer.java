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
 * Created on Sep 10, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.PathNode.PathAlternative;
import com.bigdata.rdf.sparql.ast.PathNode.PathElt;
import com.bigdata.rdf.sparql.ast.PathNode.PathMod;
import com.bigdata.rdf.sparql.ast.PathNode.PathNegatedPropertySet;
import com.bigdata.rdf.sparql.ast.PathNode.PathOneInPropertySet;
import com.bigdata.rdf.sparql.ast.PathNode.PathSequence;
import com.bigdata.rdf.sparql.ast.PropertyPathNode;
import com.bigdata.rdf.sparql.ast.PropertyPathUnionNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.ZeroLengthPathNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

public class ASTPropertyPathOptimizer extends AbstractJoinGroupOptimizer
		implements IASTOptimizer {

//	private static final transient Logger log = Logger.getLogger(ASTPropertyPathOptimizer.class);
	
    /**
     * Optimize the join group.
     */
	protected void optimizeJoinGroup(final AST2BOpContext ctx, 
    		final StaticAnalysis sa, final IBindingSet[] bSets, final JoinGroupNode group) {

    	for (PropertyPathNode node : group.getChildren(PropertyPathNode.class)) {
    		
    		optimize(ctx, sa, group, node, null);
    		
    	}
    	
    }
    
    /**
     * Optimize a single PropertyPathNode.
     */
	protected void optimize(final AST2BOpContext ctx, final StaticAnalysis sa, 
			final JoinGroupNode group, final PropertyPathNode ppNode,
			final ArbitraryLengthPathNode alpNode) {

    	final PathAlternative pathRoot = ppNode.p().getPathAlternative();
    	
    	final PropertyPathInfo sp = new PropertyPathInfo(ppNode.s(), ppNode.o(), ppNode.c(), ppNode.getScope());

    	optimize(ctx, sa, group, sp, pathRoot, alpNode, ppNode.getQueryHints(), ppNode);
    	
    	/*
    	 * We always remove the PropertyPathNode.  It has been replaced with
    	 * other executable nodes (joins, unions, paths, etc.)
    	 */
    	group.removeChild(ppNode);
    	
    }
    
    /**
     * Optimize a PathAlternative using UNIONs.
     */
	protected void optimize(final AST2BOpContext ctx, final StaticAnalysis sa, 
			final GraphPatternGroup<? extends IGroupMemberNode> group, final PropertyPathInfo ppInfo,
			final PathAlternative pathAlt, final ArbitraryLengthPathNode alpNode,
			final Properties queryHints, final PropertyPathNode ppNode) {

    	if (pathAlt.arity() == 1) {
    		
    		final PathSequence pathSeq = (PathSequence) pathAlt.get(0);
    		
    		optimize(ctx, sa, group, ppInfo, pathSeq, alpNode, queryHints, ppNode);
    		
    	} else {
    		
    		final UnionNode union = new PropertyPathUnionNode();
    		
    		group.addArg(getPositionOfNodeInGroup(ppNode, group), union);
    		
    		final Iterator<BOp> it = pathAlt.argIterator();
    		
    		while (it.hasNext()) {
    			
    			final JoinGroupNode subgroup = new JoinGroupNode();
    			
    			union.addArg(subgroup);
    			
    			final PathSequence pathSeq = (PathSequence) it.next();
    			
    			optimize(ctx, sa, subgroup, ppInfo, pathSeq, alpNode, queryHints, ppNode);
    			
    		}
    		
    	}
    	
    }
    
    /**
     * Optimize a PathSequence.
     */
	protected void optimize(final AST2BOpContext ctx, final StaticAnalysis sa, 
			final GraphPatternGroup<? extends IGroupMemberNode> group, 
			final PropertyPathInfo ppInfo, final PathSequence pathSeq,
			final ArbitraryLengthPathNode alpNode, final Properties queryHints,
			final PropertyPathNode ppNode) {

		if (pathSeq.arity() == 0) {
			
			return;
			
		}
		
    	if (pathSeq.arity() == 1) {
    		
    		final PathElt pathElt = (PathElt) pathSeq.get(0);
    		
    		/*
    		 * Below is a failed attempt to solve the zero length path problem
    		 * with a separate kind of node and operator.  This leads to
    		 * cardinality problems.
    		 */
    		
//        	final PathMod mod = pathElt.getMod();
//        	
//    		/*
//    		 * Pretty sure a singleton path sequence with a '?' or '*' modifier
//    		 * is the exact same as an optional in the special case where
//    		 * the sequence has only one element.
//    		 */
//    		if (mod == PathMod.ZERO_OR_ONE || mod == PathMod.ZERO_OR_MORE) {
//    			
//    			final PathElt _pathElt = new PathElt(pathElt);
//    			
//    			if (mod == PathMod.ZERO_OR_ONE) {
//    				
//    				_pathElt.setMod(null); // exactly one
//    				
//    			} else { // mod == PathMod.ZERO_OR_MORE
//    				
//    				_pathElt.setMod(PathMod.ONE_OR_MORE);
//    				
//    			}
//    			
//    			final PathAlternative pathAlt = new PathAlternative(
//    					new PathSequence(new PathElt(new ZeroLengthPathNode())),
//    					new PathSequence(_pathElt));
//    			
//    			optimize(ctx, sa, group, ppInfo, pathAlt);
//    			
//    		} else {

    			optimize(ctx, sa, group, ppInfo, pathElt, alpNode, queryHints, ppNode);
    			
//    		}
    		
    	} else {

            for (int i = 0; i < pathSeq.arity(); i++) {

            	final PathElt pathElt = (PathElt) pathSeq.get(i);
            	
            	final PathMod mod = pathElt.getMod();
            	
            	if (i < (pathSeq.arity()-1) && 
            			(mod == PathMod.ZERO_OR_ONE || mod == PathMod.ZERO_OR_MORE)) {
            		
            		/*
            		 * We need to create a new path sequence using an alt and
            		 * then run the optimizer on the new sequence instead of
            		 * this one.
            		 * 
            		 * This element is an optional element.  Create a new 
            		 * sequence that starts with the elements of the old
            		 * sequence until we get to the optional element, then
            		 * add an alternative (split) with the subsequent elements,
            		 * one that includes the optional element and one that
            		 * doesn't.  For example:
            		 * 
            		 * a/b?/c -> a/((b/c)|c)
            		 * 
            		 * This will work even if there are multiple optional
            		 * elements - we will just hit this recursively until
            		 * they are gone.
            		 * 
            		 * We cannot solve this with optionals or by using the
            		 * arbitrary length path operator because of the ridiculous
            		 * semantics of zero length paths.
            		 */
            		final ArrayList<PathElt> newSeq = new ArrayList<PathElt>(i+1);
            		final ArrayList<PathElt> with = new ArrayList<PathElt>(pathSeq.arity()-i);
            		final ArrayList<PathElt> without = new ArrayList<PathElt>(pathSeq.arity()-i-1);
            		
        			for (int j = 0; j < pathSeq.arity(); j++) {
        				
        				final PathElt elt = (PathElt) pathSeq.get(j);
        				
        				if (j < i) {
        					
        					// add the original, no need to clone
        					newSeq.add(elt);
        					
        				} else if (j == i) {

        	    			final PathElt _pathElt = new PathElt(pathElt);
        	    			
        	    			if (mod == PathMod.ZERO_OR_ONE) {
        	    				
        	    				_pathElt.setMod(null); // exactly one
        	    				
        	    			} else { // mod == PathMod.ZERO_OR_MORE
        	    				
        	    				_pathElt.setMod(PathMod.ONE_OR_MORE);
        	    				
        	    			}

        					with.add(_pathElt);
        					
        				} else {
        					
        					/*
        					 * After the splitting element we add a copy of
        					 * the subsequent elements to the two alternatives. 
        					 */
        					
        					with.add(new PathElt(elt));
        					
        					without.add(new PathElt(elt));
        					
        				}
        				
//        				if (i != j) {
//        					
//        					with.add(new PathElt(elt));
//        					
//        					without.add(new PathElt(elt));
//        					
//        				} else {
//        					
//        	    			final PathElt _pathElt = new PathElt(pathElt);
//        	    			
//        	    			if (mod == PathMod.ZERO_OR_ONE) {
//        	    				
//        	    				_pathElt.setMod(null); // exactly one
//        	    				
//        	    			} else { // mod == PathMod.ZERO_OR_MORE
//        	    				
//        	    				_pathElt.setMod(PathMod.ONE_OR_MORE);
//        	    				
//        	    			}
//
//        					with.add(_pathElt);
//        					
//        				}
        				
        			}
        			
        			newSeq.add(new PathElt(new PathAlternative(
        					new PathSequence((PathElt[]) with.toArray(new PathElt[with.size()])),
        					new PathSequence((PathElt[]) without.toArray(new PathElt[without.size()])))));
        			
        			final PathSequence pathSeq2 = 
        					new PathSequence(newSeq.toArray(new PathElt[newSeq.size()]));
        			
        			optimize(ctx, sa, group, ppInfo, pathSeq2, alpNode, queryHints, ppNode);
        			
        			return;
            		
            	}
            	
            }
            
    		TermNode last = ppInfo.s;
    		
            for (int i = 0; i < pathSeq.arity(); i++) {
            	
//            	TermNode next = (i == (pathSeq.arity()-1)) ? ppInfo.o : anonVar();
            	TermNode next;
            	if (i == (pathSeq.arity()-1)) {
            	    next = ppInfo.o;
            	} else {
            	    final VarNode anon = anonVar();
            	    if (alpNode != null) {
            	        alpNode.addDropVar(anon);
            	    }
            	    next = anon;
            	}
            	
            	final PropertyPathInfo _ppInfo = new PropertyPathInfo(last, next, ppInfo);
            	
        		final PathElt pathElt = (PathElt) pathSeq.get(i);
        		
    			optimize(ctx, sa, group, _ppInfo, pathElt, alpNode, queryHints, ppNode);
            	
            	last = next;
            	
            }
            
    	}
    	
    }
    
	private final String anon = "--pp-anon-";
    private VarNode anonVar() {
        VarNode v = new VarNode(anon+UUID.randomUUID().toString());
        v.setAnonymous(true);
        return v;
    }
    
    /**
     * Override during testing to give predictable results
     * @param anon
     * @return
     */
    protected VarNode anonVar(final String anon) {
        VarNode v = new VarNode(anon+UUID.randomUUID().toString());
        v.setAnonymous(true);
        return v;
    }
    
    int i = 1;
    
    /**
     * Optimize a PathElt.
     */
	protected void optimize(final AST2BOpContext ctx, final StaticAnalysis sa, 
			GraphPatternGroup<? extends IGroupMemberNode> group, 
			PropertyPathInfo ppInfo, final PathElt pathElt, 
			ArbitraryLengthPathNode alpNode, final Properties queryHints,
			final PropertyPathNode ppNode) {

    	ppInfo = pathElt.inverse() ? ppInfo.inverse() : ppInfo;
    	
		final PathMod mod = pathElt.getMod();
		
		/*
		 * Push expressions with an path length modifier down into a 
		 * ArbitraryLengthPathNode group.
		 */
		if (mod != null) {
			
//			final VarNode tVarLeft = new VarNode(Var.var("tVarLeft" + i));
			final VarNode tVarLeft = new VarNode(anonVar("-tVarLeft-"));
			
//			final VarNode tVarRight = new VarNode(Var.var("tVarRight" + i++));
			final VarNode tVarRight = new VarNode(anonVar("-tVarRight-"));
			
			alpNode = new ArbitraryLengthPathNode(ppInfo.s, ppInfo.o, tVarLeft, tVarRight, mod);
			
			// inherit gearing query hints to new ALP node
			final String userDefinedGearing = ppNode.getQueryHint(QueryHints.GEARING);
			if (userDefinedGearing != null) {
				alpNode.setQueryHint(QueryHints.GEARING, userDefinedGearing);
			}
			
         final String pipelinedHashJoinHint = queryHints==null ? 
             null : queryHints.getProperty(QueryHints.PIPELINED_HASH_JOIN);
         if (pipelinedHashJoinHint!=null) {
             alpNode.setQueryHint(
                 QueryHints.PIPELINED_HASH_JOIN, pipelinedHashJoinHint);
          }
			
			group.addArg(getPositionOfNodeInGroup(ppNode, group),alpNode);
			
			ppInfo = new PropertyPathInfo(tVarLeft, tVarRight, ppInfo);
			
			group = alpNode.subgroup();
			 
		}
		
		if (pathElt.isNestedPath()) {

			final PathAlternative pathAlt = (PathAlternative) pathElt.get(0);
			
			optimize(ctx, sa, group, ppInfo, pathAlt, alpNode, queryHints, ppNode);
			
		} else if (pathElt.isNegatedPropertySet()) {
			
			final PathNegatedPropertySet pathNPS = (PathNegatedPropertySet) pathElt.get(0);
			
			optimize(ctx, sa, group, ppInfo, pathNPS, alpNode, ppNode);
			
		} else if (pathElt.isZeroLengthPath()) {
			
			final ZeroLengthPathNode zlpNode = (ZeroLengthPathNode) pathElt.get(0);
			
			optimize(ctx, sa, group, ppInfo, zlpNode, ppNode);
			
		} else {

			final TermNode termNode = (ConstantNode) pathElt.get(0);
			
			optimize(ctx, sa, group, ppInfo, termNode, ppNode);
			
		}
    	
    }
    
	/**
	 * Optimize a TermNode (add a statement pattern to the group).
	 */
	protected void optimize(final AST2BOpContext ctx, final StaticAnalysis sa, 
			final GraphPatternGroup<? extends IGroupMemberNode> group, 
			final PropertyPathInfo ppInfo, final TermNode termNode,
			final PropertyPathNode ppNode) {
		
		final StatementPatternNode sp = ppInfo.toStatementPattern(termNode);

		group.addArg(getPositionOfNodeInGroup(ppNode, group), sp);
		
	}
	
	/**
	 * Optimize a ZeroLengthPathNode (add it to the group with the left and
	 * right properly set).
	 */
	protected void optimize(final AST2BOpContext ctx, final StaticAnalysis sa, 
			final GraphPatternGroup<? extends IGroupMemberNode> group, final PropertyPathInfo ppInfo,
			final ZeroLengthPathNode zlpNode, final PropertyPathNode ppNode) {
		
		zlpNode.setLeft(ppInfo.s);
		
		zlpNode.setRight(ppInfo.o);
		
		group.addArg(getPositionOfNodeInGroup(ppNode, group), zlpNode);
		
	}
	
    /**
     * Optimize a PathNegatedPropertySet.  This is done with a statement 
     * pattern and filter.  For example, the path:
     * 
     * ?s !(x|y|z) ?o .
     * 
     * Can be re-written into the simpler form:
     * 
     * ?s ?p ?o . filter(?p not in (x, y, z)) .
     * 
     * The more complicated case (where there are inverses involved) can be
     * run as a union of two of the above.
     */
	protected void optimize(final AST2BOpContext ctx, final StaticAnalysis sa, 
			final GraphPatternGroup<? extends IGroupMemberNode> group, final PropertyPathInfo ppInfo,
			final PathNegatedPropertySet pathNPS, final ArbitraryLengthPathNode alpNode,
			final PropertyPathNode ppNode) {

		ArrayList<ConstantNode> forward = null;
		ArrayList<ConstantNode> back = null;
		
		for (BOp child : pathNPS.args()) {
			if (!(child instanceof PathOneInPropertySet)) {
				continue;
			}
			final PathOneInPropertySet pathOIPS = (PathOneInPropertySet) child;
			
			final ConstantNode iri = (ConstantNode) pathOIPS.get(0);
			
			if (pathOIPS.inverse()) {
				
				if (back == null)
					back = new ArrayList<ConstantNode>();
				
				back.add(iri);
				
			} else {
				
				if (forward == null)
					forward = new ArrayList<ConstantNode>();
				
				forward.add(iri);
				
			}
			
		}
		
		if (forward != null && back != null) {
			
			final UnionNode union = new PropertyPathUnionNode();
			
			final JoinGroupNode forwardGroup = new JoinGroupNode();
			
			final JoinGroupNode backGroup = new JoinGroupNode();
			
			union.addArg(forwardGroup);
			
			union.addArg(backGroup);
			
			group.addArg(getPositionOfNodeInGroup(ppNode, group), union);
			
			addNegateds(forwardGroup, forward, ppInfo, alpNode, ppNode);
			
			addNegateds(backGroup, back, ppInfo.inverse(), alpNode, ppNode);
			
		} else if (forward != null) {
			
			addNegateds(group, forward, ppInfo, alpNode, ppNode);
			
		} else if (back != null) {
			
			addNegateds(group, back, ppInfo.inverse(), alpNode, ppNode);
			
		} else {
			// NOP
		}

    }
    
	protected void addNegateds( 
			final GraphPatternGroup<? extends IGroupMemberNode> group,
			final ArrayList<ConstantNode> constants,
			final PropertyPathInfo ppInfo, final ArbitraryLengthPathNode alpNode,
			final PropertyPathNode ppNode) {
    	
    	final VarNode p = anonVar();
    	
    	if (alpNode != null) {
    	    alpNode.addDropVar(p);
    	}
    	
    	final StatementPatternNode sp = ppInfo.toStatementPattern(p);
    	
    	final TermNode[] args = new TermNode[constants.size()+1];
    	
    	args[0] = p;
    	
    	System.arraycopy(constants.toArray(new ConstantNode[constants.size()]), 0, args, 1, constants.size());
    	
    	final FunctionNode function = new FunctionNode(
    			FunctionRegistry.NOT_IN, null, args
				);
    	
    	final FilterNode filter = new FilterNode(function);

    	group.addArg(getPositionOfNodeInGroup(ppNode, group),sp);
    	group.addArg(getPositionOfNodeInGroup(ppNode, group),filter);
    	
    }
    
	/**
	 * Returns the position in the group. If the node is not present in
	 * the group, the last position in the group is returned.
	 * 
	 * We calculate the position in order to make sure that we append
	 * new nodes at the same position of the property path node that
	 * will be replaced later on. This is important in order to keep
	 * semantics of the query, e.g. in the context of OPTIONAL or
	 * MINUS queries we cannot blindly append at the end. See BLZG-1498 
	 * and BLZG-1627 and the respective test cases in TestTickets for
	 * examples that illustrate why this is indeed necessary.
	 */
    protected int getPositionOfNodeInGroup(
	    final PropertyPathNode node, final GraphPatternGroup<? extends IGroupMemberNode> group) {
	    
        final List<? extends IGroupMemberNode> children = group.getChildren();
	    for (int i=0; i<children.size(); i++) {
	        if (children.get(i).equals(node))
	            return i;
	    }
	    
	    // if we don't find the property path node, we're in a nested scope
	    // (e.g. for union subgroups). In that case, we append at the end.
	    return children.size();
	}
	
	/**
	 * Used during parsing to identify simple triple patterns.
	 */
    public static final boolean isSimpleIRI(final PathAlternative pathAlt) {
    	
    	if (pathAlt.arity() == 1) {
    		
    		final PathSequence pathSeq = (PathSequence) pathAlt.get(0);
    		
    		if (pathSeq.arity() == 1) {
    		
        		final PathElt pathElt = (PathElt) pathSeq.get(0);
        		
            	return !pathElt.inverse() && pathElt.getMod() == null && pathElt.isIRI();
            	
    		}
        	
    	}
    	
    	return false;
    	
    }
    
	/**
	 * Used during parsing to identify simple triple patterns.
	 */
    public static final ConstantNode getSimpleIRI(final PathAlternative pathAlt) {
    	
    	if (pathAlt.arity() == 1) {
    		
    		final PathSequence pathSeq = (PathSequence) pathAlt.get(0);
    		
    		if (pathSeq.arity() == 1) {
    		
        		final PathElt pathElt = (PathElt) pathSeq.get(0);
        		
            	if (!pathElt.inverse() && pathElt.getMod() == null && pathElt.isIRI()) {
            		
            		return (ConstantNode) pathElt.get(0);
            				
            	}
            	
    		}
        	
    	}
    	
    	return null;
    	
    }
    
    private static class PropertyPathInfo {
    	
    	public final TermNode s, o, c;
    	
    	public final Scope scope;
    	
    	public PropertyPathInfo(final TermNode s, final TermNode o, final TermNode c, final Scope scope) {
    		this.s = s;
    		this.o = o;
    		this.c = c;
    		this.scope = scope;
    	}
    	
    	public PropertyPathInfo(final TermNode s, final TermNode o, final PropertyPathInfo base) {
    		this(s, o, base.c, base.scope);
    	}
    	
    	public PropertyPathInfo inverse() {
    		return new PropertyPathInfo(o, s, c, scope);
    	}
    	
    	public StatementPatternNode toStatementPattern(final TermNode p) {
    		return new StatementPatternNode(s, p, o, c, scope);
    	}
    	
    }
    
    
}
