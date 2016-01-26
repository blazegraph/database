/*

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
/* Portions of this code are:
 * 
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.sparql;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.PathNode;
import com.bigdata.rdf.sparql.ast.PropertyPathNode;
import com.bigdata.rdf.sparql.ast.QuadData;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;

/**
 * A group graph pattern consisting of (required and optional) tuple
 * expressions, binding assignments and boolean constraints.
 * 
 * @author Arjohn Kampman
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @openrdf
 */
class GroupGraphPattern {

    private static final Logger log = Logger.getLogger(GroupGraphPattern.class);
    
    private boolean invalid = false;
    
    private void assertValid() {
        if (invalid)
            throw new IllegalStateException();
    }

    /**
     * The context of this graph pattern.
     */
    private TermNode context;

    /**
     * The StatementPattern-scope of this graph pattern.
     */
    private StatementPattern.Scope spScope = StatementPattern.Scope.DEFAULT_CONTEXTS;

    /**
     * This is all direct children of the group.
     */
    private final List<IGroupMemberNode> children = new LinkedList<IGroupMemberNode>();

    /**
     * Creates a new graph pattern.
     */
    public GroupGraphPattern() {
    }
    
    /**
     * Creates a new graph pattern that inherits the context and scope from a
     * parent graph pattern.
     */
    public GroupGraphPattern(final GroupGraphPattern parent) {
        this(parent.context, parent.spScope);
    }
    
    /**
     * Creates a new graph pattern in the given context and scope.
     */
    public GroupGraphPattern(TermNode context, Scope spScope) {
       this.context = context;
       this.spScope = spScope;
    }


    public void setContextVar(final TermNode context) {

        assertValid();
        
        this.context = context;
        
    }

    public TermNode getContext() {
        
        assertValid();
        
        return context;
        
    }

//    /**
//     * The #of things in the group.
//     */
//    public int size() {
//        
//        return children.size();
//        
//    }
//
//    /**
//     * Return the child at that index.
//     * 
//     * @param index
//     *            The index.
//     *            
//     * @return The child.
//     */
//    public IGroupMemberNode get(final int index) {
//     
//        return children.get(index);
//        
//    }
    
    public void setStatementPatternScope(final StatementPattern.Scope spScope) {
        
        assertValid();
        
        this.spScope = spScope;
        
    }

    public StatementPattern.Scope getStatementPatternScope() {
        
        assertValid();
        
        return spScope;
        
    }

    public void add(final IGroupMemberNode child) {
        
        assertValid();
        
        if (log.isInfoEnabled())
            log.info("child=" + BOpUtility.toString((BOp)child));

        children.add(child);

    }

    public void addSP(final TermNode s, final TermNode p, final TermNode o) {

        assertValid();

        if (log.isInfoEnabled())
            log.info("pattern= ( " + s + " " + p + " " + o + " )");

        // Fill in the inherited context and scope.
        children.add(new StatementPatternNode(s, p, o, context, spScope));

    }
    
    public void addPP(final TermNode s, final PathNode p, final TermNode o) {
    	
        assertValid();

        if (log.isInfoEnabled())
            log.info("pattern= ( " + s + " " + p + " " + o + " )");

        // Fill in the inherited context and scope.
        children.add(new PropertyPathNode(s, p, o, context, spScope));
    	
    }

    public void addSP(final StatementPatternNode sp) {

        assertValid();

        if (log.isInfoEnabled())
            log.info("pattern=" + sp);

        // Fill in the inherited context and scope.
//        final StatementPatternNode t = new StatementPatternNode(sp.s(), sp.p(),
//                sp.o(), context, spScope);
		
        if (context != null) {
		
        		sp.setC(context);
        	
		}
        
        sp.setScope(spScope);

        children.add(sp);

    }
    
    public void addConstraint(final ValueExpressionNode constraint) {

        assertValid();

        if (log.isInfoEnabled())
            log.info("constraint=" + constraint);

        children.add(new FilterNode(constraint));

    }

    /**
     * Attach the data to the group node.
     * 
     * @param groupNode
     *            The group node.
     * 
     * @return The group node supplied by the caller with the data attached.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends GroupNodeBase> T buildGroup(final T groupNode) {

        assertValid();

        // mark the GroupGraphPattern as consumed.
        invalid = true;

        final boolean isUnion = groupNode instanceof UnionNode;

        if (isUnion) {

            /*
             * If we are building a union node, then we will (A) flatten nested
             * UNIONs into a single UNION; and (b) wrap any non-JoinGroupNode
             * with a JoinGroupNode.
             */
            for (IGroupMemberNode child : children) {

                if(child instanceof JoinGroupNode) {
                    
                    groupNode.addChild(child);
                    
                } else if(child instanceof UnionNode) {
                    
                    /*
                     * Lift children out of the child UNION.
                     */
                    final UnionNode childUnion = (UnionNode) child;
                    
                    for(IGroupMemberNode child2 : childUnion) {
                        
                        groupNode.addChild(child2);
                        
                    }
                    
                } else {
                
                    groupNode.addChild(new JoinGroupNode(child));
                    
                }
                
            }
            
        } else {

            for (IGroupMemberNode child : children) {

                if (child instanceof QuadData
                        && groupNode instanceof JoinGroupNode) {
                    
                    /*
                     * We need to flatten out the QuadData when it appears
                     * within a WHERE clause for a DELETE WHERE shortcut.
                     * 
                     * @see https://sourceforge.net/apps/trac/bigdata/ticket/568
                     * (DELETE WHERE fails with Java AssertionError)
                     */
                    
                    final JoinGroupNode newGroup = new JoinGroupNode();

                    ((QuadData) child).flatten(newGroup);

                    // A statement pattern node from the nested GRAPH group.
                    // They will all share the same context.
                    
                    final StatementPatternNode sp = (StatementPatternNode) newGroup
                            .get(0);

                    // The context for the SPs in that GRAPH group.
                    final TermNode context = (TermNode) sp.get(3);
                    
                    // Must be defined.
                    assert context != null;
                    
                    // Must be a named graph scope.
                    assert sp.getScope() == Scope.NAMED_CONTEXTS;
                    
                    // Set on the new group.
                    newGroup.setContext(context);

                    // Add the new group to the outer group.
                    groupNode.addChild(newGroup);
                    
                } else {
                
                    groupNode.addChild(child);

                }
                
            }

        }
        
        return groupNode;

    }
    
    /**
     * Useful when the caller knows that the children will be a single
     * {@link JoinGroupNode}.
     * 
     * @return That {@link JoinGroupNode}.
     */
    public JoinGroupNode getSingletonGroup() {
        
        assertValid();
        
        invalid = true;

        if (children.size() != 1)
            throw new RuntimeException("Expecting one child, not "
                    + children.size());
        
        return (JoinGroupNode) children.get(0);
        
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(super.toString() + "{");
        sb.append("context=" + context);
        sb.append(", scope=" + spScope);
        sb.append(", children=[");
        boolean first = true;
        for (IGroupMemberNode child : children) {
            if (!first)
                sb.append(", ");
            sb.append(child);
            first = false;
        }
        sb.append("]}");
        return sb.toString();
    }

}
