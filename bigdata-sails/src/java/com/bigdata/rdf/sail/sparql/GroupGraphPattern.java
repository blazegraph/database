/*

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.openrdf.query.algebra.StatementPattern;

import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;

/**
 * A group graph pattern consisting of (required and optional) tuple
 * expressions, binding assignments and boolean constraints.
 * 
 * @author Arjohn Kampman
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * TODO Clean up.
 */
class GroupGraphPattern {

    /**
     * The context of this graph pattern.
     */
    private TermNode context;

    /**
     * The StatementPattern-scope of this graph pattern.
     */
    private StatementPattern.Scope spScope = StatementPattern.Scope.DEFAULT_CONTEXTS;

    /**
     * The required tuple expressions in this graph pattern.
     * 
     * TODO This is all children of the group (other than the filters).
     */
    private List<IGroupMemberNode> requiredTEs = new LinkedList<IGroupMemberNode>();

//    /**
//     * The optional tuple expressions in this graph pattern.
//     */
//    private List<IGroupMemberNode> optionalTEs = new LinkedList<IGroupMemberNode>();

    /**
     * The boolean constraints in this graph pattern.
     */
    private List<ValueExpressionNode> constraints = new LinkedList<ValueExpressionNode>();

    /**
     * The binding assignments in this graph pattern.
     */
    private List<AssignmentNode> assignments = new LinkedList<AssignmentNode>();

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
        context = parent.context;
        spScope = parent.spScope;
    }

    public void setContextVar(final TermNode context) {
        this.context = context;
    }

    public TermNode getContext() {
        return context;
    }

    public void setStatementPatternScope(final StatementPattern.Scope spScope) {
        this.spScope = spScope;
    }

    public StatementPattern.Scope getStatementPatternScope() {
        return spScope;
    }

    // TODO rename since optionals not separately stored.
    public void addRequiredTE(final IGroupMemberNode te) {
        requiredTEs.add(te);
    }

    // TODO rename since optionals not separately stored.
    public void addRequiredSP(final TermNode s, final TermNode p,
            final TermNode o) {
        addRequiredTE(new StatementPatternNode(s, p, o, context, spScope));
    }

    // TODO rename since optionals not separately stored.
    public List<IGroupMemberNode> getRequiredTEs() {
        return Collections.unmodifiableList(requiredTEs);
    }

    public List<AssignmentNode> getBindingAssignments() {
        return Collections.unmodifiableList(assignments);
    }

//    public void addOptionalTE(final IGroupMemberNode te) {
//        optionalTEs.add(te);
//    }

//    public List<IGroupMemberNode> getOptionalTEs() {
//        return Collections.unmodifiableList(optionalTEs);
//    }

    public void addConstraint(final ValueExpressionNode constraint) {
        constraints.add(constraint);
    }

    // TODO Is this necessary?
    public void addBindingAssignment(final AssignmentNode bindingAssignment) {
        assignments.add(bindingAssignment);
    }

    public void addConstraints(final Collection<ValueExpressionNode> constraints) {
        this.constraints.addAll(constraints);
    }

//    public List<ValueExpressionNode> getConstraints() {
//        return Collections.unmodifiableList(constraints);
//    }

//    public List<FilterNode> removeAllConstraints() {
//        List<FilterNode> constraints = this.constraints;
//        this.constraints = new ArrayList<FilterNode>();
//        return constraints;
//    }

    /**
     * Removes all tuple expressions and constraints.
     */
    public void clear() {
        requiredTEs.clear();
//        optionalTEs.clear();
        constraints.clear();
    }

    /**
     * Attach the data to the group node.
     * 
     * @param groupNode
     *            The group node.
     * 
     * @return The group node supplied by the caller with the data attached.
     */
    public <T extends GroupNodeBase> T buildGroup(final T groupNode) {
        
        for (IGroupMemberNode child : requiredTEs)
            groupNode.addChild(child);

        for (ValueExpressionNode child : constraints)
            groupNode.addChild(new FilterNode(child));

        return groupNode;
                
    }

//    /**
//     * Builds a {@link GroupNodeBase} for the {@link GroupGraphPattern}.
//     * 
//     * @return The group node.
//     */
//    public GroupNodeBase buildTupleExpr() {
//        
//        GroupNodeBase result;
//
//        if (requiredTEs.isEmpty()) {
//            result = new SingletonSet();
//        }
//        else {
//            result = requiredTEs.get(0);
//
//            for (int i = 1; i < requiredTEs.size(); i++) {
//                result = new Join(result, requiredTEs.get(i));
//            }
//        }
//
//        for (TupleExpr optTE : optionalTEs) {
//            result = new LeftJoin(result, optTE);
//        }
//        
//        for (Extension assignment : assignments) {
//            assignment.setArg(result);
//            result = assignment;
//        }
//
//        for (ValueExpr constraint : constraints) {
//            result = new Filter(result, constraint);
//        }
//
//        return result;
//    }
    
}
