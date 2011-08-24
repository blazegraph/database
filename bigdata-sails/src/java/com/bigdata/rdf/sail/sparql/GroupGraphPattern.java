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

import java.util.LinkedList;
import java.util.List;

import org.openrdf.query.algebra.StatementPattern;

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
     * This is all direct children of the group.
     */
    private List<IGroupMemberNode> children = new LinkedList<IGroupMemberNode>();

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

    public void add(final IGroupMemberNode te) {
        children.add(te);
    }

    public void addSP(final TermNode s, final TermNode p, final TermNode o) {
        children.add(new StatementPatternNode(s, p, o, context, spScope));
    }

    public void addConstraint(final ValueExpressionNode constraint) {
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
    public <T extends GroupNodeBase> T buildGroup(final T groupNode) {
        
        for (IGroupMemberNode child : children)
            groupNode.addChild(child);

        return groupNode;
                
    }

}
