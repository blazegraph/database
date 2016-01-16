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
 * Created on Aug 18, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;

/**
 * AST node for subqueries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class SubqueryBase extends QueryBase implements
        IGroupMemberNode {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private IGroupNode<IGroupMemberNode> parent;

    final public IGroupNode<IGroupMemberNode> getParent() {

        return parent;

    }

    final public void setParent(final IGroupNode<IGroupMemberNode> parent) {

        this.parent = parent;

    }

    /**
     * Deep copy constructor.
     */
    public SubqueryBase(final SubqueryBase queryBase) {

        super(queryBase);

    }

    /**
     * Shallow copy constructor.
     */
    public SubqueryBase(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }

    public SubqueryBase(final QueryType queryType) {

        super(queryType);

    }

    public TermNode getContext() {

        final IQueryNode parent = getParent();

        if (parent instanceof GroupMemberNodeBase<?>) {

            return ((GroupMemberNodeBase<?>) parent).getContext();

        }

        return null;

    }

    public JoinGroupNode getParentJoinGroup() {

        IGroupNode<?> parent = getParent();

        while (parent != null) {

            if (parent instanceof JoinGroupNode)
                return (JoinGroupNode) parent;

            parent = parent.getParent();

        }

        return null;

    }

    @SuppressWarnings("unchecked")
    public GraphPatternGroup<IGroupMemberNode> getParentGraphPatternGroup() {
        
        IGroupNode<?> parent = getParent();

        while (parent != null) {

            if (parent instanceof GraphPatternGroup)
                return (GraphPatternGroup<IGroupMemberNode>) parent;

            parent = parent.getParent();

        }

        return null;
        
    }

    @Override
    public Set<IVariable<?>> getRequiredBound(StaticAnalysis sa) {
        return new HashSet<IVariable<?>>();
    }

    @Override
    public Set<IVariable<?>> getDesiredBound(StaticAnalysis sa) {
        return getProjectedVars(new HashSet<IVariable<?>>());
    }
}
