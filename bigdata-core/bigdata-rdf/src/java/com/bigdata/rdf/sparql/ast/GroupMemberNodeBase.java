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

import java.util.Map;

import com.bigdata.bop.BOp;

/**
 * Anything which can appear in an {@link IGroupNode}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
abstract public class GroupMemberNodeBase<E extends IGroupMemberNode> extends
        QueryNodeBase implements IGroupMemberNode {

    private static final long serialVersionUID = 1L;

    public interface Annotations extends IGroupMemberNode.Annotations {
    	
    }
    
    private IGroupNode<IGroupMemberNode> parent;

    @Override
    final public IGroupNode<IGroupMemberNode> getParent() {

        return parent;

    }

    @Override
    final public void setParent(final IGroupNode<IGroupMemberNode> parent) {

        this.parent = parent;

    }

    public GroupMemberNodeBase() {
    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public GroupMemberNodeBase(GroupMemberNodeBase<E> op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public GroupMemberNodeBase(BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }

    @Override
    public TermNode getContext() {
    
        final IQueryNode parent = getParent();
        
        if (parent instanceof GroupMemberNodeBase<?>) {

            /*
             * Recursion up to the parent context.
             * 
             * TODO It would seem better to explicitly recurse until we find the
             * first JoinGroup parent, and to define a getJoinGroup() method for
             * that.
             */
            return ((GroupMemberNodeBase<?>) parent).getContext();
            
        }
        
        return null;
    
    }

    @Override
    public JoinGroupNode getParentJoinGroup() {

        IGroupNode<?> parent = getParent();

        while (parent != null) {

            if (parent instanceof JoinGroupNode)
                return (JoinGroupNode) parent;

            parent = parent.getParent();

        }

        return null;

    }

    @Override
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
    
}
