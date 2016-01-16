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
 * Created on Sep 5, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSubGroupJoinVarOptimizer;

/**
 * Join group or union.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * TODO It would make the internal APIs significantly easier if we modeled this
 * as a type of {@link GraphPatternGroup}, similar to {@link JoinGroupNode} and
 * {@link UnionNode}.
 */
abstract public class GraphPatternGroup<E extends IGroupMemberNode> extends
        GroupNodeBase<E> implements IJoinNode {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends GroupNodeBase.Annotations {

        /**
         * An {@link IVariable}[] of the join variables will be definitely bound
         * when we begin to evaluate a sub-group. This information is used to
         * build a hash index on the join variables and to hash join the
         * sub-group's solutions back into the parent group's solutions.
         * 
         * @see ASTSubGroupJoinVarOptimizer
         */
        String JOIN_VARS = "joinVars";
        
        /**
         * An {@link IVariable}[] of the variables that are used by the 
         * group and that have already appeared in the query up to this point 
         * (and thus may be bound and should be projected into the group).
         * 
         * @see ASTSubGroupJoinVarOptimizer
         */
        String PROJECT_IN_VARS = "projectInVars";
        
    }
    
    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public GraphPatternGroup(final GraphPatternGroup<E> op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public GraphPatternGroup(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }
    
    /**
     * 
     */
    public GraphPatternGroup() {
    }

    /**
     * The join variables for the group.
     * 
     * @see Annotations#JOIN_VARS
     */
    public IVariable<?>[] getJoinVars() {
        return (IVariable[]) getProperty(Annotations.JOIN_VARS);
    }

    public void setJoinVars(final IVariable<?>[] joinVars) {
        setProperty(Annotations.JOIN_VARS, joinVars);
    }
    
    /**
     * The variables that should be projected into the group.
     * 
     * @see Annotations#PROJECT_IN_VARS
     */
    public IVariable<?>[] getProjectInVars() {
        return (IVariable[]) getProperty(Annotations.PROJECT_IN_VARS);
    }

    public void setProjectInVars(final IVariable<?>[] projectInVars) {
        setProperty(Annotations.PROJECT_IN_VARS, projectInVars);
    }
    
    /**
     * Return the nodes of the supplied type.  Uses isAssignableFrom to 
     * determine whether the node is an instance of the supplied type.
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getChildren(final Class<T> type) {
        
        final List<T> children = new LinkedList<T>();
        
        for (IQueryNode node : this) {
            
            if (type.isAssignableFrom(node.getClass())) {
                
                children.add((T) node);
                
            }
            
        }
        
        return children;
        
    }
    
}
