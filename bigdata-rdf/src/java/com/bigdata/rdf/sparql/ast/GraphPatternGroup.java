/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
/*
 * Created on Sep 5, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;

/**
 * Join group or union.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class GraphPatternGroup<E extends IGroupMemberNode> extends
        GroupNodeBase<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Required deep copy constructor.
     */
    public GraphPatternGroup(GraphPatternGroup<E> op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public GraphPatternGroup(BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }
    
    /**
     * 
     */
    public GraphPatternGroup() {
    }

    /**
     * @param optional
     */
    public GraphPatternGroup(boolean optional) {
        super(optional);
    }

    /*
     * Static analysis methods. There is one method which looks "up". This
     * corresponds to how we actually evaluation things (left to right in the
     * query plan). There are two methods which look "down". This corresponds to
     * the bottom-up evaluation semantics of SPARQL.
     */
    
    /**
     * Return the set of variables which MUST be bound coming into this group
     * during top-down, left-to-right evaluation. The returned set is based on a
     * non-recursive analysis of the definitely (MUST) bound variables in each
     * of the parent groups. The analysis is non-recursive for each parent
     * group, but all parents of this group are considered. This approach
     * excludes information about variables which MUST or MIGHT be bound from
     * both <i>this</i> group and child groups. This is useful for determining
     * when to run various filters or whether a filter can be lifted into the
     * parent group.
     * 
     * @param vars
     *            Where to store the "MUST" bound variables.
     *            
     * @return The argument.
     */
    abstract public Set<IVariable<?>> getIncomingBindings(
            final Set<IVariable<?>> vars);

    /**
     * Return the set of variables which MUST be bound for solutions after the
     * evaluation of this group. A group will produce "MUST" bindings for
     * variables from its statement patterns and a LET based on an expression
     * whose variables are known bound.
     * <p>
     * The returned collection reflects "bottom-up" evaluation semantics. This
     * method does NOT consider variables which are already bound on entry to
     * the group.
     * 
     * @param vars
     *            Where to store the "MUST" bound variables.
     * @param recursive
     *            When <code>true</code>, the child groups will be recursively
     *            analyzed. When <code>false</code>, only <i>this</i> group will
     *            be analyzed.
     * 
     * @return The argument.
     * 
     *         TODO Should the recursive analysis throw out variables when part
     *         of the tree will provably fail to bind anything? Right now we do
     *         not do that, we only report on the variables which would be bound
     *         in the solution were to be accepted.
     */
    abstract public Set<IVariable<?>> getDefinatelyProducedBindings(
            final Set<IVariable<?>> vars, boolean recursive);

    /**
     * Return the set of variables which MUST or MIGHT be bound after the
     * evaluation of this join group.
     * <p>
     * The returned collection reflects "bottom-up" evaluation semantics. This
     * method does NOT consider variables which are already bound on entry to
     * the group.
     * 
     * @param vars
     *            Where to store the "MUST" bound variables.
     * @param recursive
     *            When <code>true</code>, the child groups will be recursively
     *            analyzed. When <code>false</code>, only <i>this</i> group will
     *            be analyzed.
     *            
     * @return The argument.
     */
    abstract public Set<IVariable<?>> getMaybeProducedBindings(
            final Set<IVariable<?>> vars, boolean recursive);

}
