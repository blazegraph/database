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
 * Created on Aug 17, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@SuppressWarnings("rawtypes")
public abstract class GroupMemberValueExpressionNodeBase extends
        GroupMemberNodeBase implements IValueExpressionMetadata {

//    interface Annotations extends GroupMemberNodeBase.Annotations {
//        
//        String VALUE_EXPR = "valueExpr";
//        
//    }

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public GroupMemberValueExpressionNodeBase() {
    }

    public GroupMemberValueExpressionNodeBase(BOp[] args,
            Map<String, Object> anns) {

        super(args, anns);

    }

    public GroupMemberValueExpressionNodeBase(
            GroupMemberValueExpressionNodeBase op) {

        super(op);

    }

    abstract public IValueExpressionNode getValueExpressionNode();

    abstract IValueExpression<? extends IV> getValueExpression();

    /**
     * Return the {@link IValueExpression}.
     * 
     * @return The {@link IValueExpression} and never <code>null</code>.
     * 
     * @throws IllegalStateException
     *             if the {@link IValueExpression} is not set on this node.
     */
    public final IValueExpression<? extends IV> getRequiredValueExpression() {
        
        final IValueExpression<? extends IV> valueExpr = getValueExpression();

        if (valueExpr == null)
            throw new IllegalStateException("ValueExpression not set: " + this);

        return valueExpr;
        
    }

    @Override
    public Set<IVariable<?>> getConsumedVars() {

       final Set<IVariable<?>> consumedVars = new LinkedHashSet<IVariable<?>>();

       // collect variables from required value expression
       final Iterator<IVariable<?>> it = 
          BOpUtility.getSpannedVariables(getRequiredValueExpression());

       while (it.hasNext()) {
          consumedVars.add(it.next());
      }
       
       return consumedVars;
    }

    @Override
    public ComputedMaterializationRequirement getMaterializationRequirement() {

        final IValueExpression<?> ve = getRequiredValueExpression();

        return new ComputedMaterializationRequirement(ve);

    }

}
