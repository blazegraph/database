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

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Bind;
import com.bigdata.bop.IValueExpression;

/**
 * AST node for a GROUP BY clause.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class GroupByNode extends ValueExpressionListBaseNode<AssignmentNode> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    public GroupByNode() {
    }

    /**
     * Deep copy constructor.
     */
    public GroupByNode(final GroupByNode op) {

        super(op);

    }

    /**
     * Shallow copy constructor.
     */
    public GroupByNode(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }
    
    public void addGroupByVar(final VarNode var) {

        addExpr(new AssignmentNode(var, var));

    }

    /**
     * Return the {@link IValueExpression}s for this {@link GroupByNode}.
     */
    public IValueExpression[] getValueExpressions() {

        final IValueExpression<?>[] exprs = new IValueExpression[size()];

        int i = 0;

        for (IValueExpressionNode node : this) {

            exprs[i++] = new Bind(((AssignmentNode) node).getVar(),
                    ((AssignmentNode) node).getValueExpression());

        }

        return exprs;
        
    }

    @Override
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        sb.append("\n").append(indent(indent)).append("group by ");

        boolean first = true;
        
        for (IValueExpressionNode v : this) {
            
            if(first) {

                first = false;
                
            } else {
                
                sb.append(" ");
                
            }
            
            sb.append(v);
            
        }

        return sb.toString();

    }

    @Override
    public boolean equals(final Object o) {

        if (this == o)
            return true;

        if (!(o instanceof GroupByNode))
            return false;

        return super.equals(o);
        
    }

}
