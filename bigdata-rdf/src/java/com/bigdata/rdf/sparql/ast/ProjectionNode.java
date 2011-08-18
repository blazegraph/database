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
 * Created on Aug 17, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.LinkedList;
import java.util.List;

import com.bigdata.bop.Bind;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.IV;

/**
 * AST node modeling projected value expressions.
 * <p>
 * Note: "*" is modeled using an explicit variable whose name is <code>*</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ProjectionNode extends ValueExpressionListBaseNode<AssignmentNode> {

    private boolean distinct = false;

    public ProjectionNode() {
    }

    public void setDistinct(final boolean distinct) {

        this.distinct = distinct;

    }

    public boolean isDistinct() {

        return distinct;

    }

    @SuppressWarnings("unchecked")
    public boolean isWildcard() {

        if (isEmpty())
            return false;

        return Var.var("*").equals(exprs.get(0).getVar());

    }

    /**
     * Adds a variable to be projected. The variable is modeled as an assignment
     * of itself to itself, so everything in the projection node winds up
     * looking like an assignment.
     * 
     * @param var
     *            The variable.
     */
    public void addProjectionVar(final VarNode var) {

        addExpr(new AssignmentNode(var, var));

    }

    public void addProjectionExpression(final AssignmentNode assignment) {

        addExpr(assignment);

    }

    /**
     * Return the ordered subset of the value expressions which project a
     * computed value expression which is not a bare variable.
     * 
     * TODO Consistent API for {@link #getAssignmentProjections()} and
     * {@link #getProjectionVars()}.
     */
    public List<AssignmentNode> getAssignmentProjections() {

        final List<AssignmentNode> assignments = new LinkedList<AssignmentNode>();

        for (AssignmentNode n : this) {

            if (n.getValueExpressionNode().equals(n.getVarNode()))
                continue;

            assignments.add(n);

        }

        return assignments;

    }

    /**
     * Return the ordered subset of the value expressions which project a bare
     * variable.
     */
    public IVariable[] getProjectionVars() {

        final List<IVariable<IV>> vars = new LinkedList<IVariable<IV>>();

        for (AssignmentNode n : this) {

            vars.add(n.getVar());

        }

        return (IVariable[]) vars.toArray(new IVariable[vars.size()]);

    }

    /**
     * Return the {@link IValueExpression}s for this {@link ProjectionNode}.
     */
    public IValueExpression[] getValueExpressions() {

        final IValueExpression<?>[] exprs = new IValueExpression[size()];
        
        int i = 0;
        
        for (AssignmentNode n : this) {
        
            exprs[i++]=new Bind(n.getVar(),n.getValueExpression());
            
        }
        
        return exprs;
        
    }
    
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        sb.append("\n");
        
        sb.append(indent(indent));
        
        sb.append("select ");

        if (distinct)
            sb.append("distinct ");

        if (isWildcard()) {

            sb.append("* ");
            
        } else {
            
            for (AssignmentNode v : this) {
                
                sb.append(v);
                
            }

        }

        return sb.toString();

    }

}
