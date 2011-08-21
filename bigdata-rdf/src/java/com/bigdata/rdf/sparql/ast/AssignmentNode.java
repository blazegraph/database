package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;

/**
 * AST node models the assignment of a value expression to a variable.
 */
public class AssignmentNode extends ValueExpressionNodeBase implements
        IValueExpressionNode {

    private final VarNode                           var;

    private final IValueExpressionNode              ve;

    private final Set<IVariable<?>>                 consumedVars;

    private final INeedsMaterialization.Requirement materializationRequirement;

    private final Set<IVariable<IV>>                varsToMaterialize;

    /**
     * 
     * @param var
     *            The variable which will be bound.
     * @param ve
     *            The value expression to be computed.
     */
    public AssignmentNode(final VarNode var, final IValueExpressionNode ve) {

        this.var = var;
        this.ve = ve;

        consumedVars = new LinkedHashSet<IVariable<?>>();

        final Iterator<IVariable<?>> it = BOpUtility.getSpannedVariables(ve
                .getValueExpression());

        while (it.hasNext()) {
            consumedVars.add(it.next());
        }

        varsToMaterialize = new LinkedHashSet<IVariable<IV>>();

        materializationRequirement = gatherVarsToMaterialize(
                ve.getValueExpression(), varsToMaterialize);

    }

    public IValueExpressionNode getValueExpressionNode() {
        return ve;
    }

    public IValueExpression<? extends IV> getValueExpression() {
        return ve.getValueExpression();
    }

    public VarNode getVarNode() {
        return var;
    }

    public IVariable<IV> getVar() {
        return var.getVar();
    }

    public Set<IVariable<?>> getConsumedVars() {
        return consumedVars;
    }

    public INeedsMaterialization.Requirement getMaterializationRequirement() {
        return materializationRequirement;
    }

    public Set<IVariable<IV>> getVarsToMaterialize() {
        return varsToMaterialize;
    }

    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder(indent(indent));
        
        if (ve == var) {

            sb.append("?");
            
            sb.append(var.getVar().toString());
            
        } else {
            
            sb.append("(LET ");

            sb.append("?").append(getVar().toString());

            sb.append(":=");

            sb.append(ve.toString());

            sb.append(")");

        }
        
        return sb.toString();
        
    }

    public boolean equals(final Object o) {

        if (this == o)
            return true;

        if (!(o instanceof AssignmentNode))
            return false;

        final AssignmentNode t = (AssignmentNode) o;

        if (!var.equals(t.var))
            return false;

        if (!ve.equals(t.ve))
            return false;

        return true;

    }

}
