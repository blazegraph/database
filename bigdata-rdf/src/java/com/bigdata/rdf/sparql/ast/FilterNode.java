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
 * AST node models a value expression which imposes a constraint.
 */
public class FilterNode extends ValueExpressionNodeBase {

	private final IValueExpressionNode ve;
	
	private final Set<IVariable<?>> consumedVars;
	
	private final INeedsMaterialization.Requirement materializationRequirement;
	
	private final Set<IVariable<IV>> varsToMaterialize;

    /**
     * 
     * @param ve
     *            A value expression which places a constraint on the query.
     */
	public FilterNode(final IValueExpressionNode ve) {
		
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
	
	public IValueExpression<? extends IV> getValueExpression() {
		return ve.getValueExpression();
	}
	
	public IValueExpressionNode getValueExpressionNode() {
		return ve;
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

    @Override
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder(indent(indent));

        sb.append("filter(").append(ve).append(")");

        return sb.toString();

    }

    @Override
    public boolean equals(Object o) {

        if (this == o)
            return true;

        if (!(o instanceof FilterNode))
            return false;

        final FilterNode t = (FilterNode) o;

        if (!ve.equals(t.ve))
            return false;

        return true;

    }

}
