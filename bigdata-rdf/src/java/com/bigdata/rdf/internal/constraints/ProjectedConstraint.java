package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.error.SparqlTypeErrorException;

public class ProjectedConstraint extends com.bigdata.bop.constraint.Constraint {

    public ProjectedConstraint(final BOp[] args, final Map<String, Object> annotations) {
        super(args, annotations);
    }

    public ProjectedConstraint(final ProjectedConstraint op) {
        super(op);
    }

    public ProjectedConstraint(ConditionalBind bind) {
        super(new BOp[] { bind }, null);
    }

    @Override
    public boolean accept(IBindingSet bindingSet) {
        try {
            Object result = ((ConditionalBind) get(0)).get(bindingSet);
            return  result != null;
        } catch (SparqlTypeErrorException stee) {
        	// Extend(mu, var, expr) = mu if var not in dom(mu) and expr(mu) is an error (from the spec)
            return true;
        }
    }
}
