package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBind;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ImmutableBOp;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;

/**
 * Operator causes a variable to be bound to the result of its evaluation as a
 * side-effect unless the variable is already bound and the as-bound value does
 * not compare as equals.
 * <p>
 * Note: This is intended for use within a {@link BindingConstraint}.  That
 *
 * @author mroycsi
 */
public class ConditionalBind<E extends IV> extends ImmutableBOp implements
        IValueExpression<E>, IBind<E>, IPassesMaterialization {

    private static final long serialVersionUID = 1L;

    public interface Annotations extends BOp.Annotations {
        String PROJECTION = ConditionalBind.class.getName() + ".projection";
    }

    protected transient Boolean projection;

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public ConditionalBind(ConditionalBind<E> op) {
        super(op);
    }

    /**
     * @param var
	 *            The {@link IVariable} which will be bound to the result of
	 *            evaluating the associated value expression.
     * @param expr
     *            The {@link IValueExpression} to be evaluated.
     */
    public ConditionalBind(IVariable<E> var, IValueExpression<E> expr,boolean projection) {

        this(new BOp[] { var, expr }, NV.asMap(new NV(Annotations.PROJECTION,
                projection)));

        this.projection = projection;

    }

    /**
     * Required shallow copy constructor.
     *
     * @param args
     * @param annotations
     */
    public ConditionalBind(BOp[] args, Map<String, Object> annotations) {

        super(args, annotations);

        if (getProperty(Annotations.PROJECTION) == null)
            throw new IllegalArgumentException();

    }

    boolean isProjection(){

        if (projection == null) {

            projection = (Boolean) getRequiredProperty(Annotations.PROJECTION);

        }

        return projection;

    }

    /**
	 * Return the variable which will be bound to the result of evaluating the
	 * associated value expression.
     */
    @SuppressWarnings("unchecked")
    public IVariable<E> getVar() {

        return (IVariable<E>) get(0);

    }

    /**
     * Return the value expression.
     */
    @SuppressWarnings("unchecked")
    public IValueExpression<E> getExpr() {

        return (IValueExpression<E>) get(1);

    }

    public E get(final IBindingSet bindingSet) {

        final IVariable<E> var = getVar();

        final IValueExpression<E> expr = getExpr();

        /*
         * This code has been removed since (per LeeF) an aggregate in a
         * non-aggregate context should be an error.
         */
//        /*
//         * Evaluate the value expression.
//         *
//         * Note: This also handles the special case of an aggregate appearing
//         * within a function call context outside of a projection.
//         */
//        final E val;
//        final boolean aggregate = !isProjection()
//                && (expr instanceof IAggregate);
//        if (aggregate) {
//            ((IAggregate<E>) expr).reset();
//            expr.get(bindingSet);
//            val = (E) ((IAggregate<E>) expr).done();
//        } else {
//            val = expr.get(bindingSet);
//        }
        final E val = expr.get(bindingSet); // evaluate the value expression.

        final E existing = var.get(bindingSet); // lookup current bound value.
        try{

            if( val == null){

                throw new SparqlTypeErrorException.UnboundVarException();

            }

	        if (existing == null) {

	            // bind the variable as a side-effect.
	            bindingSet.set(var, new Constant<E>(val));

	            // return the evaluated value
	            return val;

	        } else {

	            /*
	             * Note: A BindingConstraint wrapping the ConditionalBind will see
	             * the [null] and fail the solution. This avoids throwing the
	             * SPARQLTypeErrorException, which has more overhead.
	             */

	            return (val.equals(existing)) ? val : null;

	        }
        }catch(SparqlTypeErrorException typeError){
            /*
             * Note: If an assignment fails to evaluate, and there is already a  binding in
             * the solution with the same variable name, the solution fails.
             * If there is not already an assignment, rethrow the exception, so that the
             * BindingConstraint can pass the solution, since the sparql spec says a
             * failure in an assignment passes the solution.
             */
            if(existing!=null){
                return null;
            }else{
                throw typeError;
            }
        }
    }

}
