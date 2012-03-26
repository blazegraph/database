package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Abstract base class for value expression bops operating on {@link IV}s and
 * {@link BigdataValue}s.
 * <p>
 * Note: This class implements {@link INeedsMaterialization}. Concrete instances
 * of this class MUST consider their materialization requirements.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <V>
 */
@SuppressWarnings("rawtypes")
abstract public class AbstractLiteralBOp<V extends IV> extends
        AbstractIVValueExpressionBOp2<V> implements INeedsMaterialization {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public AbstractLiteralBOp(final String lex) {
        
        this(BOpBase.NOARGS, NV.asMap(new NV(Annotations.NAMESPACE, lex)));
        
    }
    
    /**
     * Convenience constructor for most common unary function
     * 
     * @param x
     * @param lex
     */
    public AbstractLiteralBOp(final IValueExpression<? extends IV> x,
            final String lex) {

        this(new BOp[] { x }, NV.asMap(new NV(Annotations.NAMESPACE, lex)));

    }

    /**
     * Required shallow copy constructor.
     */
    public AbstractLiteralBOp(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

        if (isLexiconNamespaceRequired())
            getRequiredProperty(Annotations.NAMESPACE);

    }

    /**
     * Required deep copy constructor.
     */
    public AbstractLiteralBOp(final AbstractLiteralBOp<V> op) {
        
        super(op);
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: The default implementation returns {@link Requirement#SOMETIMES}.
     */
    @Override
    public Requirement getRequirement() {

        return Requirement.SOMETIMES;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: This delegates to {@link #_get(IBindingSet)}.
     */
    @Override
    final public V get(final IBindingSet bs) {

        return (V) _get(bs);

    }

    /**
     * Core more for the implementation of your operator semantics.
     * 
     * @param bs
     *            The source solution.
     *            
     * @return The result of evaluating this operator against its arguments in
     *         the context of that source solution.
     */
    abstract protected V _get(final IBindingSet bs);

    /**
     * Get the function argument (a value expression) and evaluate it against
     * the source solution. The evaluation of value expressions is recursive.
     * 
     * @param i
     *            The index of the function argument ([0...n-1]).
     * @param bs
     *            The source solution.
     * 
     * @return The result of evaluating that argument of this function.
     * 
     * @throws IndexOutOfBoundsException
     *             if the index is not the index of an operator for this
     *             operator.
     * 
     * @throws SparqlTypeErrorException
     *             if the value expression at that index can not be evaluated.
     * 
     * @throws NotMaterializedException
     *             if evaluation encountered an {@link IV} whose {@link IVCache}
     *             was not set when the value expression required a materialized
     *             RDF {@link Value}.
     */
    protected IV getAndCheckIfMaterializedLiteral(final int i, final IBindingSet bs)
            throws SparqlTypeErrorException, NotMaterializedException {

        final IV iv = get(i).get(bs);
        
        if (iv == null)
            throw new SparqlTypeErrorException.UnboundVarException();

        if (!iv.isLiteral())
            throw new SparqlTypeErrorException();
        
        if (!iv.isInline() && !iv.hasValue())
            throw new NotMaterializedException();

        return iv;

    }
    
    /**
     * Get the function argument (a value expression) and evaluate it against
     * the source solution. The evaluation of value expressions is recursive.
     * 
     * @param i
     *            The index of the function argument ([0...n-1]).
     * @param bs
     *            The source solution.
     * 
     * @return The result of evaluating that argument of this function.
     * 
     * @throws IndexOutOfBoundsException
     *             if the index is not the index of an operator for this
     *             operator.
     * 
     * @throws SparqlTypeErrorException
     *             if the value expression at that index can not be evaluated.
     */
    protected IV getAndCheckIfLiteral(final int i, final IBindingSet bs)
            throws SparqlTypeErrorException {

        final IV iv = get(i).get(bs);
        
        if (iv == null)
            throw new SparqlTypeErrorException.UnboundVarException();

        if (!iv.isLiteral())
            throw new SparqlTypeErrorException();
        
        return iv;

    }
    
}
