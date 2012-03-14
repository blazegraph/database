package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Abstract base class for value expression bops operating on {@link IV}s and
 * {@link BigdataValue}s.
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

//    static protected final transient DatatypeFactory datatypeFactory;
//    static {
//        try {
//            datatypeFactory = DatatypeFactory.newInstance();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }

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

    @Override
    public Requirement getRequirement() {

        return INeedsMaterialization.Requirement.SOMETIMES;
        
    }

    @Override
    final public V get(final IBindingSet bs) {

        return (V) _get(bs);

    }

    abstract protected V _get(final IBindingSet bs);

    protected IV getAndCheck(final int i, final IBindingSet bs) {

        final IV iv = get(i).get(bs);
        
        if (iv == null)
            throw new SparqlTypeErrorException.UnboundVarException();

        if (!iv.isLiteral())
            throw new SparqlTypeErrorException();

        if (!iv.isInline() && !iv.hasValue())
            throw new NotMaterializedException();

        return iv;
    }
    
}
