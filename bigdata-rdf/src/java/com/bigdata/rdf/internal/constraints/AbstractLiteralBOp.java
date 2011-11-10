package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import javax.xml.datatype.DatatypeFactory;

import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

abstract public class AbstractLiteralBOp extends IVValueExpression<IV>
        implements INeedsMaterialization {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    static protected final transient DatatypeFactory datatypeFactory;

    static {
        try {
            datatypeFactory = DatatypeFactory.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Note: The double-checked locking pattern <em>requires</em> the keyword
     * <code>volatile</code>.
     */
    private transient volatile BigdataValueFactory vf;

    protected BigdataValueFactory getValueFactory() {
        if (vf == null) {
            synchronized (this) {
                if (vf == null) {
                    final String namespace = (String) getRequiredProperty(Annotations.NAMESPACE);
                    vf = BigdataValueFactoryImpl.getInstance(namespace);
                }
            }
        }
        return vf;
    }

    public interface Annotations extends BOp.Annotations {

        /**
         * The namespace of the lexicon.
         */
        public String NAMESPACE = AbstractLiteralBOp.class.getName()
                + ".namespace";
        
    }

    public AbstractLiteralBOp(final String lex) {
    	
    	this(BOpBase.NOARGS, NV.asMap(new NV(Annotations.NAMESPACE, lex)));
    	
    }
    
    /**
     * Convenience constructor for most common unary function
     * @param x
     * @param lex
     */
    public AbstractLiteralBOp(final IValueExpression<? extends IV>  x, final String lex) {

        this(new BOp[] { x }, NV.asMap(new NV(Annotations.NAMESPACE, lex)));

    }

    /**
     * Required shallow copy constructor.
     */
    public AbstractLiteralBOp(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

        getRequiredProperty(Annotations.NAMESPACE);

    }

    /**
     * Required deep copy constructor.
     */
    public AbstractLiteralBOp(final AbstractLiteralBOp op) {
        super(op);
    }

    /**
     * This bop can only work with materialized terms.
     */
    public Requirement getRequirement() {
        return INeedsMaterialization.Requirement.SOMETIMES;
    }

    final public IV get(final IBindingSet bs) {

        return _get(bs);

    }

    abstract public IV _get(final IBindingSet bs);

    protected IV getAndCheck(int i, IBindingSet bs) {
        IV iv = get(i).get(bs);
        if (iv == null)
            throw new SparqlTypeErrorException.UnboundVarException();

        if (!iv.isLiteral())
            throw new SparqlTypeErrorException();

        if (!iv.isInline() && !iv.hasValue())
            throw new NotMaterializedException();

        return iv;
    }
    
    protected BigdataLiteral literalValue(IV iv) {

        if (iv.isInline()&&!iv.isExtension()) {

            final BigdataURI datatype = getValueFactory().asValue(iv.getDTE().getDatatypeURI());

            return getValueFactory().createLiteral(((Value) iv).stringValue(), datatype);

        } else if (iv.hasValue()) {

            return ((BigdataLiteral) iv.getValue());

        } else {

            throw new NotMaterializedException();

        }

    }

    protected static String literalLabel(IV iv) {
        if (iv.isInline()&&!iv.isInline()) {
            return ((Value) iv).stringValue();
        } else if (iv.hasValue()) {
            return ((BigdataLiteral) iv.getValue()).getLabel();
        } else {
            throw new NotMaterializedException();
        }
    }



}
