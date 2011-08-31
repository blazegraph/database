package com.bigdata.rdf.internal.constraints;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.datatype.DatatypeFactory;

import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.impl.AbstractInlineIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

abstract public class AbstractLiteralBOp extends IVValueExpression<IV> implements INeedsMaterialization {
    static protected final DatatypeFactory            datatypeFactory;
    static {
        try {
            datatypeFactory = DatatypeFactory.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected transient BigdataValueFactory vf;

    public interface Annotations extends BOp.Annotations {
        public String NAMESPACE = (AbstractLiteralBOp.class.getName() + ".namespace").intern();
    }

    /**
     * Convienence constructor for most common unary function
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

        if (getProperty(Annotations.NAMESPACE) == null)
            throw new IllegalArgumentException();

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

        if (vf == null) {
            synchronized (this) {
                if (vf == null) {
                    final String namespace = (String) getRequiredProperty(Annotations.NAMESPACE);
                    vf = BigdataValueFactoryImpl.getInstance(namespace);
                }
            }
        }
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

            final BigdataURI datatype = vf.asValue(iv.getDTE().getDatatypeURI());

            return vf.createLiteral(((Value) iv).stringValue(), datatype);

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
