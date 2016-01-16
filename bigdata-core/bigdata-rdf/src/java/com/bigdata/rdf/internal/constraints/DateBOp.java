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
package com.bigdata.rdf.internal.constraints;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.Literal;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.datetime.Timezone;
import org.openrdf.query.algebra.evaluation.function.datetime.Tz;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.ImmutableBOp;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
import com.bigdata.rdf.internal.impl.literal.XSDDecimalIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.ibm.icu.text.DateFormat;

/**
 * A date expression involving a left IValueExpression operand. The operation to be applied to the operands is specified by the {@link Annotations#OP}
 * annotation.
 */
public class DateBOp extends IVValueExpression<IV> implements INeedsMaterialization{

    /**
	 *
	 */
    private static final long serialVersionUID = 9136864442064392445L;

    public interface Annotations extends ImmutableBOp.Annotations {

        /**
         * The operation to be applied to the left operands (required). The value of this annotation is a {@link DateOp}, such as {@link DateOp#YEAR}.
         *
         * @see DateOp
         */
        String OP = (DateBOp.class.getName() + ".op").intern();

    }

    public enum DateOp {
        YEAR, MONTH, DAY, HOURS, MINUTES, SECONDS, TZ, TIMEZONE, DATE

    }

    @Override
    protected boolean areGlobalsRequired() {
     
        return true;
        
    }
    
    /**
     *
     * @param left
     *            The left operand.
     * @param right
     *            The right operand.
     * @param op
     *            The annotation specifying the operation to be performed on those operands.
     */
    public DateBOp(final IValueExpression<? extends IV> left, 
    		final DateOp op, final GlobalAnnotations globals) {

        this(new BOp[] { left }, anns(globals, new NV(Annotations.OP, op)));

    }

    /**
     * Required shallow copy constructor.
     *
     * @param args
     *            The operands.
     * @param op
     *            The operation.
     */
    public DateBOp(final BOp[] args, Map<String, Object> anns) {

        super(args, anns);

        if (args.length != 1 || args[0] == null || getProperty(Annotations.OP) == null) {

            throw new IllegalArgumentException();

        }

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     *
     * @param op
     */
    public DateBOp(final DateBOp op) {

        super(op);

    }

    final public IV get(final IBindingSet bs) {

        final IV left = left().get(bs);

        // not yet bound?
        if (left == null)
            throw new SparqlTypeErrorException.UnboundVarException();


        if (left.isLiteral()) {
            if(!left.hasValue()){
                throw new NotMaterializedException();
            }

            BigdataLiteral bl = (BigdataLiteral) left.getValue();
            if (XSD.DATETIME.equals(bl.getDatatype())||XSD.DATE.equals(bl.getDatatype())||XSD.TIME.equals(bl.getDatatype())) {
                XMLGregorianCalendar cal=bl.calendarValue();
                switch (op()) {
                case DAY:
                    return new XSDIntegerIV(BigInteger.valueOf(cal.getDay()));
                case MONTH:
                    return new XSDIntegerIV(BigInteger.valueOf(cal.getMonth()));
                case YEAR:
                    return new XSDIntegerIV(BigInteger.valueOf(cal.getYear()));
                case HOURS:
                    return new XSDIntegerIV(BigInteger.valueOf(cal.getHour()));
                case SECONDS:
                    return new XSDDecimalIV(BigDecimal.valueOf(cal.getSecond()));
                case MINUTES:
                    return new XSDIntegerIV(BigInteger.valueOf(cal.getMinute()));
                case TZ:
                    return tz(bl);
                case TIMEZONE:
                    return timezone(bl);
                case DATE:
                	/*
                	 * @see https://jira.blazegraph.com/browse/BLZG-1388
                	 * FullyInlineTypedLiteralIV is used to keep exact string representation of newDate in literal,
                	 * that results in different internal representation in contrast to xsd:dateTime values,
                	 * which represented as LiteralExtensionIV with delegate of XSDLong 
                	 */
                	Date d = cal.toGregorianCalendar().getTime();
                	String newDate = new SimpleDateFormat("yyyy-MM-dd").format(d);
                    return new FullyInlineTypedLiteralIV<>(newDate, null, XSD.DATE);
                default:
                    throw new UnsupportedOperationException();
                }
            }
        }
        throw new SparqlTypeErrorException();
    }

    public IValueExpression<? extends IV> left() {
        return get(0);
    }

    public DateOp op() {
        return (DateOp) getRequiredProperty(Annotations.OP);
    }

    public String toString() {

        final StringBuilder sb = new StringBuilder();
        sb.append(op());
        sb.append("(").append(left()).append(")");
        return sb.toString();

    }

    public Requirement getRequirement() {
        return Requirement.SOMETIMES;
    }
    
    protected IV tz(final BigdataLiteral l) {
     
        final Tz func = new Tz();
        
        final BigdataValueFactory vf = super.getValueFactory();
        
        try {
            
            final BigdataLiteral l2 = (BigdataLiteral) func.evaluate(vf, l);
            
            return DummyConstantNode.toDummyIV(l2);
            
        } catch (ValueExprEvaluationException e) {
            
            throw new SparqlTypeErrorException();
            
        }
        
    }

    protected IV timezone(final BigdataLiteral l) {
        
        final Timezone func = new Timezone();
        
        final BigdataValueFactory vf = super.getValueFactory();
        
        try {
            
            final BigdataLiteral l2 = (BigdataLiteral) func.evaluate(vf, l);
            
            return DummyConstantNode.toDummyIV(l2);
            
        } catch (ValueExprEvaluationException e) {
            
            throw new SparqlTypeErrorException();
            
        }
        
    }
    
}
