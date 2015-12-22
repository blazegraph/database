package com.bigdata.rdf.internal.constraints;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;
import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.MathBOp.MathOp;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;

public class DateTimeUtility implements IMathOpHandler {
    static protected final DatatypeFactory datatypeFactory;
    static {
        try {
            datatypeFactory = DatatypeFactory.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean canInvokeMathOp(final Literal... args) {
    	for (Literal lit : args) {
    		final URI dt = lit.getDatatype();
    		if (dt == null)
    			return false;
    		if (!(XMLDatatypeUtil.isCalendarDatatype(dt) || dt.equals(XMLSchema.DURATION)))
				return false;
    	}
    	return true;
    }

    @Override
    public IV doMathOp(
    		final Literal l1, final IV iv1,
    		final Literal l2, final IV iv2,
    		final MathOp op,
    		final BigdataValueFactory vf) {

        if (!canInvokeMathOp(l1, l2))
        	throw new SparqlTypeErrorException();

    	final URI dt1 = l1.getDatatype();
        final URI dt2 = l2.getDatatype();
        XMLGregorianCalendar c1 = XMLDatatypeUtil.isCalendarDatatype(dt1) ? l1.calendarValue() : null;
        XMLGregorianCalendar c2 = XMLDatatypeUtil.isCalendarDatatype(dt2) ? l2.calendarValue() : null;
        Duration d1 = dt1.equals(XMLSchema.DURATION) ? datatypeFactory.newDuration(l1.getLabel()) : null;
        Duration d2 = dt2.equals(XMLSchema.DURATION) ? datatypeFactory.newDuration(l2.getLabel()) : null;
        if (op == MathOp.PLUS) {
            if (c1 != null && c2 != null) {
                throw new IllegalArgumentException("Cannot add 2 calendar literals:" + l1 + ":" + l2);
            } else if (c1 != null && d2 != null) {
                c1.add(d2);
                final BigdataLiteral str = vf.createLiteral(c1);
                return DummyConstantNode.toDummyIV(str);
            } else if (c2 != null && d1 != null) {
                c2.add(d1);
                final BigdataLiteral str = vf.createLiteral(c2);
                return DummyConstantNode.toDummyIV(str);
            } else if (d1 != null && d2 != null) {
                Duration result = d1.add(d2);
                final BigdataLiteral str = vf.createLiteral(result.toString(), XMLSchema.DURATION);
                return DummyConstantNode.toDummyIV(str);
            } else {
                throw new IllegalArgumentException("Cannot add process datatype literals:" + l1 + ":" + l2);
            }
        } else if (op == MathOp.MINUS) {
            if (c1 != null && c2 != null) {
                long milliseconds = c1.toGregorianCalendar().getTimeInMillis() - c2.toGregorianCalendar().getTimeInMillis();
                double days = ((double) milliseconds) / ((double) (1000 * 60 * 60 * 24));
                return new XSDNumericIV(days);
            } else if (d1 != null && d2 != null) {
                Duration result = d1.subtract(d2);
                final BigdataLiteral str = vf.createLiteral(result.toString(), XMLSchema.DURATION);
                return DummyConstantNode.toDummyIV(str);
            } else {
                throw new IllegalArgumentException("Cannot add process datatype literals:" + l1 + ":" + l2);
            }

        } else if (op == MathOp.MIN) {
            if (c1 != null && c2 != null) {
                int comp = c1.compare(c2);
                if (comp <= 0) {
                    return iv1;
                } else {
                    return iv2;
                }
            } else if (d1 != null && d2 != null) {
                int comp= d1.compare(d2);
                if(comp<=0){
                    return iv1;
                }else{
                    return iv2;
                }
            } else {
                throw new IllegalArgumentException("Cannot add process datatype literals:" + l1 + ":" + l2);
            }
        } else if (op == MathOp.MAX) {
            if (c1 != null && c2 != null) {
                int comp = c1.compare(c2);
                if (comp >= 0) {
                    return iv1;
                } else {
                    return iv2;
                }
            } else if (d1 != null && d2 != null) {
               int comp= d1.compare(d2);
               if(comp>=0){
                   return iv1;
               }else{
                   return iv2;
               }
            } else {
                throw new IllegalArgumentException("Cannot add process datatype literals:" + l1 + ":" + l2);
            }
        } else {
            throw new SparqlTypeErrorException();
        }
    }

}
