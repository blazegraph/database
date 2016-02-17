/*

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
/*
 * Created on November 30, 2015
 */
package com.bigdata.rdf.internal.constraints;

import java.math.BigInteger;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * Convert the {@link IV} to a <code>xsd:unsignedLong</code>.
 * Note that this is a non-standard extension.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class XsdUnsignedLongBOp extends IVValueExpression<IV>
		implements INeedsMaterialization {

    private static final long serialVersionUID = -8564789336767221003L;
    
    private static final transient Logger log = Logger.getLogger(XsdUnsignedLongBOp.class);

    private static BigInteger MIN_UNSIGNED_LONG = new BigInteger("0");
    private static BigInteger MAX_UNSIGNED_LONG = new BigInteger("18446744073709551615");
    
    
    public XsdUnsignedLongBOp(final IValueExpression<? extends IV> x, final GlobalAnnotations globals) {

        this(new BOp[] { x }, anns(globals));

    }

    /**
     * Required shallow copy constructor.
     */
    public XsdUnsignedLongBOp(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();

        if (getProperty(Annotations.NAMESPACE) == null)
            throw new IllegalArgumentException();

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public XsdUnsignedLongBOp(final XsdUnsignedLongBOp op) {
        super(op);
    }

    public IV get(final IBindingSet bs) {

        final IV iv = getAndCheckBound(0, bs);
        
        if (log.isDebugEnabled()) {
        	log.debug(iv);
        }
        
        final Value val = asValue(iv);

        if (log.isDebugEnabled()) {
        	log.debug(val);
        }
        
        // use to create my simple literals
        final BigdataValueFactory vf = getValueFactory();

        try {
            if (val instanceof Literal) {
            	final Literal lit = (Literal) val;
                if (lit.getDatatype() != null && lit.getDatatype().equals(XSD.UNSIGNED_LONG)) {
                    // if xsd:unsignedLong literal return it
                    return iv;
            	}
            	else {
            	    final BigInteger valAsBigInt = new BigInteger(lit.getLabel());
            	    if (valAsBigInt.compareTo(MIN_UNSIGNED_LONG)>=0 && valAsBigInt.compareTo(MAX_UNSIGNED_LONG)<=0) {
                        final BigdataLiteral str = 
                            vf.createLiteral(String.valueOf(valAsBigInt.toString()), XSD.UNSIGNED_LONG);
                        return super.asIV(str, bs);
            	    }
                }
            }
        } catch (Exception e) {
            // exception handling following
        }

        throw new SparqlTypeErrorException(); // fallback
    }

    /**
     * This bop can only work with materialized terms.
     */
    public Requirement getRequirement() {

        return INeedsMaterialization.Requirement.SOMETIMES;

    }

}
