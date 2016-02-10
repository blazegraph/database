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
package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * Convert the {@link IV} to a <code>xsd:string</code>.
 */
public class XsdStrBOp extends IVValueExpression<IV>
		implements INeedsMaterialization {

    /**
	 *
	 */
    private static final long serialVersionUID = 3125106876006900339L;

    private static final transient Logger log = Logger.getLogger(XsdStrBOp.class);

    public XsdStrBOp(final IValueExpression<? extends IV> x, final GlobalAnnotations globals) {

        this(new BOp[] { x }, anns(globals));

    }

    /**
     * Required shallow copy constructor.
     */
    public XsdStrBOp(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();

        if (getProperty(Annotations.NAMESPACE) == null)
            throw new IllegalArgumentException();

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public XsdStrBOp(final XsdStrBOp op) {
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

        if (val instanceof Literal) {
        	final Literal lit = (Literal) val;
            if (lit.getDatatype() != null && lit.getDatatype().equals(XSD.STRING)) {
                // if xsd:string literal return it
                return iv;
        	}
        	else {
                // else return new xsd:string literal using Literal.getLabel
                final BigdataLiteral str = vf.createLiteral(lit.getLabel(), XSD.STRING);
                return super.asIV(str, bs);
            }
        } else if (val instanceof URI) {
            // return new simple literal using URI label
            final BigdataLiteral str = vf.createLiteral(val.stringValue(), XSD.STRING);
            return super.asIV(str, bs);
        } else {
            throw new SparqlTypeErrorException();
        }
        
//        final IV iv = getAndCheckBound(0, bs);
//
//        // use to create my simple literals
//        final BigdataValueFactory vf = getValueFactory();
//
//        if (iv.isInline() && !iv.isExtension()) {
//            if(iv.isLiteral()){
//                return super.asIV(vf.createLiteral(
//                        ((AbstractLiteralIV)iv).getLabel(), XSD.STRING), bs);
//            }else{
//                return super.asIV(vf.createLiteral(iv
//                        .getInlineValue().toString(), XSD.STRING), bs);
//            }
//        }
//
//        if (iv.isURI()) {
//            // return new xsd:string literal using URI label
//            final URI uri = (URI) iv.getValue();
//            final BigdataLiteral str = vf.createLiteral(uri.toString(), XSD.STRING);
//            return super.asIV(str, bs);
//        } else if (iv.isLiteral()) {
//            final BigdataLiteral lit = (BigdataLiteral) iv.getValue();
//            if (lit.getDatatype() != null && lit.getDatatype().equals(XSD.STRING)) {
//                // if xsd:string literal return it
//                return iv;
//        	}
//        	else {
//                // else return new xsd:string literal using Literal.getLabel
//                final BigdataLiteral str = vf.createLiteral(lit.getLabel(), XSD.STRING);
//                return super.asIV(str, bs);
//            }
//        } else {
//            throw new SparqlTypeErrorException();
//        }

    }

    /**
     * This bop can only work with materialized terms.
     */
    public Requirement getRequirement() {

        return INeedsMaterialization.Requirement.SOMETIMES;

    }

}
