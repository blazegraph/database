/*

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
import org.openrdf.model.URI;

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

//    public interface Annotations extends BOp.Annotations {
//
//        String NAMESPACE = XsdStrBOp.class.getName() + ".namespace";
//
//    }

    public XsdStrBOp(final IValueExpression<? extends IV> x, final String lex) {

        this(new BOp[] { x },
        		NV.asMap(new NV(Annotations.NAMESPACE, lex)));

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
     * Required deep copy constructor.
     */
    public XsdStrBOp(final XsdStrBOp op) {
        super(op);
    }

    public IV get(final IBindingSet bs) {

        final IV iv = get(0).get(bs);

        if (log.isDebugEnabled()) {
            log.debug(iv);
        }

        // not yet bound
        if (iv == null)
            throw new SparqlTypeErrorException();

        final String namespace = (String)
        	getRequiredProperty(Annotations.NAMESPACE);

        // use to create my simple literals
        final BigdataValueFactory vf =
        	BigdataValueFactoryImpl.getInstance(namespace);

        if (iv.isInline() && !iv.isExtension()) {
            if(iv.isLiteral()){
                return super.createIV(vf.createLiteral(
                        ((AbstractLiteralIV)iv).getLabel(), XSD.STRING), bs);
            }else{
                return super.createIV(vf.createLiteral(iv
                        .getInlineValue().toString(), XSD.STRING), bs);
            }
        }

        if (iv.isURI()) {
            // return new xsd:string literal using URI label
            final URI uri = (URI) iv.getValue();
            final BigdataLiteral str = vf.createLiteral(uri.toString(), XSD.STRING);
            return super.createIV(str, bs);
        } else if (iv.isLiteral()) {
            final BigdataLiteral lit = (BigdataLiteral) iv.getValue();
            if (lit.getDatatype() != null && lit.getDatatype().equals(XSD.STRING)) {
                // if xsd:string literal return it
                return iv;
        	}
        	else {
                // else return new xsd:string literal using Literal.getLabel
                final BigdataLiteral str = vf.createLiteral(lit.getLabel(), XSD.STRING);
                return super.createIV(str, bs);
            }
        } else {
            throw new SparqlTypeErrorException();
        }

    }

    /**
     * This bop can only work with materialized terms.
     */
    public Requirement getRequirement() {

        return INeedsMaterialization.Requirement.ALWAYS;

    }

}
