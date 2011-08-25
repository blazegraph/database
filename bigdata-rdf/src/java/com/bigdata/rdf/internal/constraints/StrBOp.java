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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;

/**
 * Convert the {@link IV} to a <code>xsd:string</code>.
 */
public class StrBOp extends IVValueExpression<IV> 
		implements INeedsMaterialization {

    /**
	 *
	 */
    private static final long serialVersionUID = 3125106876006900339L;

    private static final transient Logger log = Logger.getLogger(StrBOp.class);

    public interface Annotations extends BOp.Annotations {

        String NAMESPACE = StrBOp.class.getName() + ".namespace";

    }

    public StrBOp(final IValueExpression<? extends IV> x, final String lex) {

        this(new BOp[] { x }, 
        		NV.asMap(new NV(Annotations.NAMESPACE, lex)));

    }

    /**
     * Required shallow copy constructor.
     */
    public StrBOp(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();

        if (getProperty(Annotations.NAMESPACE) == null)
            throw new IllegalArgumentException();

    }

    /**
     * Required deep copy constructor.
     */
    public StrBOp(final StrBOp op) {
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
            return DummyConstantNode.dummyIV(vf.createLiteral(iv
                    .getInlineValue().toString()));
        }

        if (iv.isURI()) {
            // return new simple literal using URI label
            final URI uri = (URI) iv.getValue();
            final BigdataLiteral str = vf.createLiteral(uri.toString());
            return DummyConstantNode.dummyIV(str);
        } else if (iv.isLiteral()) {
            final BigdataLiteral lit = (BigdataLiteral) iv.getValue();
            if (lit.getDatatype() == null && lit.getLanguage() == null) {
                // if simple literal return it
                return iv;
        	}
        	else {
                // else return new simple literal using Literal.getLabel
                final BigdataLiteral str = vf.createLiteral(lit.getLabel());
                return DummyConstantNode.dummyIV(str);
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

    private volatile transient Set<IVariable<IV>> terms;

    public Set<IVariable<IV>> getVarsToMaterialize() {

        if (terms == null) {

            terms = new LinkedHashSet<IVariable<IV>>(1);
            if (get(0) instanceof IVariable) {

                terms.add((IVariable<IV>) get(0));

            }

        }

        return terms;

    }

}
