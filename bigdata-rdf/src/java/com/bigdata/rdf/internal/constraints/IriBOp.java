/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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

import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * The IRI function, as defined in <a
 * href="http://www.w3.org/TR/sparql11-query/#SparqlOps">SPARQL 1.1 Query
 * Language for RDF</a>.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class IriBOp extends IVValueExpression<IV> implements INeedsMaterialization {

    private static final long serialVersionUID = -8448763718374010166L;

    public IriBOp(final IValueExpression<? extends IV> x, 
    		final GlobalAnnotations globals) {
        super(x, globals);
    }

    public IriBOp(BOp[] args, Map<String, Object> anns) {
        super(args, anns);
        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();
    }

    public IriBOp(IriBOp op) {
        super(op);
    }

	@Override
	public Requirement getRequirement() {
		return Requirement.SOMETIMES;
	}

	@Override
    public IV get(final IBindingSet bs) throws SparqlTypeErrorException {
        
    	final IV iv = getAndCheckBound(0, bs);

        if (iv.isURI()) {
        	return iv;
        }

        if (!iv.isLiteral())
            throw new SparqlTypeErrorException();

        final Literal lit = literalValue(iv);

        final URI dt = lit.getDatatype();

        if (dt != null && !dt.stringValue().equals(XSD.STRING.stringValue()))
            throw new SparqlTypeErrorException();

        final BigdataURI uri = getValueFactory().createURI(lit.getLabel());

        return super.getOrCreateIV(uri, bs);

    }

}
