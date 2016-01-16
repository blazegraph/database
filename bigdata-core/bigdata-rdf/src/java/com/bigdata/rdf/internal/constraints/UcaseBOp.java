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

import java.util.Map;

import org.openrdf.model.Literal;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

public class UcaseBOp extends IVValueExpression<IV> implements INeedsMaterialization {

    private static final long serialVersionUID = 5894411703430694650L;

    public UcaseBOp(IValueExpression<? extends IV> x, final GlobalAnnotations globals) {
        super(x, globals);
    }

    public UcaseBOp(BOp[] args, Map<String, Object> anns) {
        super(args, anns);
        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();

    }

    public UcaseBOp(UcaseBOp op) {
        super(op);
    }

	@Override
	public Requirement getRequirement() {
		return Requirement.SOMETIMES;
	}

	@Override
    public IV get(final IBindingSet bs) throws SparqlTypeErrorException {
        final Literal lit = getAndCheckLiteralValue(0, bs);

        if (lit.getLanguage() != null) {
            final BigdataLiteral str = getValueFactory().createLiteral(lit.getLabel().toUpperCase(), lit.getLanguage());
            return super.asIV(str, bs);
        } else if (lit.getDatatype() != null) {
            final BigdataLiteral str = getValueFactory().createLiteral(lit.getLabel().toUpperCase(), lit.getDatatype());
            return super.asIV(str, bs);
        } else {
            final BigdataLiteral str = getValueFactory().createLiteral(lit.getLabel().toUpperCase());
            return super.asIV(str, bs);
        }
    }

}
