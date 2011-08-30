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

import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization.Requirement;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;

public class StrdtBOp extends AbstractLiteralBOp {

    private static final long serialVersionUID = -6571446625816081957L;

    public StrdtBOp(IValueExpression<? extends IV> x, IValueExpression<? extends IV> dt, String lex) {
        this(new BOp[] { x, dt }, NV.asMap(new NV(Annotations.NAMESPACE, lex)));
    }

    public StrdtBOp(BOp[] args, Map<String, Object> anns) {
        super(args, anns);
        if (args.length != 2 || args[0] == null || args[1] == null)
            throw new IllegalArgumentException();

    }

    public StrdtBOp(StrdtBOp op) {
        super(op);
    }

    public Requirement getRequirement() {
        return Requirement.SOMETIMES;
    }

    public IV _get(final IBindingSet bs) throws SparqlTypeErrorException {
        IV iv = getAndCheck(0, bs);

        final IV datatype = get(1).get(bs);
        if (datatype == null) {
            throw new SparqlTypeErrorException.UnboundVarException();
        }
        if (!datatype.isURI())
            throw new SparqlTypeErrorException();

        if (!datatype.isInline() && !datatype.hasValue())
            throw new NotMaterializedException();

        final BigdataURI dt = (BigdataURI) datatype.getValue();
        final BigdataLiteral lit = literalValue(iv);
        String label = lit.getLabel();
        final BigdataLiteral str = vf.createLiteral(label, dt);
        return DummyConstantNode.toDummyIV(str);

    }

}
