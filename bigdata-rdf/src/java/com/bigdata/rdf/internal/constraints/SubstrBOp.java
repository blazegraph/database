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

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;

public class SubstrBOp extends AbstractLiteralBOp {

    private static final long serialVersionUID = -7022953617164154412L;

    public SubstrBOp(IValueExpression<? extends IV> x, IValueExpression<? extends IV> start, IValueExpression<? extends IV> length, String lex) {
        this(new BOp[] { x, start, length }, NV.asMap(new NV(Annotations.NAMESPACE, lex)));
    }

    public SubstrBOp(BOp[] args, Map<String, Object> anns) {
        super(args, anns);
        if (args.length < 2 || args[0] == null || args[1] == null)
            throw new IllegalArgumentException();

    }

    public SubstrBOp(SubstrBOp op) {
        super(op);
    }

    public Requirement getRequirement() {
        return Requirement.SOMETIMES;
    }

    public IV _get(final IBindingSet bs) throws SparqlTypeErrorException {
        IV iv = getAndCheck(0, bs);
          IV start = get(1).get(bs);
        if (!start.isFixedNumeric())
            throw new SparqlTypeErrorException();
        IV length = null;
        if (arity() > 2) {
            length = get(2).get(bs);
            if (!length.isFixedNumeric())
                throw new SparqlTypeErrorException();
        }

        final BigdataLiteral lit = literalValue(iv);
        String lang = lit.getLanguage();
        BigdataURI dt = lit.getDatatype();
        String label = lit.getLabel();

        int s = literalValue(start).intValue();
        if (length != null) {
            int l = literalValue(length).intValue();
            label = label.substring(s, l);
        } else {
            label = label.substring(s);
        }
        if (lang != null) {
            // else return new simple literal using Literal.getLabel
            final BigdataLiteral str = vf.createLiteral(label, lang);
            return DummyConstantNode.dummyIV(str);
        } else if (dt != null) {
            // else return new simple literal using Literal.getLabel
            final BigdataLiteral str = vf.createLiteral(label, dt);
            return DummyConstantNode.dummyIV(str);
        }else{
            final BigdataLiteral str = vf.createLiteral(label);
            return DummyConstantNode.dummyIV(str);
        }
    }

}
