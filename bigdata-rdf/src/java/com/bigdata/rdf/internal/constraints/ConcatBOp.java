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
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;

public class ConcatBOp extends AbstractLiteralBOp<IV> {

    private static final long serialVersionUID = 5894411703430694650L;

    public ConcatBOp(final String lex, IValueExpression<? extends IV>... args) {

        super(args, NV.asMap(new NV(Annotations.NAMESPACE, lex)));
    }

    /**
     * Required shallow copy constructor.
     */
    public ConcatBOp(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

        if (args.length < 1 || args[0] == null)
            throw new IllegalArgumentException();

    }

    /**
     * Required deep copy constructor.
     */
    public ConcatBOp(final ConcatBOp op) {
        super(op);
    }

    @Override
    public IV _get(final IBindingSet bs) {
        URI datatype = null;
        String lang = null;
        boolean allSame = true;
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < arity(); i++) {
            @SuppressWarnings("rawtypes")
            final IV v = getAndCheckIfMaterializedLiteral(i, bs);
            String label = null;
            if (allSame) {
                final BigdataLiteral lit = literalValue(v);
                label = lit.getLabel();
                if (lit.getDatatype() != null) {
                    if (lang != null) {
                        allSame = false;
                    } else if (datatype == null) {
                        if (i == 0) {
                            datatype = lit.getDatatype();
                        } else {
                            allSame = false;
                        }
                    } else if (!datatype.equals(lit.getDatatype())) {
                        allSame = false;
                    }
                } else if (lit.getLanguage() != null) {
                    if (datatype != null) {
                        allSame = false;
                    } else if (lang == null) {
                        if (i == 0) {
                            lang = lit.getLanguage();
                        } else {
                            allSame = false;
                        }
                    } else if (!lang.equals(lit.getLanguage())) {
                        allSame = false;
                    }
                } else {
                    allSame = false;
                }
            } else {
                label = literalLabel(v);
            }
            sb.append(label);
        }
        if (allSame) {
            if (datatype != null) {
                return DummyConstantNode.toDummyIV(getValueFactory().createLiteral(sb.toString(),datatype));
            } else if (lang != null) {
                return DummyConstantNode.toDummyIV(getValueFactory().createLiteral(sb.toString(),lang));
            }
        }
        return DummyConstantNode.toDummyIV(getValueFactory().createLiteral(sb.toString()));

    }

}
