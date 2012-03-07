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

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * @see http://www.w3.org/2005/xpath-functions#contains
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StrcontainsBOp extends LiteralBooleanBOp {

    /**
     * 
     */
    private static final long serialVersionUID = -8277059116677497632L;

    public StrcontainsBOp(//
            final IValueExpression<? extends IV> x,//
            final IValueExpression<? extends IV> y, //
            final String lex//
            ) {

        this(new BOp[] { x, y }, NV.asMap(new NV(Annotations.NAMESPACE, lex)));

    }

    public StrcontainsBOp(final BOp[] args, final Map<String, Object> anns) {
        
        super(args, anns);
        
        if (args.length != 2 || args[0] == null || args[1] == null)
            throw new IllegalArgumentException();
    
    }

    public StrcontainsBOp(LiteralBooleanBOp op) {
        super(op);
    }

    public Requirement getRequirement() {
        return Requirement.SOMETIMES;
    }

    @Override
    boolean _accept(final BigdataValueFactory vf, final IV value,
            final IBindingSet bs) throws SparqlTypeErrorException {

        final IV compare = get(1).get(bs);

        if (compare == null)
            throw new SparqlTypeErrorException.UnboundVarException();

        if (!compare.isInline() && !compare.hasValue())
            throw new NotMaterializedException();

        final String v = literalValue(value).getLabel();

        final String c = literalValue(compare).getLabel();

        return v.contains(c);

    }

}
