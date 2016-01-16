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

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

public class StrstartsBOp extends XSDBooleanIVValueExpression implements INeedsMaterialization {

    private static final long serialVersionUID = 7680098071087046784L;

    @SuppressWarnings("rawtypes")
    public StrstartsBOp(//
            final IValueExpression<? extends IV> x,//
            final IValueExpression<? extends IV> y//
            ) {

        this(new BOp[] { x, y }, BOp.NOANNS);
        
    }

    public StrstartsBOp(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);
        
        if (args.length != 2 || args[0] == null || args[1] == null)
            throw new IllegalArgumentException();
        
    }

    public StrstartsBOp(StrstartsBOp op) {
        super(op);
    }

    @Override
    public Requirement getRequirement() {
        return Requirement.SOMETIMES;
    }
    
    @Override
    public boolean accept(final IBindingSet bs) throws SparqlTypeErrorException {

        final String v = getAndCheckLiteralValue(0, bs).getLabel();
        final String c = getAndCheckLiteralValue(1, bs).getLabel();
        return v.startsWith(c);

    }

}
