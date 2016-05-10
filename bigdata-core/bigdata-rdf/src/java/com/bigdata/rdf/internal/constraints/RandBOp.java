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
import java.util.Random;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

public class RandBOp extends IVValueExpression<IV> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    final private Random rand = new Random();

    @Override
    protected boolean areGlobalsRequired() {
     
        return false;
        
    }
    
    public RandBOp() {

        this(BOp.NOARGS, BOp.NOANNS);
        
    }

    public RandBOp(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

        if (args.length != 0)
            throw new IllegalArgumentException();

    }

    public RandBOp(final RandBOp op) {
        
        super(op);
        
    }

    public IV get(IBindingSet bindingSet) {

        return new XSDNumericIV(rand.nextDouble());
        
    }

}
