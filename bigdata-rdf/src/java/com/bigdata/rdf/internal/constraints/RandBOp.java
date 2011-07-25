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
import java.util.Random;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.WrappedIV;
import com.bigdata.rdf.internal.XSDDoubleIV;
import com.bigdata.rdf.model.BigdataValueFactory;

public class RandBOp extends IVValueExpression<IV> {
    Random rand = new Random();

    public RandBOp() {
        this(new BOp[] {}, null);
    }

    public RandBOp(BOp[] args, Map<String, Object> anns) {
        super(args, anns);
        if (args.length != 0)
            throw new IllegalArgumentException();

    }

    public RandBOp(RandBOp op) {
        super(op);
    }

    public IV get(IBindingSet bindingSet) {
        return new XSDDoubleIV(rand.nextDouble());
    }

}
