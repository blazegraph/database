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
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

public class StrdtBOp extends IVValueExpression<IV> implements INeedsMaterialization {

    private static final long serialVersionUID = -6571446625816081957L;

    public StrdtBOp(IValueExpression<? extends IV> x, IValueExpression<? extends IV> dt, 
    		final GlobalAnnotations globals) {
    	
        this(new BOp[] { x, dt }, anns(globals));
        
    }

    public StrdtBOp(BOp[] args, Map<String, Object> anns) {
        super(args, anns);
        if (args.length != 2 || args[0] == null || args[1] == null)
            throw new IllegalArgumentException();

    }

    public StrdtBOp(StrdtBOp op) {
        super(op);
    }

	@Override
	public Requirement getRequirement() {
		return Requirement.SOMETIMES;
	}

	@Override
    public IV get(final IBindingSet bs) throws SparqlTypeErrorException {
        
        final IV iv = getAndCheckLiteral(0, bs);

        final IV datatype = getAndCheckBound(1, bs);
        
        if (!datatype.isURI())
            throw new SparqlTypeErrorException();

        final BigdataURI dt = (BigdataURI) asValue(datatype);

        final Literal lit = asLiteral(iv);

      if (!QueryEvaluationUtil.isSimpleLiteral(lit)) {
            throw new SparqlTypeErrorException();
        }
        
        final String label = lit.getLabel();
        
        final BigdataLiteral str = getValueFactory().createLiteral(label, dt);
        
        return super.asIV(str, bs);

    }

}
