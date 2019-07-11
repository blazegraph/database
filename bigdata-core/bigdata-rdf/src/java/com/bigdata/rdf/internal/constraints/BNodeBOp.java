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
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * The BNODE()/BNODE(Literal) function as defined in <a
 * href="http://www.w3.org/TR/sparql11-query/#SparqlOps">SPARQL 1.1 Query
 * Language for RDF</a>.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class BNodeBOp extends IVValueExpression<IV> implements INeedsMaterialization {

    private static final long serialVersionUID = -8448763718374010166L;

    public BNodeBOp(final GlobalAnnotations globals) {
    	super(globals);
    }
    
    public BNodeBOp(final IValueExpression<? extends IV> x, 
    		final GlobalAnnotations globals) {
        this(new BOp[] { x }, anns(globals));
    }

    public BNodeBOp(BOp[] args, Map<String, Object> anns) {
        super(args, anns);
        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();
    }

    public BNodeBOp(BNodeBOp op) {
        super(op);
    }

    public Requirement getRequirement() {
        return Requirement.SOMETIMES;
    }

    public IV get(final IBindingSet bs) throws SparqlTypeErrorException {
        
    	if (arity() == 0) {
    		
    		return super.asIV(getValueFactory().createBNode(), bs);
    		
    	}
    	
        final Literal lit = getAndCheckLiteralValue(0, bs);

        if (!QueryEvaluationUtil.isStringLiteral(lit))
            throw new SparqlTypeErrorException();

        final BigdataBNode bnode = getValueFactory().createBNode(
                "-bnode-func-" + lit.getLabel());

        return super.asIV(bnode, bs);

    }
    
}
