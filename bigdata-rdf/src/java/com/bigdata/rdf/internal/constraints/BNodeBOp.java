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
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;

/**
 * The BNODE()/BNODE(Literal) function as defined in <a
 * href="http://www.w3.org/TR/sparql11-query/#SparqlOps">SPARQL 1.1 Query
 * Language for RDF</a>.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class BNodeBOp extends AbstractLiteralBOp {

    private static final long serialVersionUID = -8448763718374010166L;

    public BNodeBOp(final String lex) {
    	super(lex);
    }
    
    public BNodeBOp(IValueExpression<? extends IV> x, String lex) {
        super(x, lex);
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

    public IV _get(final IBindingSet bs) throws SparqlTypeErrorException {
        
    	if (arity() == 0) {
    		
    		return DummyConstantNode.toDummyIV(getValueFactory().createBNode());
    		
    	}
    	
    	IV iv = get(0).get(bs);
    	
        if (iv == null)
            throw new SparqlTypeErrorException.UnboundVarException();

        if (!iv.isLiteral())
        	throw new SparqlTypeErrorException();
        
        final BigdataLiteral lit = literalValue(iv);
        
        final BigdataURI dt = lit.getDatatype();
        
        if (dt != null && !dt.stringValue().equals(XSD.STRING.stringValue()))
            throw new SparqlTypeErrorException();
        	
        final BigdataBNode bnode = getValueFactory().createBNode("-bnode-func-"+lit.getLabel());
            
        return DummyConstantNode.toDummyIV(bnode);
            
    }
    
}
