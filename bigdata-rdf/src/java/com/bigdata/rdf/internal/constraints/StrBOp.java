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

import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.StrIV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Imposes the constraint <code>isURI(x)</code>.
 */
public class StrBOp extends IVValueExpression<IV> {

    /**
	 * 
	 */
	private static final long serialVersionUID = 3125106876006900339L;

    public StrBOp(final IVariable<IV> x) {
        
        this(new BOp[] { x }, null/*annocations*/);
        
    }
    
    /**
     * Required shallow copy constructor.
     */
    public StrBOp(final BOp[] args, final Map<String, Object> anns) {

    	super(args, anns);
    	
        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();

    }

    /**
     * Required deep copy constructor.
     */
    public StrBOp(final StrBOp op) {
        super(op);
    }

    public IV get(final IBindingSet bs) {
        
        final IV iv = get(0).get(bs);
        
        // not yet bound
        if (iv == null)
        	throw new SparqlTypeErrorException();

        // uh oh how the heck do I get my hands on this? big change
        final AbstractTripleStore db = null;
        
        // use to materialize my terms
        final LexiconRelation lex = db.getLexiconRelation();
        
        // use to create my simple literals
        final BigdataValueFactory vf = db.getValueFactory();
        
        if (iv.isURI()) {
        	// return new simple literal using URI label
        	final URI uri = (URI) iv.asValue(lex);
        	final BigdataLiteral str = vf.createLiteral(uri.toString());
        	return new StrIV(iv, str);
        } else if (iv.isLiteral()) {
        	final BigdataLiteral lit = (BigdataLiteral) iv.asValue(lex);
        	if (lit.getDatatype() == null && lit.getLanguage() == null) {
            	// if simple literal return it
        		return iv;
        	}
        	else {
            	// else return new simple literal using Literal.getLabel
            	final BigdataLiteral str = vf.createLiteral(lit.getLabel());
            	return new StrIV(iv, str);
        	}
        } else {
        	throw new SparqlTypeErrorException();
        }
        
    }
    
}
