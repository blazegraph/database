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

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * @see http://www.w3.org/2009/sparql/docs/query-1.1/rq25.xml#func-strafter
 */
public class StrAfterBOp extends IVValueExpression<IV> implements INeedsMaterialization {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2786852247821202390L;

	/**
     * @see http://www.w3.org/2009/sparql/docs/query-1.1/rq25.xml#func-strafter
     */
    @SuppressWarnings("rawtypes")
    public StrAfterBOp(//
            final IValueExpression<? extends IV> arg1,
            final IValueExpression<? extends IV> arg2,
            final GlobalAnnotations globals) {
     
        this(new BOp[] { arg1, arg2 }, anns(globals));
        
    }

    public StrAfterBOp(BOp[] args, Map<String, Object> anns) {

        super(args, anns);
        
        if (args.length < 2 || args[0] == null || args[1] == null)
            throw new IllegalArgumentException();

    }

    public StrAfterBOp(StrAfterBOp op) {
        super(op);
    }

	@Override
	public Requirement getRequirement() {
		return Requirement.SOMETIMES;
	}

	@Override
    @SuppressWarnings("rawtypes")
    public IV get(final IBindingSet bs) throws SparqlTypeErrorException {

        final Literal arg1 = getAndCheckLiteralValue(0, bs);

        final Literal arg2 = getAndCheckLiteralValue(1, bs);
        
        checkCompatibility(arg1, arg2);
        
        final String s2 = arg2.getLabel();
        
        if (s2.isEmpty()) {
        	
        	return ret(arg1, arg1.getLabel(), bs);
        	
        }
        
        final String s1 = arg1.getLabel();
        
        final int i = s1.indexOf(s2);
        
        // didn't find it
    	if (i < 0) {
        	
        	return ret(arg1, null, bs);
        	
        }

        // found it, but it's at the end
        if (i + s2.length() == s1.length()) {
        	
        	return ret(arg1, "", bs);
        	
        }
        
        final String val = s1.substring(i + s2.length());
        
        return ret(arg1, val, bs);

    }
    
    private IV ret(final Literal arg1, final String label, final IBindingSet bs) {
    	
        if (label == null) {
            
            final BigdataLiteral str = getValueFactory().createLiteral("");
            
            return super.asIV(str, bs);
            
        }

    	final String lang = arg1.getLanguage();
    	
    	if (lang != null) {
    		
            final BigdataLiteral str = getValueFactory().createLiteral(label, lang);

            return super.asIV(str, bs);
    		
    	}
    	
    	final URI dt = arg1.getDatatype();
    	
    	if (dt != null) {
    		
            final BigdataLiteral str = getValueFactory().createLiteral(label, dt);

            return super.asIV(str, bs);
    		
    	}
    	
        final BigdataLiteral str = getValueFactory().createLiteral(label);

        return super.asIV(str, bs);
    	
    }
    
    private void checkCompatibility(final Literal arg1, final Literal arg2)
    		throws SparqlTypeErrorException {

        if (!QueryEvaluationUtil.compatibleArguments(arg1, arg2)) {
            throw new SparqlTypeErrorException();
        }
	}

}
