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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * Return the datatype of the literal argument.
 */
public class DatatypeBOp extends IVValueExpression<IV> 
		implements INeedsMaterialization {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7391999162162545704L;
	
	private static final transient Logger log = Logger.getLogger(DatatypeBOp.class);
	

	public interface Annotations extends BOp.Annotations {

		String NAMESPACE = (DatatypeBOp.class.getName() + ".namespace").intern();

    }
	
    public DatatypeBOp(final IValueExpression<? extends IV> x, final String lex) {
        
        this(new BOp[] { x }, 
        		NV.asMap(new NV(Annotations.NAMESPACE, lex)));
        
    }
    
    /**
     * Required shallow copy constructor.
     */
    public DatatypeBOp(final BOp[] args, final Map<String, Object> anns) {

    	super(args, anns);
    	
        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();

		if (getProperty(Annotations.NAMESPACE) == null)
			throw new IllegalArgumentException();
		
    }

    /**
     * Required deep copy constructor.
     */
    public DatatypeBOp(final DatatypeBOp op) {
        super(op);
    }

    public IV get(final IBindingSet bs) {
     
        final String namespace = (String)
			getRequiredProperty(Annotations.NAMESPACE);
	
	    final BigdataValueFactory vf = 
	    	BigdataValueFactoryImpl.getInstance(namespace);
    	
        final IV iv = get(0).get(bs);
        
        if (log.isDebugEnabled()) {
        	log.debug(iv);
        }

        // not yet bound
        if (iv == null)
        	throw new SparqlTypeErrorException();
        
        if (iv.isNumeric()) {
        	
//            final BigdataURI datatype = vf.createURI(iv.getDTE().getDatatype());
            final BigdataURI datatype = vf.asValue(iv.getDTE().getDatatypeURI());
        	
	    	IV datatypeIV = datatype.getIV();
	    	
	    	if (datatypeIV == null) {
	    		
                datatypeIV = TermId.mockIV(VTE.URI);
	    		datatype.setIV(datatypeIV);
		    	
	    	}
	    	
	    	// cache the value on the IV
	    	datatypeIV.setValue(datatype);
	    	
	    	return datatypeIV;
        	
        }
        
        final BigdataValue val = iv.getValue();
        
        if (val == null)
        	throw new NotMaterializedException();
        
        if (val instanceof BigdataLiteral) {
        	
        	final BigdataLiteral literal = (BigdataLiteral) val;
        	
        	final BigdataURI datatype;
        	
			if (literal.getDatatype() != null) {
				
				// literal with datatype
				datatype = literal.getDatatype();
				
			} else if (literal.getLanguage() == null) {
				
				// simple literal
				datatype = vf.asValue(XSD.STRING);
				
			} else {
				
				throw new SparqlTypeErrorException();
				
			}
			
	    	IV datatypeIV = datatype.getIV();
	    	
	    	if (datatypeIV == null) {
	    		
	    	    datatypeIV = TermId.mockIV(VTE.URI);
	    		
	    	    datatype.setIV(datatypeIV);
		    	
	    	}
	    	
	    	// cache the value on the IV
	    	datatypeIV.setValue(datatype);
	    	
	    	return datatypeIV;
        	
        }
        
        throw new SparqlTypeErrorException();
        
    }
    
    /**
     * The DatatypeBOp can evaluate against unmaterialized inline numerics.  
     */
    public Requirement getRequirement() {
    	
    	return INeedsMaterialization.Requirement.SOMETIMES;
    	
    }
    
    private volatile transient Set<IVariable<IV>> terms;
    
    public Set<IVariable<IV>> getVarsToMaterialize() {
    
    	if (terms == null) {
    		
    		terms = new LinkedHashSet<IVariable<IV>>();
    		
    		for (BOp bop : args()) {
    			
    			if (bop instanceof IVariable)
    				terms.add((IVariable<IV>) bop);
    		
    		}
    		
    	}
    	
    	return terms;
    	
    }
    
}
