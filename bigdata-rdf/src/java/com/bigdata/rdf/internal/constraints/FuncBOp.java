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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.Function;
import org.openrdf.query.algebra.evaluation.function.FunctionRegistry;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * Call one of the Sesame casting functions.
 */
public class FuncBOp extends IVValueExpression<IV> 
		implements INeedsMaterialization {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2587499644967260639L;
	
	private static final transient Logger log = Logger.getLogger(FuncBOp.class);
	

	public interface Annotations extends BOp.Annotations {

		String NAMESPACE = (FuncBOp.class.getName() + ".namespace").intern();
		
		String FUNCTION = (FuncBOp.class.getName() + ".function").intern();

    }
	
    public FuncBOp(final IValueExpression<? extends IV>[] args, 
    		final String func, final String lex) {
        
        this(args, NV.asMap(
        		new NV(Annotations.NAMESPACE, lex),
        		new NV(Annotations.FUNCTION, func)));
        
    }
    
    /**
     * Required shallow copy constructor.
     */
    public FuncBOp(final BOp[] args, final Map<String, Object> anns) {

    	super(args, anns);
    	
		if (getProperty(Annotations.NAMESPACE) == null)
			throw new IllegalArgumentException();
		
		if (getProperty(Annotations.FUNCTION) == null)
			throw new IllegalArgumentException();
		
    }

    /**
     * Required deep copy constructor.
     */
    public FuncBOp(final FuncBOp op) {
        super(op);
    }

    public IV get(final IBindingSet bs) {
        
    	final List<BOp> args = args();
    	
    	final Value[] vals = new Value[args.size()];
    	
    	for (int i = 0; i < vals.length; i++) {

    		final IV iv = get(i).get(bs);
    		
            if (log.isDebugEnabled()) {
            	log.debug(iv);
            }

            // not yet bound
            if (iv == null)
            	throw new SparqlTypeErrorException();
            
            final BigdataValue val = iv.getValue();
            
            if (val == null)
            	throw new NotMaterializedException();
            
            vals[i] = val;
            
    	}
    	
    	final String funcName = 
    		(String) getRequiredProperty(Annotations.FUNCTION);
    	
    	final Function func = FunctionRegistry.getInstance().get(funcName);

		if (func == null) {
			throw new RuntimeException("Unknown function '" + funcName + "'");
		}
		
        final String namespace = (String)
	    	getRequiredProperty(Annotations.NAMESPACE);
	    
	    final BigdataValueFactory vf = 
	    	BigdataValueFactoryImpl.getInstance(namespace);
    
	    try {
	    
	    	final BigdataValue val = (BigdataValue) func.evaluate(vf, vals);
	    	
	    	IV iv = val.getIV();
	    	
	    	if (iv == null) {
	    		
	    		iv = TermId.mockIV(VTE.valueOf(val));
	    		
		    	val.setIV(iv);
		    	
	    	}
	    	
	    	// cache the value on the IV
	    	iv.setValue(val);
	    	
	    	return iv;
	    	
	    } catch (ValueExprEvaluationException ex) {
	    	
	    	throw new SparqlTypeErrorException();
	    	
	    }
        
    }
    
    /**
     * This bop can only work with materialized terms.  
     */
    public Requirement getRequirement() {
    	
    	return INeedsMaterialization.Requirement.ALWAYS;
    	
    }
    
    private volatile transient Set<IVariable<IV>> terms;
    
    public Set<IVariable<IV>> getTermsToMaterialize() {
    
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
