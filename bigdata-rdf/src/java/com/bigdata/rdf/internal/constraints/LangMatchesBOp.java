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
import org.openrdf.model.Literal;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization.Requirement;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Implements the langMatches SPARQL operator.
 */
public class LangMatchesBOp extends XSDBooleanIVValueExpression 
		implements INeedsMaterialization {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5910711647357240974L;
	
	private static final transient Logger log = Logger.getLogger(LangMatchesBOp.class);
	
	
    public LangMatchesBOp(final IValueExpression<? extends IV> tag, 
    		final IValueExpression<? extends IV> range) {
        
        this(new BOp[] { tag, range }, null/*annocations*/);
        
        if (log.isDebugEnabled()) {
        	log.info(tag);
        	log.info(range);
        }
        
    }
    
    /**
     * Required shallow copy constructor.
     */
    public LangMatchesBOp(final BOp[] args, final Map<String, Object> anns) {

    	super(args, anns);
    	
        if (args.length != 2 || args[0] == null || args[1] == null)
            throw new IllegalArgumentException();

    }

    /**
     * Required deep copy constructor.
     */
    public LangMatchesBOp(final LangMatchesBOp op) {
        super(op);
    }

    protected boolean accept(final IBindingSet bs) {
        
        final IV tag = get(0).get(bs);
        final IV range = get(1).get(bs);
        
        if (log.isDebugEnabled()) {
        	log.debug(tag);
        	log.debug(range);
        }

        // not yet bound
        if (tag == null || range == null)
        	throw new SparqlTypeErrorException();

        final BigdataValue tagVal = tag.getValue();
        final BigdataValue rangeVal = range.getValue();
        
        if (log.isDebugEnabled()) {
        	log.debug(tagVal);
        	log.debug(rangeVal);
        }

        // not yet materialized
        if (tagVal == null || rangeVal == null)
        	throw new NotMaterializedException();
        
		if (QueryEvaluationUtil.isSimpleLiteral(tagVal)
				&& QueryEvaluationUtil.isSimpleLiteral(rangeVal))
		{
			String langTag = ((Literal)tagVal).getLabel();
			String langRange = ((Literal)rangeVal).getLabel();

			boolean result = false;
			if (langRange.equals("*")) {
				result = langTag.length() > 0;
			}
			else if (langTag.length() == langRange.length()) {
				result = langTag.equalsIgnoreCase(langRange);
			}
			else if (langTag.length() > langRange.length()) {
				// check if the range is a prefix of the tag
				String prefix = langTag.substring(0, langRange.length());
				result = prefix.equalsIgnoreCase(langRange) && langTag.charAt(langRange.length()) == '-';
			}

			return result;
		}

		throw new SparqlTypeErrorException();
		
    }
    
    /**
     * This bop can only work with materialized terms.  
     */
    public Requirement getRequirement() {
    	
    	return INeedsMaterialization.Requirement.ALWAYS;
    	
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
