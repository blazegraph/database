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
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization.Requirement;

public class RegexBOp extends XSDBooleanIVValueExpression 
		implements INeedsMaterialization {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1357420268214930143L;
	
	private static final transient Logger log = Logger.getLogger(RegexBOp.class);
	

	/**
	 * Construct a regex bop without flags.
	 */
	public RegexBOp(
			final IValueExpression<? extends IV> var, 
			final IValueExpression<? extends IV> pattern) {
        
        this(new BOp[] { var, pattern }, null/*annocations*/);
        
    }
    
	/**
	 * Construct a regex bop with flags.
	 */
	public RegexBOp(
			final IValueExpression<? extends IV> var, 
			final IValueExpression<? extends IV> pattern,
			final IValueExpression<? extends IV> flags) {
        
        this(new BOp[] { var, pattern, flags }, null/*annocations*/);
        
    }
    
    /**
     * Required shallow copy constructor.
     */
    public RegexBOp(final BOp[] args, final Map<String, Object> anns) {

    	super(args, anns);
    	
        if (args.length < 2 || args[0] == null || args[1] == null)
            throw new IllegalArgumentException();

    }

    /**
     * Required deep copy constructor.
     */
    public RegexBOp(final RegexBOp op) {
        super(op);
    }
    
    /**
     * This bop can only work with materialized terms.  
     */
    public Requirement getRequirement() {
    	
    	return INeedsMaterialization.Requirement.ALWAYS;
    	
    }
    
    public boolean accept(final IBindingSet bs) {
        
        final IV var = get(0).get(bs);
        final IV pattern = get(1).get(bs);
        final IV flags = arity() > 2 ? get(2).get(bs) : null;
        
        if (log.isDebugEnabled()) {
        	log.debug("regex var: " + var);
        	log.debug("regex pattern: " + pattern);
        	log.debug("regex flags: " + flags);
        }
        
        // not yet bound
        if (var == null || pattern == null)
        	throw new SparqlTypeErrorException();
        
        return accept(var.getValue(), pattern.getValue(), 
        		flags != null ? flags.getValue() : null);

    }
    
    /**
     * Lifted directly from Sesame's EvaluationStrategyImpl.
     */
    private boolean accept(final Value arg, final Value parg, final Value farg) {
    	
        if (log.isDebugEnabled()) {
        	log.debug("regex var: " + arg);
        	log.debug("regex pattern: " + parg);
        	log.debug("regex flags: " + farg);
        }
        
		if (QueryEvaluationUtil.isSimpleLiteral(arg) && QueryEvaluationUtil.isSimpleLiteral(parg)
				&& (farg == null || QueryEvaluationUtil.isSimpleLiteral(farg)))
		{
			String text = ((Literal)arg).getLabel();
			String ptn = ((Literal)parg).getLabel();
			String flags = "";
			if (farg != null) {
				flags = ((Literal)farg).getLabel();
			}
			// TODO should this Pattern be cached?
			int f = 0;
			for (char c : flags.toCharArray()) {
				switch (c) {
					case 's':
						f |= Pattern.DOTALL;
						break;
					case 'm':
						f |= Pattern.MULTILINE;
						break;
					case 'i':
						f |= Pattern.CASE_INSENSITIVE;
						break;
					case 'x':
						f |= Pattern.COMMENTS;
						break;
					case 'd':
						f |= Pattern.UNIX_LINES;
						break;
					case 'u':
						f |= Pattern.UNICODE_CASE;
						break;
					default:
						throw new SparqlTypeErrorException();
				}
			}
			Pattern pattern = Pattern.compile(ptn, f);
			boolean result = pattern.matcher(text).find();
			return result;
		}
		
		throw new SparqlTypeErrorException();
    	
    }
    
}
