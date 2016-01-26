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
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * @see http://www.w3.org/2009/sparql/docs/query-1.1/rq25.xml#func-replace
 */
public class ReplaceBOp extends IVValueExpression<IV> implements INeedsMaterialization {

	private static final transient Logger log = Logger.getLogger(ReplaceBOp.class);

    public interface Annotations extends XSDBooleanIVValueExpression.Annotations {
    	
        /**
         * The cached regex pattern.
         */
        public String PATTERN = ReplaceBOp.class.getName()
                + ".pattern";
        
    }

    private static Map<String,Object> anns(
			final IValueExpression<? extends IV> pattern,
			final IValueExpression<? extends IV> flags,
			final GlobalAnnotations globals) {
    	
    	if (pattern instanceof IConstant && 
    			(flags == null || flags instanceof IConstant)) {
    		
    		final IV parg = ((IConstant<IV>) pattern).get();
    		
    		final IV farg = flags != null ?
    				((IConstant<IV>) flags).get() : null;
    				
			if (parg.hasValue() && (farg == null || farg.hasValue())) {
    		
				final Value pargVal = parg.getValue();
				
				final Value fargVal = farg != null ? farg.getValue() : null;
				
	    		return anns(globals,
	    				new NV(Annotations.PATTERN, 
	    						getPattern(pargVal, fargVal)));
	    		
			}
    		
    	}
    		
		return anns(globals);
    	
    }
    
	/**
	 * Construct a replace bop without flags.
	 */
    @SuppressWarnings("rawtypes")
	public ReplaceBOp(
			final IValueExpression<? extends IV> var, 
			final IValueExpression<? extends IV> pattern,
			final IValueExpression<? extends IV> replacement,
			final GlobalAnnotations globals) {
        
        this(new BOp[] { var, pattern, replacement }, anns(pattern, null, globals));

    }
    
	/**
	 * Construct a replace bop with flags.
	 */
    @SuppressWarnings("rawtypes")
	public ReplaceBOp(
			final IValueExpression<? extends IV> var, 
			final IValueExpression<? extends IV> pattern,
			final IValueExpression<? extends IV> replacement,
			final IValueExpression<? extends IV> flags,
			final GlobalAnnotations globals) {
        
        this(new BOp[] { var, pattern, replacement, flags }, anns(pattern, flags, globals));

    }
    
    /**
     * Required shallow copy constructor.
     */
    public ReplaceBOp(final BOp[] args, final Map<String, Object> anns) {

    	super(args, anns);
    	
        if (args.length < 2 || args[0] == null || args[1] == null)
            throw new IllegalArgumentException();

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public ReplaceBOp(final ReplaceBOp op) {
        super(op);
    }
    
	@Override
	public Requirement getRequirement() {
		return Requirement.SOMETIMES;
	}

	@Override
    @SuppressWarnings("rawtypes")
    public IV get(final IBindingSet bs) {
        
        @SuppressWarnings("rawtypes")
        final Literal var = getAndCheckLiteralValue(0, bs);
        
        @SuppressWarnings("rawtypes")
        final Literal pattern = getAndCheckLiteralValue(1, bs);

        @SuppressWarnings("rawtypes")
        final Literal replacement = getAndCheckLiteralValue(2, bs);
        
        @SuppressWarnings("rawtypes")
        final Literal flags = arity() > 3 ? getAndCheckLiteralValue(3, bs) : null;
        
        if (log.isDebugEnabled()) {
        	log.debug("var: " + var);
        	log.debug("pattern: " + pattern);
        	log.debug("replacement: " + replacement);
        	log.debug("flags: " + flags);
        }
        
        try {

        	final BigdataLiteral l = 
        		evaluate(getValueFactory(), var, pattern, replacement, flags);
        	
        	return super.asIV(l, bs);
        	
        } catch (ValueExprEvaluationException ex) {
        	
        	throw new SparqlTypeErrorException();
        	
        }

    }
    
    /**
     * Lifted directly from Sesame's Replace operator.
     * 
     * FIXME The Pattern should be cached if the pattern argument and flags are
     * constants.
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/516">
     *      REGEXBOp should cache the Pattern when it is a constant </a>
     */
	public BigdataLiteral evaluate(final BigdataValueFactory valueFactory, final Value... args)
			throws ValueExprEvaluationException {
		if (args.length < 3 || args.length > 4) {
			throw new ValueExprEvaluationException(
					"Incorrect number of arguments for REPLACE: " + args.length);
		}

		try {
			Literal arg = (Literal) args[0];
			Literal pattern = (Literal) args[1];
			Literal replacement = (Literal) args[2];
			Literal flags = null;
			if (args.length == 4) {
				flags = (Literal) args[3];
			}

			if (!QueryEvaluationUtil.isStringLiteral(arg)) {
				throw new ValueExprEvaluationException(
						"incompatible operand for REPLACE: " + arg);
			}

			if (!QueryEvaluationUtil.isSimpleLiteral(replacement)) {
				throw new ValueExprEvaluationException(
						"incompatible operand for REPLACE: " + replacement);
			}

			String argString = arg.getLabel();
			String replacementString = replacement.getLabel();

			Pattern p = (Pattern) getProperty(Annotations.PATTERN);
			if (p == null) {
				p = getPattern(pattern, flags);
			}
			
			String result = p.matcher(argString).replaceAll(replacementString);

			String lang = arg.getLanguage();
			URI dt = arg.getDatatype();

			if (lang != null) {
				return valueFactory.createLiteral(result, lang);
			} else if (dt != null) {
				return valueFactory.createLiteral(result, dt);
			} else {
				return valueFactory.createLiteral(result);
			}
		} catch (ClassCastException e) {
			throw new ValueExprEvaluationException("literal operands expected",
					e);
		}

	}    
    
    private static Pattern getPattern(final Value pattern, final Value flags) 
			throws IllegalArgumentException {
		
		if (!QueryEvaluationUtil.isSimpleLiteral(pattern)) {
			throw new IllegalArgumentException(
					"incompatible operand for REPLACE: " + pattern);
		}

		String flagString = null;
		if (flags != null) {
			if (!QueryEvaluationUtil.isSimpleLiteral(flags)) {
				throw new IllegalArgumentException(
						"incompatible operand for REPLACE: " + flags);
			}
			flagString = ((Literal) flags).getLabel();
		}

		String patternString = ((Literal) pattern).getLabel();

		int f = 0;
		if (flagString != null) {
			for (char c : flagString.toCharArray()) {
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
					throw new IllegalArgumentException(flagString);
				}
			}
		}

		Pattern p = Pattern.compile(patternString, f);
		
		return p;
		
	}
	
}
