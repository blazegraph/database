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
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Implements the <a
 * href="http://www.w3.org/TR/sparql11-query/#func-langMatches" >langMatches</a>
 * SPARQL operator.
 * 
 * @see http://www.w3.org/TR/sparql11-query/#func-langMatches
 * @see http://www.ietf.org/rfc/rfc4647.txt
 */
public class LangMatchesBOp extends XSDBooleanIVValueExpression 
		implements INeedsMaterialization {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5910711647357240974L;
	
    /**
     * 
     * @param tag
     *            The language tag.
     * @param range
     *            The language range (allows "*", a language range such as "EN"
     *            or "DE", or an extended language range such as "de-DE" or
     *            "de-Latn-DE").
     * 
     * @see http://www.ietf.org/rfc/rfc4647.txt
     */
    @SuppressWarnings("rawtypes")
    public LangMatchesBOp(
    		final IValueExpression<? extends IV> tag, 
    		final IValueExpression<? extends IV> range) { 
        
        this(new BOp[] { tag, range }, BOp.NOANNS);
        
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
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public LangMatchesBOp(final LangMatchesBOp op) {
        super(op);
    }

    @Override
    protected boolean accept(final IBindingSet bs) {
        
        final IV<?, ?> tag = get(0).get(bs);

        // not yet bound
        if (tag == null)
            throw new SparqlTypeErrorException();

        final IV<?, ?> range = get(1).get(bs);

        // not yet bound
        if (range == null)
            throw new SparqlTypeErrorException();

//        if (log.isDebugEnabled()) {
//        	log.debug(tag);
//        	log.debug(range);
//        }


        final BigdataValue tagVal = tag.getValue();

        // not yet materialized
        if (tagVal == null)
            throw new NotMaterializedException();

        if (!QueryEvaluationUtil.isSimpleLiteral(tagVal))
            throw new SparqlTypeErrorException();

        final BigdataValue rangeVal = range.getValue();

        // not yet materialized
        if (rangeVal == null)
            throw new NotMaterializedException();

        if (!QueryEvaluationUtil.isSimpleLiteral(rangeVal))
            throw new SparqlTypeErrorException();

//        if (log.isDebugEnabled()) {
//        	log.debug(tagVal);
//        	log.debug(rangeVal);
//        }

        final String langTag = ((Literal) tagVal).getLabel();
        final String langRange = ((Literal) rangeVal).getLabel();

        boolean result = false;

        if (langRange.equals("*")) {
        
            // Note: Must have a language tag to match.
            result = langTag.length() > 0;
            
        } else if (langTag.length() == langRange.length()) {
            
            // Same length, same characters (case insensitive).
            result = langTag.equalsIgnoreCase(langRange);
            
        } else if (langTag.length() > langRange.length()) {
            
            /*
             * Check if the range is a prefix of the tag. If the range is longer
             * the match must terminate on a "-" boundary in the language range.
             */

            final String prefix = langTag.substring(0, langRange.length());

            result = prefix.equalsIgnoreCase(langRange)
                    && langTag.charAt(langRange.length()) == '-';

        }

        return result;

    }
    
    /**
     * This bop can only work with materialized terms.  
     */
    @Override
    public Requirement getRequirement() {

        return INeedsMaterialization.Requirement.ALWAYS;

    }

}
