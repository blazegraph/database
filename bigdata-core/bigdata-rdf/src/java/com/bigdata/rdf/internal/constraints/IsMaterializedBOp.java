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

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Imposes the constraint <code>isMaterialized(x)</code>.
 */
public class IsMaterializedBOp extends XSDBooleanIVValueExpression {

    /**
	 * 
	 */
    private static final long serialVersionUID = -7552628930845996572L;

    private static final transient Logger log = Logger
            .getLogger(IsMaterializedBOp.class);

    public interface Annotations extends XSDBooleanIVValueExpression.Annotations {

        /**
         * If <code>true</code>, only accept variable bindings for
         * <code>x</code> that have a materialized RDF {@link BigdataValue}. If
         * <code>false</code>, only accept those that don't.
         */
    	String MATERIALIZED = IsMaterializedBOp.class.getName() + ".materialized";
    	
    }
    
    /**
     * 
     * @param x
     *            The variable.
     * @param materialized
     *            If <code>true</code>, only accept variable bindings for
     *            <code>x</code> that have a materialized RDF
     *            {@link BigdataValue}. If <code>false</code>, only accept those
     *            that don't.
     * @param lex
     *            The namespace of the lexicon relation.
     */
    public IsMaterializedBOp(final IVariable<IV> x, final boolean materialized) {

        this(new BOp[] { x }, NV.asMap(Annotations.MATERIALIZED, materialized));

    }
    
    /**
     * Required shallow copy constructor.
     */
    public IsMaterializedBOp(final BOp[] args, final Map<String, Object> anns) {

    	super(args, anns);
    	
        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();

		if (getProperty(Annotations.MATERIALIZED) == null)
			throw new IllegalArgumentException();
		
    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public IsMaterializedBOp(final IsMaterializedBOp op) {
        super(op);
    }

    public boolean accept(final IBindingSet bs) {
        
        final boolean materialized = 
        	(Boolean) getRequiredProperty(Annotations.MATERIALIZED); 
        
        final IV<?,?> iv = get(0).get(bs);
        
        if (log.isDebugEnabled()) {
        	log.debug(iv);
        	if (iv != null) 
        		log.debug("materialized?: " + iv.hasValue());
        }
        
        // not yet bound
        if (iv == null)
        	throw new SparqlTypeErrorException();

    	return iv.hasValue() == materialized || !iv.needsMaterialization();

    }
    
}
