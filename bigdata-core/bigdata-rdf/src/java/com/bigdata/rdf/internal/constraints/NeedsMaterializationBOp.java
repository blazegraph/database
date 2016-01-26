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

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.util.InnerCause;

/**
 * Attempts to run a constraint prior to materialization. Returns
 * <code>false</code> if it completes successfully without a
 * {@link NotMaterializedException}, <code>true</code> otherwise.
 */
public class NeedsMaterializationBOp extends XSDBooleanIVValueExpression {

	/**
	 * 
	 */
    private static final long serialVersionUID = 4767476516948560884L;

//    private static final transient Logger log = Logger
//            .getLogger(NeedsMaterializationBOp.class);

    public NeedsMaterializationBOp(final IValueExpression<?> x) {

        this(new BOp[] { x }, BOp.NOANNS);

    }
    
    /**
     * Required shallow copy constructor.
     */
    public NeedsMaterializationBOp(final BOp[] args,
            final Map<String, Object> anns) {

        super(args, anns);
    	
        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public NeedsMaterializationBOp(final NeedsMaterializationBOp op) {
        super(op);
    }

    public boolean accept(final IBindingSet bs) {

    	final IValueExpression<?> ve = get(0); 
    	
    	try {
    		
//    		if (log.isDebugEnabled()) {
//    			log.debug("about to attempt evaluation prior to materialization");
//    		}
    		
    		ve.get(bs);
    		
//    		if (log.isDebugEnabled()) {
//    			log.debug("successfully evaluated constraint without materialization");
//    		}
    		
    		return false;
    		
    	} catch (Throwable t) {

			if (InnerCause.isInnerCause(t, NotMaterializedException.class)) {
    		
//	    		if (log.isDebugEnabled()) {
//	    			log.debug("could not evaluate constraint without materialization");
//	    		}
	    		
	    		return true;
    		
			} else throw new RuntimeException(t);
    		
    	}

    }
    
}
