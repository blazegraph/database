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
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.util.InnerCause;

/**
 * This is useful when a solution can be filtered out before it goes through the
 * materialization pipeline. Note that if a solution passes, it will still enter
 * the materialization pipeline. It is up to the first step in the pipeline to
 * ensure that the solution gets routed around the materialization steps. See
 * {@link IsMaterializedBOp}.
 */
public class TryBeforeMaterializationConstraint extends BOpBase implements
        IConstraint {

    /**
	 * 
	 */
    private static final long serialVersionUID = -761919593813838105L;

    private static final transient Logger log = Logger
            .getLogger(TryBeforeMaterializationConstraint.class);
	
	public TryBeforeMaterializationConstraint(final IConstraint x) {
		
        this(new BOp[] { x }, null/*annocations*/);

    }

    /**
     * Required shallow copy constructor.
     */
    public TryBeforeMaterializationConstraint(final BOp[] args, 
    		final Map<String, Object> anns) {
    	
        super(args, anns);
        
        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public TryBeforeMaterializationConstraint(
            final TryBeforeMaterializationConstraint op) {

        super(op);
        
    }

    /**
     * This is useful when a solution can be filtered out before it goes
     * through the materialization pipeline.  Note that if a solution passes,
     * it will still enter the materialization pipeline.  It is up to the first
     * step in the pipeline to ensure that the solution gets routed around
     * the materialization steps.  See {@link IsMaterializedBOp}.
     */
    @Override
    public boolean accept(final IBindingSet bs) {

        final IConstraint c = (IConstraint) get(0);

        try {

            if (log.isDebugEnabled()) {
                log.debug("about to attempt evaluation prior to materialization");
            }

            final boolean accept = c.accept(bs);

            if (log.isDebugEnabled()) {
                log.debug("successfully evaluated constraint without materialization");
            }

            return accept;

        } catch (Throwable t) {

            if (InnerCause.isInnerCause(t, NotMaterializedException.class)) {

                if (log.isDebugEnabled()) {
                    log.debug("could not evaluate constraint without materialization");
                }

                // let the solution through for now, it will get tested again
                // on the other side of the materialization pipeline
                return true;

            } else
                throw new RuntimeException(t);

        }

    }

}
