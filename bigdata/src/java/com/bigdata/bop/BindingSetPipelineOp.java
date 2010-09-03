/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
/*
 * Created on Sep 2, 2010
 */

package com.bigdata.bop;

import java.util.Map;

/**
 * Abstract base class for pipeline operators where the data moving along the
 * pipeline is chunks of {@link IBindingSet}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class BindingSetPipelineOp extends
        PipelineOp<IBindingSet> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {

        /**
         * The value of the annotation is the {@link BOp.Annotations#BOP_ID} of
         * the ancestor in the operator tree which serves as an alternative sink
         * for binding sets.
         */
        String ALT_SINK_REF = BindingSetPipelineOp.class.getName()
                + ".altSinkRef";

    }

    /**
     * Required deep copy constructor.
     * 
     * @param op
     */
    protected BindingSetPipelineOp(PipelineOp<IBindingSet> op) {
        super(op);
    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    protected BindingSetPipelineOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

    }

}
