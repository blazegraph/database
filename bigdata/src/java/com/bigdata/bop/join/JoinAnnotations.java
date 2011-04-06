/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Mar 31, 2011
 */

package com.bigdata.bop.join;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;

/**
 * Common annotations for various join operators.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface JoinAnnotations extends com.bigdata.bop.PipelineOp.Annotations {

    /**
     * An optional {@link IVariable}[] identifying the variables to be retained
     * in the {@link IBindingSet}s written out by the operator. All variables
     * are retained unless this annotation is specified.
     */
    String SELECT = JoinAnnotations.class.getName() + ".select";
    
    /**
     * An {@link IConstraint}[] which places restrictions on the legal
     * patterns in the variable bindings (optional).
     */
    String CONSTRAINTS = JoinAnnotations.class.getName() + ".constraints";

}
