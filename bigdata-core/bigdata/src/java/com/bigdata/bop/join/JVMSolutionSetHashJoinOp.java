/**

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
/*
 * Created on Aug 30, 2011
 */

package com.bigdata.bop.join;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.NV;

/**
 * {@inheritDoc}
 * <p>
 * JVM Specific version.
 * 
 * @see JVMHashJoinUtility
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: NamedSubqueryIncludeOp.java 5178 2011-09-12 19:09:23Z
 *          thompsonbry $
 */
public class JVMSolutionSetHashJoinOp extends SolutionSetHashJoinOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Deep copy constructor.
     */
    public JVMSolutionSetHashJoinOp(JVMSolutionSetHashJoinOp op) {

        super(op);
        
    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public JVMSolutionSetHashJoinOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);
        
    }

    public JVMSolutionSetHashJoinOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

}
