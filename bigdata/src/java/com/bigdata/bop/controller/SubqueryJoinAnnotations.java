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

package com.bigdata.bop.controller;

import com.bigdata.bop.IPredicate;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.join.JoinAnnotations;
import com.bigdata.bop.join.PipelineJoin;

/**
 * Annotations for joins against a subquery.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface SubqueryJoinAnnotations extends JoinAnnotations {

    /**
     * The subquery to be evaluated for each binding sets presented to the
     * {@link SubqueryOp} (required). This should be a {@link PipelineOp}. (It
     * is basically the equivalent of the {@link IPredicate} for a
     * {@link PipelineJoin}).
     */
    String SUBQUERY = SubqueryOp.class.getName() + ".subquery";

    /**
     * When <code>true</code> the subquery has optional semantics (if the
     * subquery fails, the original binding set will be passed along to the
     * downstream sink anyway) (default {@value #DEFAULT_OPTIONAL}).
     * 
     * @todo This is somewhat in conflict with how we mark optional predicates
     *       to support the RTO. The OPTIONAL marker might need to be moved onto
     *       the subquery.
     */
    String OPTIONAL = SubqueryOp.class.getName() + ".optional";

    boolean DEFAULT_OPTIONAL = false;

}
