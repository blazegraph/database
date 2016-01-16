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
 * Created on Aug 14, 2011
 */

package com.bigdata.bop.join;

import com.bigdata.bop.IPredicate;
import com.bigdata.relation.accesspath.IAccessPath;

/**
 * Interface declares additional annotations for joins against an
 * {@link IAccessPath} (versus a subquery or temporary solution set).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface AccessPathJoinAnnotations extends JoinAnnotations {

    /**
     * The {@link IPredicate} which is used to generate the {@link IAccessPath}s
     * during the join.
     * <p>
     * Note: Scale-out relies on this annotation when mapping intermediate
     * results across the shards on a cluster so they can be joined on the nodes
     * which have the data for the shards on which the {@link IAccessPath} must
     * read.
     */
    String PREDICATE = AccessPathJoinAnnotations.class.getName() + ".predicate";

}
