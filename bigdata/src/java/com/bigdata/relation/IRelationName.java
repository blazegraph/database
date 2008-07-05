/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 25, 2008
 */

package com.bigdata.relation;

import java.io.Serializable;

/**
 * The unique identifier for an {@link IRelation} (its namespace).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo There should be a facility for creating and destroying temporary
 *       indices. Perhaps they should be placed into their own namespace, e.g.,
 *       "#x" would be a temporary index named "x" (could support scale-out) and
 *       "##x" would be a data-service local temporary index named "x". "x" by
 *       itself is a normal index. Normally, such temporary indices should be
 *       scoped to something like a transaction but transaction support is not
 *       yet finished.
 *       <p>
 *       You can name a view of two relations by just concatenating their names.
 * 
 * @param <R>
 *            The generic type of the [R]elation.
 */
public interface IRelationName<R> extends Serializable {

    /**
     * The namespace of the relation.
     */
    public String toString();
    
}
