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

package com.bigdata.join;

import java.io.Serializable;

/**
 * A symbolic "name" for a relation. The "name" may be any of the following, and
 * other kinds of "names" may be defined. It is up to the {@link IJoinNexus} to
 * understand an implementation of this interface and to resolve it to the
 * {@link IRelation} object that is used to read and/or write on the identified
 * relation.
 * <ul>
 * 
 * <li>The most common use case simply identifies the name of the relation in a
 * manner than can be more or less directly translated into the name of the
 * index(s) backing that relation. The timestamp of the historical commit point
 * or a transaction identifier may also be specified.</li>
 * 
 * <li>The relation may exist within a temporary store.</li>
 * 
 * <li>The relation may be a view of two or more relations. See
 * {@link RelationFusedView}.</li>
 * 
 * </ul>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <R> The generic type of the [R]elation.
 * 
 * @todo it may be worthwhile to introduce more structure into this interface, a
 *       convention for naming indices on persistent vs explicit temporary vs
 *       transaction scoped temporary storage. how timestamps fit in here also
 *       needs to be explored.
 */
public interface IRelationName<R> extends Serializable {

    /**
     * A human readable representation of the relation "name".
     */
    public String toString();
    
}
