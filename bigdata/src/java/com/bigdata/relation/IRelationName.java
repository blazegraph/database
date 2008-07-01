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

import com.bigdata.relation.rule.eval.IJoinNexus;

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
 * @param <R>
 *            The generic type of the [R]elation.
 * 
 * @todo (These issues are currently handled by an appropriate
 *       {@link IRelationLocator} implementation.)
 *       <p>
 *       It is perfectly reasonable for rules to write on a temporary index and
 *       there should be a facility for creating and destroying temporary
 *       indices. Perhaps they should be placed into their own namespace, e.g.,
 *       "#x" would be a temporary index named "x" (could support scale-out) and
 *       "##x" would be a data-service local temporary index named "x". "x" by
 *       itself is a normal index. Normally, such temporary indices should be
 *       scoped to something like a transaction but transaction support is not
 *       yet finished.
 *       <p>
 *       There MUST be a way to name a view of two relations to support
 *       {@link RelationFusedView}. The view has to be described before we are
 *       able to resolve the relations to their functional objects.
 * 
 * @todo alternatively, make this a simple String. Right now it is being
 *       interpreted as the namespace of the indices and the timestamp is being
 *       specified in parallel through {@link IRelationLocator}.
 */
public interface IRelationName<R> extends Serializable {

    /**
     * A human readable representation of the relation "name".
     */
    public String toString();
    
}
