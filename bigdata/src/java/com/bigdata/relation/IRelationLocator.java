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

import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.DataService;
import com.bigdata.service.DataServiceIndex;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IClientIndex;
import com.bigdata.service.LocalDataServiceFederation;
import com.bigdata.service.LocalDataServiceFederation.LocalDataServiceImpl;

/**
 * An object that knows how to resolve an {@link IRelationName} to an
 * {@link IRelation} instance.
 * 
 * FIXME Javadoc update:
 * 
 * There are several ways to locate a relation, which generally means locating
 * its indices and perhaps some metadata that tells you which indices it is
 * using:
 * <ol>
 * 
 * <li>{@link JournalRelationLocator} and {@link TemporaryStoreRelationLocator} are
 * useful for locating an {@link IRelation} on a local temporary store, an in
 * memory store, etc. Such relations can only be located on the host and JVM
 * where they reside.</li>
 * 
 * <li> {@link FederationRelationLocator} is useful for locating any
 * {@link IRelation} in an {@link IBigdataFederation} anywhere with access to
 * that {@link IBigdataFederation}. The index references will be
 * {@link IClientIndex} instances -- either {@link ClientIndexView} for a
 * scale-out federation or {@link DataServiceIndex} for a
 * {@link LocalDataServiceFederation}. These indices are efficient for batch
 * operations. When the {@link DataService}(s) are remote, RMI will be used.</li>
 * 
 * <li>{@link AbstractTaskRelationLocator} is useful inside of the
 * {@link LocalDataServiceImpl} for a {@link LocalDataServiceFederation}. This
 * class of federation uses monolithic indices (vs index partitions). Tasks can
 * then be run on the {@link ConcurrencyManager} for the {@link DataService}
 * that gain simultaneous access to the indices for some {@link IRelation}.
 * This allows extremely efficient JOINs, but the JOINs do not scale-out.</li>
 * 
 * <li>{@link FusedViewRelationLocator} is used to locate a view of fused view
 * of two relations. This is used by the RDF DB for truth maintenance.</li>
 * 
 * </ol>
 * 
 * There are some other possibilities, but these are the main use cases so far.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <R>
 *            The generic type of the [R]elation.
 */
public interface IRelationLocator<R> {

    /**
     * Resolve the {@link IRelation}.
     * 
     * @param relationName
     *            The identifier for that relation.
     * @param timestamp
     *            The timestamp for the view of that relation.
     * 
     * @return The {@link IRelation} iff it exists and never <code>null</code>.
     * 
     * @throws RuntimeException
     *             if there is an error when resolving the relation.
     */
    public IRelation<R> getRelation(IRelationName<R> relationName, long timestamp);
    
}
