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
 * Created on Apr 2, 2012
 */

package com.bigdata.rdf.sparql.ast.service;

import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Interface for custom services.
 * 
 * @see <a
 *      href="https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=FederatedQuery">
 *      Federated Query and Custom Services</a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface CustomServiceFactory extends ServiceFactory {

    /**
     * Callback is used to notify the {@link ServiceFactory} of connection
     * starts. If the service wishes to be notified of updates, then it must
     * return a {@link IChangeLog} implementation here. The listener will be
     * obtained when the {@link ServiceFactory} is registered.
     * <p>
     * Note: This is not invoked unless you are using a
     * {@link BigdataSailConnection}. Updates that are made directly using an
     * {@link AbstractTripleStore} are not visible to the service.
     * 
     * @return The {@link IChangeLog} listener -or- <code>null</code> if the
     *         service does not want to observe changes.
     * 
     *         FIXME Re-think the notification mechanism. Services should see
     *         ALL updates, not just those made using the
     *         {@link BigdataSailConnection}.
     */
    void startConnection(BigdataSailConnection conn);

}
