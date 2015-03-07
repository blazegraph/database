/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Mar 1, 2012
 */

package com.bigdata.rdf.sparql.ast.service;

/**
 * A factory for service calls against remote SPARQL end points. You can control
 * the way in which bigdata handles SPARQL 1.1 Federated Query for a remote
 * SPARQL end point by: (1) create an instance of this class for the SPARQL end
 * point; (2) customize the {@link RemoteServiceOptions}; and (3) add it to this
 * {@link ServiceRegistry}. You can also subclass this class if you want to
 * support configuration options which are not already supported by
 * {@link RemoteServiceOptions} and {@link RemoteServiceCallImpl}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: RemoteServiceFactoryImpl.java 6068 2012-03-03 21:34:31Z
 *          thompsonbry $
 */
public class RemoteServiceFactoryImpl implements ServiceFactory {

    private final RemoteServiceOptions serviceOptions;

    /**
     * Create a {@link ServiceFactory} for remote SPARQL end points.
     * 
     * @param isSparql11
     *            <code>true</code> if the end points support SPARQL 1.1.
     */
    public RemoteServiceFactoryImpl(final boolean isSparql11) {
        
        this.serviceOptions = new RemoteServiceOptions();
        
        this.serviceOptions.setSparql11(isSparql11);
        
    }

    /**
     * Create a {@link ServiceFactory} for remote SPARQL end points.
     * 
     * @param serviceOptions
     *            The configuration options for the end points.
     */
    public RemoteServiceFactoryImpl(final RemoteServiceOptions serviceOptions) {

        if (serviceOptions == null)
            throw new IllegalArgumentException();
        
        this.serviceOptions = serviceOptions;
        
    }

    @Override
    public RemoteServiceCall create(final ServiceCallCreateParams params) {

        return new RemoteServiceCallImpl(params);

    }

    @Override
    public RemoteServiceOptions getServiceOptions() {

        return serviceOptions;

    }

}
