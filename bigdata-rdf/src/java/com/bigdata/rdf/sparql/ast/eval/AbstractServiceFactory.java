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
 * Created on Sep 9, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.HashSet;
import java.util.Set;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.service.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceFactory;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * An abstract {@link ServiceFactory} that deals with service parameters (magic
 * predicates that configure the service) in a standardized manner using the
 * {@link ServiceParams} helper class.
 */
public abstract class AbstractServiceFactory implements ServiceFactory {

    public AbstractServiceFactory() {
    
    }

    /**
     * Create a {@link BigdataServiceCall}.  Does the work of collecting
     * the service parameter triples and then delegates to 
     * {@link #create(ServiceCallCreateParams, ServiceParams)}.
     */
    @Override
    final public BigdataServiceCall create(final ServiceCallCreateParams params) {

        if (params == null)
            throw new IllegalArgumentException();

        final AbstractTripleStore store = params.getTripleStore();

        if (store == null)
            throw new IllegalArgumentException();

        final ServiceNode serviceNode = params.getServiceNode();

        if (serviceNode == null)
            throw new IllegalArgumentException();

        final ServiceParams serviceParams = ServiceParams.gatherServiceParams(params);
        
        return create(params, serviceParams);
        
    }
    
    /**
     * Implemented by subclasses - verify the group and create the service call.
     */
    public abstract BigdataServiceCall create(
    		final ServiceCallCreateParams params,
    		final ServiceParams serviceParams);

    /**
     * Default implementation for method
     * {@link ServiceFactory#getRequiredBound(ServiceNode)}, allowing for
     * simple services where all variables used inside the service are
     * considered "outgoing". As a consequence, when building upon this
     * default implementation, the Service will be executed *before* the
     * variables used inside the Service body are bound.
     */
    @Override
    public Set<IVariable<?>> getRequiredBound(final ServiceNode serviceNode) {
       return new HashSet<IVariable<?>>();
    }

    /**
     * Default implementation for method
     * {@link ServiceFactory#getDesiredBound(ServiceNode)}, allowing for
     * simple services where all variables used inside the service are
     * considered "outgoing". As a consequence, when building upon this
     * default implementation, the Service will be executed *before* the
     * variables used inside the Service body are bound.
     */
    @Override
    public Set<IVariable<?>> getDesiredBound(final ServiceNode serviceNode) {
       return new HashSet<IVariable<?>>();       
    }    
    
}
