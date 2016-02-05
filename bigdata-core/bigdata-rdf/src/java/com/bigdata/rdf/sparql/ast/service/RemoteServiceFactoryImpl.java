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
 * Created on Mar 1, 2012
 */

package com.bigdata.rdf.sparql.ast.service;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.rdf.sparql.ast.eval.AbstractServiceFactoryBase;

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
public class RemoteServiceFactoryImpl extends AbstractServiceFactoryBase {

    private final RemoteServiceOptions serviceOptions;

    /**
     * Create a {@link ServiceFactory} for remote SPARQL end points.
     * 
     * @param isSparql11
     *            <code>true</code> if the end points support SPARQL 1.1.
     */
    public RemoteServiceFactoryImpl(final SPARQLVersion sparqlVersion) {
        
        this.serviceOptions = new RemoteServiceOptions();
        
        this.serviceOptions.setSPARQLVersion(sparqlVersion);
        
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
    
    /**
     * In order to be able to evaluate the SPARQL 1.1 remote service, the
     * endpoint URIs must be known. Everything else is desired, but not
     * required, and nothing is forbidden.
     */
    @Override
    public Set<IVariable<?>> getRequiredBound(final ServiceNode serviceNode) {
       
       final Set<IVariable<?>> requiredBound = new HashSet<IVariable<?>>();
       
       final IVariableOrConstant<?> serviceRef = 
          serviceNode.getServiceRef().getValueExpression();

       if (serviceRef!=null && serviceRef instanceof IVariable) {
          requiredBound.add((IVariable<?>)serviceRef);
       }
          
       return requiredBound;
    }

    @Override
    public Set<IVariable<?>> getDesiredBound(final ServiceNode serviceNode) {
       
       final Set<IVariable<?>> desiredBound = new HashSet<IVariable<?>>();
       
       final Iterator<IVariable<?>> varIt = 
          BOpUtility.getSpannedVariables(serviceNode.getGraphPattern());
       while (varIt.hasNext()) {
          desiredBound.add(varIt.next());
       }
       
       return desiredBound;
    }        

}
