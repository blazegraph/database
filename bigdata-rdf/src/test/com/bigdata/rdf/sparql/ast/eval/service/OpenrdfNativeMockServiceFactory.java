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
 * Created on Mar 7, 2012
 */

package com.bigdata.rdf.sparql.ast.eval.service;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.openrdf.query.BindingSet;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.service.ExternalServiceCall;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.OpenrdfNativeServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceFactory;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.striterator.CloseableIteratorWrapper;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Mock service reports the solutions provided in the constructor.
 * <p>
 * Note: This can not be used to test complex queries because the caller needs
 * to know the order in which the query will be evaluated in order to know the
 * correct response for the mock service.
 */
public class OpenrdfNativeMockServiceFactory implements ServiceFactory {

    private final OpenrdfNativeServiceOptions serviceOptions = new OpenrdfNativeServiceOptions();

    private final List<BindingSet> serviceSolutions;

    public OpenrdfNativeMockServiceFactory(
            final List<BindingSet> serviceSolutions) {

        this.serviceSolutions = serviceSolutions;

    }

    @Override
    public ServiceCall<?> create(final ServiceCallCreateParams params) {

        TestOpenrdfNativeServiceEvaluation.assertNotNull(params);
        
        TestOpenrdfNativeServiceEvaluation.assertNotNull(params.getTripleStore());

        TestOpenrdfNativeServiceEvaluation.assertNotNull(params.getServiceNode());

        return new MockExternalServiceCall();

    }

    @Override
    public IServiceOptions getServiceOptions() {
        return serviceOptions;
    }

    private class MockExternalServiceCall implements ExternalServiceCall {

        @Override
        public ICloseableIterator<BindingSet> call(
                final BindingSet[] bindingSets) {

            TestOpenrdfNativeServiceEvaluation.assertNotNull(bindingSets);

            // System.err.println("ServiceCall: in="+Arrays.toString(bindingSets));
            //
            // System.err.println("ServiceCall: out="+serviceSolutions);

            return new CloseableIteratorWrapper<BindingSet>(
                    serviceSolutions.iterator());

        }

        @Override
        public IServiceOptions getServiceOptions() {
            return serviceOptions;
        }

    }
    
    @Override
    public Set<IVariable<?>> getRequiredBound(final ServiceNode serviceNode) {
       return new HashSet<IVariable<?>>();
    }

    @Override
    public Set<IVariable<?>> getDesiredBound(final ServiceNode serviceNode) {
       return new HashSet<IVariable<?>>();       
    }        

}
