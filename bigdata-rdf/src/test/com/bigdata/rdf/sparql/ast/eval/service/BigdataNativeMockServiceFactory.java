/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 7, 2012
 */

package com.bigdata.rdf.sparql.ast.eval.service;

import java.util.List;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.service.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.OpenrdfNativeServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceFactory;
import com.bigdata.striterator.CloseableIteratorWrapper;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Mock service reports the solutions provided in the constructor.
 * <p>
 * Note: This can not be used to test complex queries because the caller needs
 * to know the order in which the query will be evaluated in order to know the
 * correct response for the mock service.
 */
public class BigdataNativeMockServiceFactory implements ServiceFactory {

    private final OpenrdfNativeServiceOptions serviceOptions = new OpenrdfNativeServiceOptions();

    private final List<IBindingSet> serviceSolutions;

    public BigdataNativeMockServiceFactory(final List<IBindingSet> serviceSolutions) {

        this.serviceSolutions = serviceSolutions;

    }

    @Override
    public BigdataServiceCall create(final ServiceCallCreateParams params) {

        TestBigdataNativeServiceEvaluation.assertNotNull(params);

        TestBigdataNativeServiceEvaluation.assertNotNull(params.getTripleStore());

        TestBigdataNativeServiceEvaluation.assertNotNull(params.getServiceNode());

        return new MockBigdataServiceCall();

    }

    @Override
    public IServiceOptions getServiceOptions() {
        return serviceOptions;
    }

    private class MockBigdataServiceCall implements BigdataServiceCall {

        @Override
        public ICloseableIterator<IBindingSet> call(
                final IBindingSet[] bindingSets) {

            TestBigdataNativeServiceEvaluation.assertNotNull(bindingSets);

            // System.err.println("ServiceCall: in="+Arrays.toString(bindingSets));
            //
            // System.err.println("ServiceCall: out="+serviceSolutions);

            return new CloseableIteratorWrapper<IBindingSet>(
                    serviceSolutions.iterator());

        }

        @Override
        public IServiceOptions getServiceOptions() {
            return serviceOptions;
        }

    }

}
