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
 * Created on Sep 8, 2011
 */

package com.bigdata.rdf.sparql.ast.service;

import java.util.List;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;

/**
 * <p>
 * Service invocation interface for an external (non-bigdata) service. Data
 * interchange with the external service uses the internal {@link IBindingSet} 
 * abstraction and it is up to the implementing service to do any conversion of
 * external entities. More precisely, the service must return internal values
 * with mocked IVs, which will be automatically resolved against the database,
 * turning them into {@link IV}s. This can be understood as the most generic
 * kind of interface for interacting with external services.
 * </p>
 * 
 * <p>
 * In order to determine the set of mocked IVs, the service must specify the set
 * of variables for which such mocked IVs are generated. This can be understood
 * as the set of "outgoing" variables of the service, i.e. those variables
 * that are bound by executing the service call. All variables occurring in
 * the service that are not part of this set are considered incoming variables;
 * they must be bound (if at all) prior to executing the service.
 * </p>
 * 
 * @see ServiceRegistry
 * @see ServiceFactory
 */
public interface MockIVReturningServiceCall extends ServiceCall<IBindingSet> { 
   
   /**
    * Returns the set of variables that are internally bound. As this service
    * is not operating on the internal data model, these are the variables
    * that are pointing to mock IVs (which need to be resovled later on).
    * 
    * @return
    */
   public List<IVariable<IV>> getMockVariables();

}
