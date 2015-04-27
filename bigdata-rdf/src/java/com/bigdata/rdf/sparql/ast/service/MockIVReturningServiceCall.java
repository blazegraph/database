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
 * Created on Sep 8, 2011
 */

package com.bigdata.rdf.sparql.ast.service;

import java.util.List;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;

/**
 * Service invocation interface for an external (non-bigdata, but same JVM)
 * service. Data interchange with the external service uses the internal
 * {@link IBindingSet} and it is up to the implementing service to do any
 * conversion. The service must return internal values with mocked IVs, which
 * will be resolved against the database, turning them into {@link IV}s.
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
