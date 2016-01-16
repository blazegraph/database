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
 * Created on June 26, 2015
 */
package com.bigdata.rdf.sparql.ast.eval;

import java.util.HashSet;
import java.util.Set;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.service.ServiceFactory;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

/**
 * Base class for abstract services, providing base implementation for
 * getRequiredBound and getDesiredBound methods (which can be overridden
 * by subclasses).
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public abstract class AbstractServiceFactoryBase implements ServiceFactory {

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
