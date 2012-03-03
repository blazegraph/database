/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Sep 8, 2011
 */

package com.bigdata.rdf.sparql.ast.service;

import org.openrdf.query.BindingSet;

import com.bigdata.bop.IBindingSet;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Service invocation interface.
 * 
 * @param <E>
 *            The generic type of the solutions which are exchanged with the
 *            service implementation. This will be either {@link IBindingSet} or
 *            {@link BindingSet}. Note that those two interfaces do not have any
 *            common ancestor other than {@link Object}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ServiceCall<E> {

    /**
     * Return options and metadata for the service end point.
     * 
     * TODO The options for a {@link ServiceCall} instance are shared with all
     * instances for the same factory. This should be appropriate because the
     * factory is specific to the service URI. The only exception would be the
     * {@link ServiceRegistry#getDefaultServiceFactory()}. Consider replacing
     * this with a back reference to the {@link ServiceFactory}.
     */
    IServiceOptions getServiceOptions();
    
    /**
     * Invoke an service.
     * 
     * @param bindingSets
     *            The binding sets flowing into the service.
     * 
     * @return An iterator from which the solutions can be drained. If the
     *         iterator is closed, the service invocation must be cancelled.
     * @throws Exception 
     */
    ICloseableIterator<E> call(E[] bindingSets) throws Exception;

}
