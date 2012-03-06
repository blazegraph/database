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
 * Created on Mar 3, 2012
 */

package com.bigdata.rdf.sparql.ast.service;

import org.openrdf.query.BindingSet;

import com.bigdata.bop.IBindingSet;

/**
 * Options and metadata for service end points.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IServiceOptions {

    /**
     * Return <code>true</code> iff the service is "bigdata" aware. When
     * <code>true</code>, the {@link ServiceCall} implementation is expected to
     * exchange {@link IBindingSet}s rather than {@link BindingSet}s containing
     * materialized RDF Values.
     */
    boolean isBigdataService();

    /**
     * Return <code>true</code> iff the service is "remote". A remote service is
     * one where we must generate and send a SPARQL query. A local service is
     * one where the {@link ServiceNode#getGraphPattern()} may be directly
     * evaluated by the service implementation.
     */
    boolean isRemoteService();

    /**
     * Return <code>true</code> iff the end point supports
     * <code>SPARQL 1.1</code> (including the <code>SPARQL 1.1 BINDINGS</code>).
     * When <code>false</code>, the solutions will be vectored to the end point
     * using a technique which is compatible with <code>SPARQL 1.0</code>.
     */
    boolean isSparql11();

}
