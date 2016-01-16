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
 * Created on Mar 3, 2012
 */

package com.bigdata.rdf.sparql.ast.service;

import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;

/**
 * Options and metadata for service end points.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IServiceOptions {

    /**
     * Return <code>true</code> iff the service is a native (aka "bigdata"
     * aware) internal service. When <code>true</code>, the {@link ServiceCall}
     * implementation is expected to exchange {@link IBindingSet}s containing
     * {@link IV}s and those {@link IV}s are NOT guaranteed to be materialized.
     * When <code>false</code>, the service is expected to exchange
     * {@link BindingSet}s containing materialized RDF {@link Value}s.
     */
    boolean isBigdataNativeService();

    /**
     * Return <code>true</code> iff the service is "remote". A remote service is
     * one where we must generate and send a SPARQL query. A local service is
     * one where the {@link ServiceNode#getGraphPattern()} may be directly
     * evaluated by the service implementation.
     */
    boolean isRemoteService();

    /**
     * Return <code>true</code> iff the end point supports
     * <code>SPARQL 1.0</code>  
     */
     boolean isSparql10();

    /**
     * Return <code>true</code> iff the service end point is one which should
     * always be run as early as possible within the join group (default
     * <code>false</code>).
     * <p>
     * Note: This option is only obeyed when the SERVICE reference is a
     * constant. If the SERVICE reference is a variable, then it is not possible
     * to resolve the {@link IServiceOptions} for that SERVICE until after the
     * query planner has locked in the join evaluation order.
     */
    boolean isRunFirst();

    /**
     * Return <code>true</code> if the remote service is known to be a bigdata
     * service that exposes the HA load balancer servlet (default
     * <code>false</code>). The default may be overridden iff the end point is
     * known to expose the bigdata LBS pattern.
     */
    boolean isBigdataLBS();
    
    
    /**
     * Returns if {@link SPARQLVersion#SPARQL_11} iff the end point supports
     * <code>SPARQL 1.1</code>.
     * When {@link SPARQLVersion#SPARQL_10}, the solutions will be vectored to the end point
     * using a technique which is compatible with <code>SPARQL 1.0</code>.
     * 
     * @return the SPARQL version {@link SPARQLVersion}
     * 
     *      
     */
    SPARQLVersion getSPARQLVersion();
    
}
