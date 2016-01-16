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
 * Created on Mar 7, 2012
 */

package com.bigdata.rdf.sparql.ast.service;


/**
 * Additional options for native services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface INativeServiceOptions extends IServiceOptions {

//    /**
//     * When <code>true</code>, the {@link ServiceNode} will be lifted into a
//     * {@link NamedSubqueryRoot}. {@link NamedSubqueryRoot}s are evaluated first
//     * against a single empty input solution.
//     * 
//     * @see QueryHints#RUN_ONCE
//     */
//    boolean isRunOnce();
//
//    /**
//     * When <code>true</code>, the {@link ServiceNode} will always be annotated
//     * as an "at-once" operator. All inputs to the {@link ServiceNode} will be
//     * materialized and the {@link ServiceNode} will run exactly once against
//     * those inputs.
//     * 
//     * @see QueryHints#AT_ONCE
//     */
//    boolean isAtOnce();

}
