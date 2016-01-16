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
 * Created on Nov 11, 2011
 */

package com.bigdata.bop.controller;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.join.IHashJoinUtility;

/**
 * Attributes for named solution set processing.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface NamedSetAnnotations {

    /**
     * The name of {@link IQueryAttributes} attribute under which the
     * {@link INamedSolutionSetRef} can be located. That
     * {@link INamedSolutionSetRef} may be used to locate the
     * {@link IHashJoinUtility}, which includes metadata about the hash index
     * and encapsulates the hash index state. It may also be used to identify a
     * named index that will be resolved against the SPARQL CACHE, the local
     * index manager, or the federation index manager (on a cluster).
     * <p>
     * The attribute name includes the query UUID. The query UUID must be
     * extracted and used to lookup the {@link IRunningQuery} to which the
     * solution set was attached.
     * <p>
     * Note: For a MERGE JOIN, the value under this attribute is a
     * {@link INamedSolutionSetRef}[].
     * 
     * @see INamedSolutionSetRef
     * 
     * @see BOpContext#getAlternateSource(PipelineOp, INamedSolutionSetRef)
     */
    final String NAMED_SET_REF = "namedSetRef";

}
