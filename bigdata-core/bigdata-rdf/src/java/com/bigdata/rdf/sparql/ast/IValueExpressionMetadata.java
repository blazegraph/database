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
 * Created on Aug 17, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Set;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;

/**
 * Abstract base class for AST nodes which embed value expressions and hence
 * must be able to report certain metadata about those value expressions to the
 * query planner.
 */
public interface IValueExpressionMetadata {

    /**
     * Return the set of variables that will be used by this constraint to
     * determine which solutions will continue on through the pipeline and which
     * will be filtered out.
     */
    Set<IVariable<?>> getConsumedVars();

    /**
     * Return the materialization requirement for this filter. Many filters
     * require materialized variables to do their filtering. Some filters can
     * work on both materialized terms and internal values (a good example of
     * this is CompareBOp).
     */
    INeedsMaterialization getMaterializationRequirement();
    
//    /**
//     * Return the materialization requirement for this filter. Many filters
//     * require materialized variables to do their filtering. Some filters can
//     * work on both materialized terms and internal values (a good example of
//     * this is CompareBOp).
//     */
//    abstract public INeedsMaterialization.Requirement getRequirement();
//
//    /**
//     * Return the set of variables that will need to be materialized in the
//     * binding set in order for this filter to be evaluated.
//     */
//    abstract public Set<IVariable<IV>> getVarsToMaterialize();

}
