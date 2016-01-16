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
 * Created on June 12, 2015
 */
package com.bigdata.rdf.sparql.ast;

import java.util.Set;

import com.bigdata.bop.IVariable;

/**
 * Interface describing constraints and desiderata w.r.t. variable bindings
 * that should, must, or must not be present at the time when executing the
 * construct.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public interface IVariableBindingRequirements {

   /**
    * Return the variables used in the node that *must* be bound prior to
    * evaluating the node. For instance, when evaluation a SPARQL 1.1
    * service and the endpoint is specified through a variable, then this
    * variable must be bound in order to identify the endpoint to which the
    * query is to be sent.
    * 
    * By *must* be bound we mean that, if it is possible to bring the variables
    * in a join group in order such that they are bound when executing the
    * construct (e.g., think of a FILTER), then we *must* consider that.
    * However, when the {@link IVariableBindingRequirements#getRequiredBound()}
    * interface cannot be satisified, the query may still be valid and return
    * results (for the SPARQL 1.1 service example mentioned above, it may not,
    * for the FILTER it may).
    * 
    * The set of these variables must be disjoint from 
    * {@link IVariableBindingRequirements#getRequiredUnound()}.
    * 
    * @return the set of variables that must be bound prior to evaluation
    */
   public Set<IVariable<?>> getRequiredBound(final StaticAnalysis sa);

   /**
    * Get variables that are not required to be bound, but desired in the
    * sense that we want them to be bound when evaluating the construct.
    * In general, the more variables are bound, the better, but this might
    * differ for certain constructs. Should only report variables that are
    * *not* reported through method
    * {@link IVariableBindingRequirements#getRequiredBound(StaticAnalysis)}.
    * 
    * @return the set of variables that are desires to be bound, but not 
    *         required
    */
   public Set<IVariable<?>> getDesiredBound(final StaticAnalysis sa);
   
}
