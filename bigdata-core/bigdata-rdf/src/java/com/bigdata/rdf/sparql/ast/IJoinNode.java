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
 * Created on Oct 20, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.List;

import com.bigdata.rdf.sparql.ast.optimizers.ASTAttachJoinFiltersOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSimpleOptionalOptimizer;

/**
 * A marker interface for any kind of AST Node which joins stuff.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IJoinNode extends IBindingProducerNode {

    public interface Annotations {

        /**
         * A {@link List} of {@link FilterNode}s for constraints which MUST run
         * <em>with</em> the JOIN.
         */
        String FILTERS = "filters";

        /**
         * Boolean flag indicates that a join node has OPTIONAL semantics.
         */
        String OPTIONAL = "optional";
    
        boolean DEFAULT_OPTIONAL = false;
        
        /**
         * When <code>true</code>, the join group has the semantics of a SPARQL
         * MINUS operator and only those solutions in the parent group which do
         * NOT join with this group will be output.
         */
        String MINUS = "minus";
        
        boolean DEFAULT_MINUS = false;
        
    }
    
    /**
     * Return whether or not this is an join with "optional" semantics. Optional
     * joins may or may not produce variable bindings, but will not reduce the
     * incoming solutions based on whether or not they bind.
     */
    boolean isOptional();

    /**
     * Return <code>true</code> iff this is a join group representing a SPARQL
     * MINUS operator.
     */
    boolean isMinus();
    
    /**
     * Return the FILTER(s) associated with this {@link IJoinNode}. Such filters
     * will be run with the JOIN for this statement pattern. As such, they MUST
     * NOT rely on materialization of variables which would not have been bound
     * before that JOIN.
     * 
     * @return The attached join filters and never <code>null</code> (it may
     *         return an empty list)(.
     * 
     * @see ASTSimpleOptionalOptimizer
     * @see ASTAttachJoinFiltersOptimizer
     */
    List<FilterNode> getAttachedJoinFilters();

    void setAttachedJoinFilters(final List<FilterNode> filters);
 
}
