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
 * Created on Aug 26, 2011
 */

package com.bigdata.rdf.sparql.ast;

import org.openrdf.model.URI;

/**
 * A special function node for modeling value expression nodes which are
 * evaluated against an inner graph expression, such as EXISTS and NOT EXISTS.
 * Another possibility is IN(subSelect), where IN is evaluated against the
 * subquery result.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class SubqueryFunctionNodeBase extends FunctionNode {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    interface Annotations extends FunctionNode.Annotations {

        /**
         * The graph pattern associated with the subquery.
         */
        String GRAPH_PATTERN = "graphPattern";

    }

    /**
     * @param lex
     * @param anonvar
     *            An anonymous variable which will be bound by an ASK subquery.
     * @param graphPattern
     *            The inner value of the EXISTS function.
     */
    protected SubqueryFunctionNodeBase(String lex, URI functionURI, VarNode anonvar,
            GroupNodeBase graphPattern) {

        super(lex, functionURI, null/* scalarValues */,
                new ValueExpressionNode[] { anonvar });

        if (anonvar == null)
            throw new IllegalArgumentException();

        if (graphPattern == null)
            throw new IllegalArgumentException();

        setProperty(Annotations.GRAPH_PATTERN, graphPattern);

    }

}
