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
 * Created on Aug 26, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;

/**
 * A special function node for modeling EXISTS.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ExistsNode extends SubqueryFunctionNodeBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param lex
     * @param anonvar
     *            An anonymous variable which will be bound by an ASK subquery.
     * @param graphPattern
     *            The inner value of the EXISTS function.
     */
    public ExistsNode(final VarNode anonvar,
            final GraphPatternGroup<IGroupMemberNode> graphPattern) {

        super(FunctionRegistry.EXISTS, anonvar, graphPattern );

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public ExistsNode(ExistsNode op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public ExistsNode(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }
    
}
