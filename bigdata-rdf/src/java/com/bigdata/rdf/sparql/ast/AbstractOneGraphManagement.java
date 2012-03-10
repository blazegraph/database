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
 * Created on Mar 10, 2012
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;

/**
 * Any of the operations which acts on a single target graph.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractOneGraphManagement extends GraphManagement {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param updateType
     */
    public AbstractOneGraphManagement(UpdateType updateType) {
        super(updateType);
    }

    /**
     * @param op
     */
    public AbstractOneGraphManagement(AbstractOneGraphManagement op) {
        super(op);
    }

    /**
     * @param args
     * @param anns
     */
    public AbstractOneGraphManagement(BOp[] args, Map<String, Object> anns) {
        super(args, anns);
    }

    @Override
    final public ConstantNode getTargetGraph() {
        
        return (ConstantNode) getProperty(Annotations.TARGET_GRAPH);
        
    }

    @Override
    final public void setTargetGraph(final ConstantNode targetGraph) {

        if (targetGraph == null)
            throw new IllegalArgumentException();

        setProperty(Annotations.TARGET_GRAPH, targetGraph);

    }

    // CREATE ( SILENT )? GRAPH IRIref
    // DROP  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
    final public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        sb.append(indent(indent));
        
        sb.append(getUpdateType());

        if (isSilent())
            sb.append(" SILENT");

        final ConstantNode targetGraph = getTargetGraph();

        if (targetGraph != null) {
            sb.append(" target=" + targetGraph);
        } else {
            sb.append(" all");
        }

        sb.append("\n");

        return sb.toString();

    }

}
