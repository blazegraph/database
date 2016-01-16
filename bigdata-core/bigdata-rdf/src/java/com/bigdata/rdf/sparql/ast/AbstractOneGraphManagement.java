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
 * Created on Mar 10, 2012
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.spo.ISPO;

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
    public AbstractOneGraphManagement(final UpdateType updateType) {
        
        super(updateType);
        
    }

    /**
     * @param op
     */
    public AbstractOneGraphManagement(final AbstractOneGraphManagement op) {
        
        super(op);
        
    }

    /**
     * @param args
     * @param anns
     */
    public AbstractOneGraphManagement(final BOp[] args,
            final Map<String, Object> anns) {
        
        super(args, anns);
        
    }

    /**
     * Return <code>true</code> if the target is a GRAPH.
     */
    final boolean isTargetGraph() {
        
        return getProperty(Annotations.TARGET) instanceof ConstantNode;
        
    }

    /**
     * Return <code>true</code> if the target is a SOLUTION SET.
     */
    public final boolean isTargetSolutionSet() {

        return getProperty(Annotations.TARGET) instanceof String;
        
    }

    @Override
    final public ConstantNode getTargetGraph() {
        
        final Object o = getProperty(Annotations.TARGET);

        if (o instanceof ConstantNode) {
        
            return (ConstantNode) o;
            
        }

        return null;
        
    }

    @Override
    final public void setTargetGraph(final ConstantNode targetGraph) {

        if (targetGraph == null)
            throw new IllegalArgumentException();

        setProperty(Annotations.TARGET, targetGraph);

    }

//    @Override
    final public String getTargetSolutionSet() {

        final Object o = getProperty(Annotations.TARGET);

        if (o instanceof String) {

            return (String) o;

        }

        return null;

    }

//    @Override
    final public void setTargetSolutionSet(final String targetSolutionSet) {

        if (targetSolutionSet == null)
            throw new IllegalArgumentException();

        setProperty(Annotations.TARGET, targetSolutionSet);

    }

    // CREATE ( SILENT )? GRAPH IRIref
    // DROP  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL | GRAPHS | SOLUTIONS | SOLUTIONS %VARNAME)
    // CLEAR ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL | GRAPHS | SOLUTIONS | SOLUTIONS %VARNAME)
    final public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        sb.append(indent(indent));
        
        sb.append(getUpdateType());

        if (isSilent())
            sb.append(" SILENT");

        final ConstantNode targetGraph = getTargetGraph();

        final String targetSolutionSet = getTargetSolutionSet();

        if (targetGraph != null) {

            sb.append(" targetGraph=" + targetGraph);
        
        }
        if (targetSolutionSet != null) {
            
            sb.append(" targetSolutionSet=" + targetSolutionSet);

        }
        
        if(this instanceof DropGraph) {

            final DropGraph t = (DropGraph) this;
            
            if (t.isAllGraphs())
                sb.append(" ALL-GRAPHS");

            if (t.isAllSolutionSets())
                sb.append(" ALL-SOLUTIONS");

        } else if(this instanceof CreateGraph) {

            final CreateGraph t = (CreateGraph) this;

            final ISPO[] params = t.getParams();

            if (params != null) {

                final String s = indent(indent + 1);

                sb.append("\n").append(s).append("PARAMS {");

                for (ISPO v : params) {

                    sb.append(v.toString());

                }

                sb.append("\n").append(s).append("}");

            }

        }

        sb.append("\n");

        return sb.toString();

    }

}
