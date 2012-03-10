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
 * The CLEAR operation removes all the triples in the specified graph(s) in the
 * Graph Store.
 * 
 * <pre>
 * CLEAR  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
 * </pre>
 * 
 * Note: Bigdata does not support empty graphs, so DROP and CLEAR have identical
 * semantics.
 * 
 * @see http://www.w3.org/TR/sparql11-update/#clear
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClearGraph extends DropGraph {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public ClearGraph() {
        super(UpdateType.Clear);
    }

    /**
     * @param op
     */
    public ClearGraph(ClearGraph op) {
        super(op);
    }

    /**
     * @param args
     * @param anns
     */
    public ClearGraph(BOp[] args, Map<String, Object> anns) {
        super(args, anns);
    }

//    /**
//     * Return <code>true</code> IFF this is <code>CLEAR ALL</code>.
//     */
//    public boolean isAll() {
//        
//        return getTargetGraph() == null && getScope() == null;
//        
//    }
//    
//    @Override
//    public ConstantNode getTargetGraph() {
//        
//        return (ConstantNode) getProperty(Annotations.TARGET_GRAPH);
//        
//    }
//
//    @Override
//    public void setTargetGraph(final ConstantNode targetGraph) {
//
//        if (targetGraph == null)
//            throw new IllegalArgumentException();
//
//        setProperty(Annotations.TARGET_GRAPH, targetGraph);
//
//    }
//
//    @Override
//    public boolean isSilent() {
//
//        return getProperty(Annotations.SILENT, Annotations.DEFAULT_SILENT);
//
//    }
//
//    @Override
//    public void setSilent(final boolean silent) {
//
//        setProperty(Annotations.SILENT, silent);
//
//    }
//
//    public Scope getScope() {
//
//        return (Scope) getProperty(Annotations.SCOPE);
//
//    }
//
//    public void setScope(final Scope scope) {
//
//        setProperty(Annotations.SCOPE, scope);
//
//    }
//
//    // CLEAR  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
//    public String toString(final int indent) {
//
//        final StringBuilder sb = new StringBuilder();
//
//        sb.append(indent(indent));
//        
//        sb.append(getUpdateType());
//
//        if (isSilent())
//            sb.append(" SILENT");
//
//        final ConstantNode targetGraph = getTargetGraph();
//        final Scope scope = getScope();
//
//        if (targetGraph != null) {
//            sb.append(" target=" + targetGraph);
//        } else if (scope != null) {
//            sb.append(" scope=" + scope);
//        } else {
//            sb.append(" all");
//        }
//
//        return sb.toString();
//
//    }

}
