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

import org.openrdf.model.URI;

import com.bigdata.bop.BOp;

/**
 * The LOAD operation reads an RDF document from a IRI and inserts its triples
 * into the specified graph in the Graph Store.
 * 
 * <pre>
 * LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
 * </pre>
 * 
 * @see http://www.w3.org/TR/sparql11-update/#load
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LoadGraph extends GraphUpdate {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public LoadGraph() {
        
        super(UpdateType.Load);
        
    }

    /**
     * @param op
     */
    public LoadGraph(final LoadGraph op) {
        
        super(op);
        
    }

    /**
     * @param args
     * @param anns
     */
    public LoadGraph(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: For {@link LoadGraph}, this is the {@link URI} of the external
     * resource to be loaded.
     */
    @Override
    public ConstantNode getSourceGraph() {
        
        return (ConstantNode) getProperty(Annotations.SOURCE_GRAPH);
        
    }

    @Override
    public void setSourceGraph(final ConstantNode sourceGraph) {

        if (sourceGraph == null)
            throw new IllegalArgumentException();
        
        setProperty(Annotations.SOURCE_GRAPH, sourceGraph);
        
    }

    @Override
    public ConstantNode getTargetGraph() {
        
        return (ConstantNode) getProperty(Annotations.TARGET_GRAPH);
        
    }

    @Override
    public void setTargetGraph(final ConstantNode targetGraph) {

        if (targetGraph == null)
            throw new IllegalArgumentException();
        
        setProperty(Annotations.TARGET_GRAPH, targetGraph);
        
    }

    @Override
    public boolean isSilent() {

        return getProperty(Annotations.SILENT, Annotations.DEFAULT_SILENT);

    }

    @Override
    public void setSilent(final boolean silent) {

        setProperty(Annotations.SILENT, silent);

    }

//    LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        sb.append(indent(indent));
        
        sb.append(getUpdateType());

        if (isSilent())
            sb.append(" SILENT");

        final ConstantNode sourceGraph = getSourceGraph();
        
        final ConstantNode targetGraph = getTargetGraph();        

        if (sourceGraph != null) {
            sb.append("\n");
            sb.append(indent(indent + 1));
            sb.append("source=" + sourceGraph);
        }

        if (targetGraph != null) {
            sb.append("\n");
            sb.append(indent(indent + 1));
            sb.append("target=" + targetGraph);
        }

        sb.append("\n");

        return sb.toString();

    }

}
