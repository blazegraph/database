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

import org.openrdf.model.URI;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.RDFParserBase;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.rio.IRDFParserOptions;
import com.bigdata.rdf.rio.RDFParserOptions;

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
 */
public class LoadGraph extends GraphUpdate {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Adds options to control the behavior of the {@link RDFParser}.
     * 
     * @see RDFParserOptions
     * @see RDFParserBase
     * @see RDFParser
     */
    public interface Annotations extends GraphUpdate.Annotations{

		/*
		 * Options for RDF data parser that can be specified by SPARQL UPDATE
		 * "LOAD" extension syntax.
		 */
		String VERIFY_DATA = "verifyData";
		String PRESERVE_BLANK_NODE_IDS = "preserveBlankNodeIDs";
		String STOP_AT_FIRST_ERROR = "stopAtFirstError";
		String DATA_TYPE_HANDLING = "dataTypeHandling";
    	
    }
    
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
        
        return (ConstantNode) getProperty(Annotations.SOURCE);
        
    }

    @Override
    public void setSourceGraph(final ConstantNode sourceGraph) {

        if (sourceGraph == null)
            throw new IllegalArgumentException();
        
        setProperty(Annotations.SOURCE, sourceGraph);
        
    }

    @Override
    public ConstantNode getTargetGraph() {
        
        return (ConstantNode) getProperty(Annotations.TARGET);
        
    }

    @Override
    public void setTargetGraph(final ConstantNode targetGraph) {

        if (targetGraph == null)
            throw new IllegalArgumentException();
        
        setProperty(Annotations.TARGET, targetGraph);
        
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
    @Override
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        sb.append(indent(indent));
        
        sb.append(getUpdateType());

        final boolean silent = isSilent();
        
        final ConstantNode sourceGraph = getSourceGraph();
        
        final ConstantNode targetGraph = getTargetGraph();        

        if(silent)
            sb.append(" SILENT");

		if (getProperty(Annotations.VERIFY_DATA) != null) {
			sb.append(" " + Annotations.VERIFY_DATA + "=" + getProperty(Annotations.VERIFY_DATA));
		}
        
		if (getProperty(Annotations.PRESERVE_BLANK_NODE_IDS) != null) {
			sb.append(" " + Annotations.PRESERVE_BLANK_NODE_IDS + "=" + getProperty(Annotations.PRESERVE_BLANK_NODE_IDS));
		}
		
		if (getProperty(Annotations.STOP_AT_FIRST_ERROR) != null) {
			sb.append(" " + Annotations.STOP_AT_FIRST_ERROR + "=" + getProperty(Annotations.STOP_AT_FIRST_ERROR));
		}
		
		if (getProperty(Annotations.DATA_TYPE_HANDLING) != null) {
			sb.append(" " + Annotations.DATA_TYPE_HANDLING + "=" + getProperty(Annotations.DATA_TYPE_HANDLING));
		}

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
