/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * @version $Id$
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

        /**
         * {@link RDFParserOptions} (optional).
         */
        String OPTIONS = "options";
        
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

    /**
     * Return the {@link RDFParserOptions}.
     * 
     * @return The {@link RDFParserOptions} -or- <code>null</code> if the
     *         options were not configured.
     */
    public IRDFParserOptions getRDFParserOptions() {

        return (IRDFParserOptions) getProperty(Annotations.OPTIONS);

    }

    /**
     * Set the {@link RDFParserOptions}.
     * 
     * @param options
     *            The options (may be <code>null</code>).
     */
    public void setRDFParserOptions(final IRDFParserOptions options) {

        setProperty(Annotations.OPTIONS, options);

    }
    
//    LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        sb.append(indent(indent));
        
        sb.append(getUpdateType());

        final boolean silent = isSilent();
        
        final ConstantNode sourceGraph = getSourceGraph();
        
        final ConstantNode targetGraph = getTargetGraph();        

        final IRDFParserOptions rdfParserOptions = getRDFParserOptions();        

        if(silent)
            sb.append(" SILENT");

        if (rdfParserOptions != null) {
            sb.append(" OPTIONS=" + rdfParserOptions);
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
