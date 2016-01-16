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
 * Created on Aug 24, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.spo.ISPO;

/**
 * A template for the construction of one or more graphs based on the solutions
 * projected by a query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ConstructNode extends AbstractStatementContainer<StatementPatternNode>
//GroupNodeBase<StatementPatternNode>
        implements IStatementContainer {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends AbstractStatementContainer.Annotations {

        /**
         * Boolean property (default {@value #DEFAULT_NATIVE_DISTINCT}) which is
         * <code>true</code> iff a native DISTINCT {@link ISPO} filter should be
         * applied (large cardinality is expected for the constructed graph).
         * When <code>false</code>, a JVM based DISTINCT {@link ISPO} filter
         * will be applied.
         * <p>
         * Note: This can be set using either {@link QueryHints#ANALYTIC} or
         * {@link QueryHints#NATIVE_DISTINCT_SPO}.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/579">
         *      CONSTRUCT should apply DISTINCT (s,p,o) filter </a>
         */
        String NATIVE_DISTINCT = "nativeDistinct";

        /**
         * Internal boolean property (default {@value #DEFAULT_DISTINCT_QUDS}) which is
         * <code>true</code> when used internally for constructing sets of quads rather 
         * than sets of triples.
         * <p>
         * Note: This can only be set programmatically using {@link ConstructNode#setDistinctQuads(boolean)}
         * <p>
         * Note: When this property is set and mixed quads are detected or suspected then the {@link #NATIVE_DISTINCT} property is ignored.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/579">
         *      CONSTRUCT should apply DISTINCT (s,p,o) filter </a>
         */
        String DISTINCT_QUADS = "quadsDistinct";

        boolean DEFAULT_NATIVE_DISTINCT = false;
        
        boolean DEFAULT_DISTINCT_QUADS = false;

    }

    public ConstructNode() {

        super();
        
    }

    public ConstructNode(final AST2BOpContext ctx) {

        super();
        
        if (ctx.nativeDistinctSPO) {
         
            // Native DISTINCT SPO FILTER is requested.
            
            setNativeDistinct(true);
            
        }

    }
    
    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public ConstructNode(final ConstructNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public ConstructNode(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }
    
    @Override
    public IGroupNode<StatementPatternNode> addChild(
            final StatementPatternNode child) {

        if (!(child instanceof StatementPatternNode))
            throw new UnsupportedOperationException();
        
        super.addChild(child);
        
        return this;
    }

    /**
     * When <code>true</code>, a native DISTINCT {@link ISPO} filter will be
     * applied to the constructed graph, otherwise a Java Heap based DISTINCT
     * {@link ISPO} filter will be applied.
     * 
     * @see Annotations#NATIVE_DISTINCT
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/579">
     *      CONSTRUCT should apply DISTINCT (s,p,o) filter </a>
     */
    public boolean isNativeDistinct() {

        return getProperty(Annotations.NATIVE_DISTINCT,
                Annotations.DEFAULT_NATIVE_DISTINCT);

    }

    public void setNativeDistinct(final boolean nativeDistinct) {
        
        setProperty(Annotations.NATIVE_DISTINCT, nativeDistinct);
        
    }
    
    public boolean isDistinctQuads() {
    	
        return getProperty(Annotations.DISTINCT_QUADS,
                Annotations.DEFAULT_DISTINCT_QUADS);
    	
    }
	public void setDistinctQuads(final boolean quads) {
		
        setProperty(Annotations.DISTINCT_QUADS, quads);
	}
    
    @Override
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        final String s = indent(indent);
        
        sb.append("\n").append(s).append("CONSTRUCT {");

        for (StatementPatternNode v : this) {

            sb.append(v.toString(indent+1));

        }

        sb.append("\n").append(s).append("}");

        if (isNativeDistinct())
            sb.append(" [nativeDistinct]");

        return sb.toString();

    }


}
