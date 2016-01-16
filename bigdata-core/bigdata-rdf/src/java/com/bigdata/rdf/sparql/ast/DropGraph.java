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

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;

/**
 * The DROP operation removes the specified graph(s) from the Graph Store.
 * 
 * <pre>
 * DROP  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL | GRAPHS | SOLUTIONS | SOLUTIONS %VARNAME)
 * </pre>
 * 
 * Note: Bigdata does not support empty graphs, so DROP and CLEAR have identical
 * semantics.
 * 
 * @see http://www.w3.org/TR/sparql11-update/#drop
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DropGraph extends AbstractOneGraphManagement {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends AbstractOneGraphManagement.Annotations {

        /**
         * Boolean property (default {@value #DEFAULT_ALL_GRAPHS}) which is
         * <code>true</code> iff ALL GRAPHS should be effected by this
         * operation.
         */
        String ALL_GRAPHS = "allGraphs";

        boolean DEFAULT_ALL_GRAPHS = false;
        
        /**
         * Boolean property (default {@value #DEFAULT_ALL_GRAPHS}) which is
         * <code>true</code> iff ALL SOLUTION SETS should be effected by this
         * operation.
         */
        String ALL_SOLUTION_SETS = "allSolutionSets";

        boolean DEFAULT_ALL_SOLUTION_SETS = false;

    }
    
    /**
     * Used by {@link ClearGraph}.
     */
    protected DropGraph(final UpdateType updateType) {

        super(updateType);
        
    }

    public DropGraph() {
        super(UpdateType.Drop);
    }

    /**
     * @param op
     */
    public DropGraph(final DropGraph op) {
        
        super(op);
        
    }

    /**
     * @param args
     * @param anns
     */
    public DropGraph(final BOp[] args, final Map<String, Object> anns) {
        
        super(args, anns);
        
    }

    /**
     * Return <code>true</code> IFF this operation should effect ALL GRAPHS.
     */
    public boolean isAllGraphs() {

        return getProperty(Annotations.ALL_GRAPHS,
                Annotations.DEFAULT_ALL_GRAPHS);
        
    }

    public void setAllGraphs(final boolean allGraphs) {

        setProperty(Annotations.ALL_GRAPHS, allGraphs);
        
    }
    
    /**
     * Return <code>true</code> IFF this operation should effect ALL SOLUTION
     * SETS.
     */
    public boolean isAllSolutionSets() {

        return getProperty(Annotations.ALL_SOLUTION_SETS,
                Annotations.DEFAULT_ALL_SOLUTION_SETS);
        
    }

    public void setAllSolutionSets(final boolean allSolutionSets) {

        setProperty(Annotations.ALL_SOLUTION_SETS, allSolutionSets);
        
    }
    
    public Scope getScope() {

        return (Scope) getProperty(Annotations.SCOPE);

    }

    public void setScope(final Scope scope) {

        setProperty(Annotations.SCOPE, scope);

    }

}
