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
 * Created on Mar 27, 2012
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;

/**
 * An AST node which models either {@link QuadData} or a named solution set.
 * This is used for the INSERT clause and DELETE clause of a
 * {@link DeleteInsertGraph} operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class QuadsDataOrNamedSolutionSet extends QueryNodeBase implements
        INamedSolutionSet, IProjectionDecl {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends QueryNodeBase.Annotations,
            INamedSolutionSet.Annotations, IProjectionDecl.Annotations {

        /**
         * The optional {@link QuadData}.
         */
        String QUAD_DATA = "quadData";

    }
    
    /**
     * @param op
     */
    public QuadsDataOrNamedSolutionSet(final QuadsDataOrNamedSolutionSet op) {
        
        super(op);
        
    }

    /**
     * @param args
     * @param annotations
     */
    public QuadsDataOrNamedSolutionSet(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

    }

    public QuadsDataOrNamedSolutionSet(final QuadData quadData) {

        super(BOp.NOARGS, NV.asMap(Annotations.QUAD_DATA, quadData));

    }

    public QuadsDataOrNamedSolutionSet(final String namedSet) {

        super(BOp.NOARGS, NV.asMap(Annotations.NAMED_SET, namedSet));

    }

    /**
     * Return the {@link QuadData} template.
     */
    public QuadData getQuadData() {

        return (QuadData) getProperty(Annotations.QUAD_DATA);

    }

    public void setQuadData(final QuadData data) {

        setProperty(Annotations.QUAD_DATA, data);

    }

    public String getName() {

        return (String) getProperty(Annotations.NAMED_SET);
        
    }

    public void setName(final String name) {

        setProperty(Annotations.NAMED_SET, name);
        
    }

    public void setProjection(final ProjectionNode projection) {

        setProperty(Annotations.PROJECTION, projection);

    }

    public ProjectionNode getProjection() {

        return (ProjectionNode) getProperty(Annotations.PROJECTION);

    }
    
    public Set<IVariable<?>> getProjectedVars(final Set<IVariable<?>> vars) {
        
        final ProjectionNode tmp = getProjection();
        
        if(tmp != null) {
            
            tmp.getProjectionVars(vars);
            
        }

        return vars;

    }

    @Override
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        final QuadData quadData = getQuadData();

        final String namedSolutionSet = getName();

        final ProjectionNode projection = getProjection();

        if (quadData != null) {
            sb.append("\n");
            sb.append(quadData.toString(indent));
        }

        if (namedSolutionSet != null) {
            sb.append("namedSet=" + namedSolutionSet);
        }
        if (projection != null) {
            sb.append("\n");
            sb.append(projection.toString(indent + 1));
        }

        return sb.toString();

    }

}
