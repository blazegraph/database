/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Iterator;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Generates the {@link ProjectionNode} for a CONSTRUCT or DESCRIBE query. It is
 * populated with each variable which appears in the {@link ConstructNode}. The
 * {@link DescribeOptimizer} MUST be run first for a DESCRIBE query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ConstructOptimizer implements IASTOptimizer {

//    private static final Logger log = Logger
//            .getLogger(ConstructOptimizer.class);
    
    public ConstructOptimizer() {
    }

    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {

        if (!(queryNode instanceof QueryRoot))
            return queryNode;

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        switch (queryRoot.getQueryType()) {
        case CONSTRUCT:
            break;
        default:
            return queryRoot;
        }

        final ConstructNode constructNode = queryRoot.getConstruct();

        if (constructNode == null) {

            throw new RuntimeException("No CONSTRUCT clause?");
            
        }

        final ProjectionNode projection = new ProjectionNode();

        queryRoot.setProjection(projection); // set on the query.
        
        projection.setReduced(true);

        // Visit the distinct variables in the CONSTRUCT clause.
        final Iterator<IVariable<?>> itr = BOpUtility
                .getSpannedVariables(constructNode);

        while(itr.hasNext()) {
            
            // Add each variable to the projection.
            projection.addProjectionVar(new VarNode(itr.next().getName()));

        }
        
        return queryRoot;

    }

}
