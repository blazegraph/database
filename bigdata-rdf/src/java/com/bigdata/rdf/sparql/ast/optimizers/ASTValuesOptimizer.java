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
package com.bigdata.rdf.sparql.ast.optimizers;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 */
public class ASTValuesOptimizer extends AbstractJoinGroupOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTValuesOptimizer.class);

    
    
    @Override
    protected void optimizeJoinGroup(final AST2BOpContext ctx, 
            final StaticAnalysis sa,
            final IBindingSet[] bSets, final JoinGroupNode op) {
        
        for (IGroupMemberNode child : op.getChildren()) {

            if (child instanceof BindingsClause) {

                // replace the bindings clause with a named solution set ref
                // and a named subquery include
                
            }
            
        }
        
    }
    
}
