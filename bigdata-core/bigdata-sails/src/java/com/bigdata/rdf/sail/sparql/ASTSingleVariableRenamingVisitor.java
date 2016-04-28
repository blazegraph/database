
/*
 * Copyright (C) 2016 SYSTAP, LLC DBA Blazegraph
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package com.bigdata.rdf.sail.sparql;

import com.bigdata.bop.Var;
import com.bigdata.rdf.sail.sparql.ast.*;

/**
 * Recursively replaces a given variable with another given variable everywhere
 * in a visited AST. The replacement is destructive: contents of ASTVar nodes 
 * are changed.
 * @author <a href="mailto:ariazanov@blazegraph.com">Alexandre Riazanov</a>
 *
 * @since Apr 27, 2016
 */
public class ASTSingleVariableRenamingVisitor extends ASTVisitorBase {

    private final String replacedVar;
    private final String replacingVar;
    
    /**
     * The constructed object can be used to recursively replace 
     * {@link replacedVariable} with {@link replacingVariable} everywhere
     * in visited ASTs. The replacement is destructive: contents of ASTVar nodes 
     * are changed. 
     * @param replacedVariable non-null
     * @param replacingVariable non-null
     */
    public ASTSingleVariableRenamingVisitor(String replacedVariable, 
            String replacingVariable) {
        replacedVar = replacedVariable;
        replacingVar = replacingVariable;
        
        if (replacedVariable == null) {
            throw new IllegalArgumentException(); 
        }
        if (replacingVariable == null) {
            throw new IllegalArgumentException(); 
        }
    }


    @Override
    public Object visit(ASTVar node, Object data)
            throws VisitorException {
        
        if (node.getName().equals(replacedVar)) {
            node.setName(replacingVar);
        } 
                
        
        return node.childrenAccept(this, data);
    }

} // class ASTSingleVariableRenamingVisitor
