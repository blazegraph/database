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
/* Portions of this code are:
 *
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
/*
 * Created on Aug 21, 2011
 */

package com.bigdata.rdf.sail.sparql;


import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.sparql.ast.ASTBlankNode;
import com.bigdata.rdf.sail.sparql.ast.ASTFalse;
import com.bigdata.rdf.sail.sparql.ast.ASTGraphGraphPattern;
import com.bigdata.rdf.sail.sparql.ast.ASTIRI;
import com.bigdata.rdf.sail.sparql.ast.ASTNumericLiteral;
import com.bigdata.rdf.sail.sparql.ast.ASTQName;
import com.bigdata.rdf.sail.sparql.ast.ASTRDFLiteral;
import com.bigdata.rdf.sail.sparql.ast.ASTString;
import com.bigdata.rdf.sail.sparql.ast.ASTTrue;
import com.bigdata.rdf.sail.sparql.ast.ASTVar;
import com.bigdata.rdf.sail.sparql.ast.Node;
import com.bigdata.rdf.sail.sparql.ast.VisitorException;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Base class for AST visitor impls.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @openrdf
 */
public abstract class BigdataASTVisitorBase extends ASTVisitorBase {

    protected final BigdataASTContext context;

    protected BigdataASTVisitorBase(final BigdataASTContext context) {

        this.context = context;

    }

    
    /**
     * Return the depth of the node in the parse tree. The depth is ZERO (0) if
     * the node does not have a parent.
     * 
     * @param node
     *            The node.

     * @return The depth of that node.
     */
    final protected int depth(Node node) {
        
        int i = 0;
        
        while ((node = node.jjtGetParent()) != null) {
        
            i++;
            
        }
        
        return i;

    }

    /**
     * Return a white space string which may be used to indent the node to its
     * depth in the parse tree.
     * 
     * @param node
     *            The node.
     *            
     * @return The indent string.
     */
    final protected String indent(final Node node) {

        return indent(depth(node));
        
    }
    
    /**
     * Returns a string that may be used to indent a dump of the nodes in the
     * tree.
     * 
     * @param depth
     *            The indentation depth.
     * 
     * @return A string suitable for indent at that height.
     */
    protected static String indent(final int depth) {

        if (depth < 0) {

            return "";

        }

        return ws.substring(0, depth *2);

    }

    private static final transient String ws = "                                                                                                                                                                                                                  ";

    @SuppressWarnings("unchecked")
    private IV<BigdataValue, ?> makeIV(final BigdataValue value)
            throws VisitorException {

        if (value == null)
            throw new VisitorException("Value property not set?");
        
        final IV<BigdataValue,?> iv = value.getIV();
        
        if (iv == null)
            throw new VisitorException("IV not resolved : " + value);
        
        return iv;

//        IV iv = context.lexicon.getInlineIV(value);
//
//        if (iv == null) {
//
//            iv = (IV<BigdataValue, ?>) TermId.mockIV(VTE.valueOf(value));
//            
//            iv.setValue(value);
//            
//        }
//
//        return iv;

    }
    
    /*
     * Productions used by different ASTVisitor facets.
     */
    
    /**
     * Note: openrdf uses the {@link BlankNodeVarProcessor} create anonymous
     * variables from blank nodes and then flags those as anonymous variables in
     * this step.
     */
    @Override
    final public VarNode visit(final ASTVar node, final Object data)
            throws VisitorException {

        final VarNode var = new VarNode(node.getName());

        if (node.isAnonymous())
            var.setAnonymous(true);

        return var;

    }

    @Override
    final public Object visit(final ASTQName node, final Object data)
            throws VisitorException {
 
        throw new VisitorException(
                "QNames must be resolved before building the query model");
        
    }

    @Override
    final public Object visit(ASTBlankNode node, Object data)
            throws VisitorException {
        
        throw new VisitorException(
                "Blank nodes must be replaced with variables before building the query model");
    
    }

    @Override
    final public ConstantNode visit(final ASTIRI node, Object data)
            throws VisitorException {
        
        return new ConstantNode(makeIV((BigdataValue)node.getRDFValue()));
        
    }

    @Override
    final public ConstantNode visit(final ASTRDFLiteral node, Object data)
            throws VisitorException {
        
        return new ConstantNode(makeIV((BigdataValue) node.getRDFValue()));

    }

    @Override
    final public ConstantNode visit(ASTNumericLiteral node, Object data)
            throws VisitorException {

        return new ConstantNode(makeIV((BigdataValue) node.getRDFValue()));

    }

    @Override
    final public ConstantNode visit(ASTTrue node, Object data)
            throws VisitorException {

        return new ConstantNode(makeIV((BigdataValue) node.getRDFValue()));

    }

    @Override
    final public ConstantNode visit(ASTFalse node, Object data)
            throws VisitorException {

        return new ConstantNode(makeIV((BigdataValue) node.getRDFValue()));

    }

    @Override
    final public String visit(ASTString node, Object data)
            throws VisitorException {

        return node.getValue();

    }
    
    /**
     * Builds a fresh {@link GroupGraphPattern} that inherits the scope
     * for the given node. This is done by looking up the scope of the given
     * node by following its ancestor chain, to identify whether the node
     * has some named graph ancestors. If so, the scope from the enclosing
     * named graph ancestor is copied over, otherwise we're in default context.
     * 
     * @param n
     * @return
     */
    protected GroupGraphPattern scopedGroupGraphPattern(Node n)
    throws VisitorException {
       
        // extract the enclosing ASTGraphGraphPattern, if any
        ASTGraphGraphPattern scopePattern = firstASTGraphGraphAncestor(n);

        // if we're in a ASTGraphGraphPattern scope (for instance,
        // this is the case for a subquery enclosed in a graph section,
        // so we need to inherit that scope (see e.g. #832)
        if (scopePattern!=null) {
            Node child = scopePattern.jjtGetChild(0);
            if (child!=null) {
                final TermNode s = 
                    (TermNode) scopePattern.jjtGetChild(0).jjtAccept(this, null);
                   
                if (s!=null)
                    return new GroupGraphPattern(s, Scope.NAMED_CONTEXTS);
            }
        }               
         
        // otherwise, we're in default scope
        return new GroupGraphPattern();
    }
    
    
    /**
     * Returns the enclosing ASTGraphGraphPattern ancestor-or-self for the
     * given node, or null if none exists.
     * 
     * @param node node at which to start lookup
     * @return first enclosing {@link ASTGraphGraphPattern} ancestor, if any,
     *         null otherwise
     */
    protected ASTGraphGraphPattern firstASTGraphGraphAncestor(Node node) {
       if (node==null)
          return null;
       
       if (node instanceof ASTGraphGraphPattern)
          return (ASTGraphGraphPattern)node;
       
       return firstASTGraphGraphAncestor(node.jjtGetParent());
    }
}
