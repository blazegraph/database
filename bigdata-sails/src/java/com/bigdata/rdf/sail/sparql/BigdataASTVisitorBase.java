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


import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.sparql.ast.ASTBlankNode;
import com.bigdata.rdf.sail.sparql.ast.ASTFalse;
import com.bigdata.rdf.sail.sparql.ast.ASTIRI;
import com.bigdata.rdf.sail.sparql.ast.ASTNumericLiteral;
import com.bigdata.rdf.sail.sparql.ast.ASTQName;
import com.bigdata.rdf.sail.sparql.ast.ASTRDFLiteral;
import com.bigdata.rdf.sail.sparql.ast.ASTRDFValue;
import com.bigdata.rdf.sail.sparql.ast.ASTString;
import com.bigdata.rdf.sail.sparql.ast.ASTTrue;
import com.bigdata.rdf.sail.sparql.ast.ASTVar;
import com.bigdata.rdf.sail.sparql.ast.Node;
import com.bigdata.rdf.sail.sparql.ast.VisitorException;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Base class for AST visitor impls.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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

    /**
     * Note: The {@link BatchRDFValueResolver} is responsible for annotating the
     * {@link ASTRDFValue}s in the parse tree with {@link BigdataValue}s whose
     * {@link IV}s have been resolved against the database. That processor MUST
     * run before we build the bigdata AST from the jjtree AST nodes.
     */
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

}
