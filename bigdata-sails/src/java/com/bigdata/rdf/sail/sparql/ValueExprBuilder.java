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
 * Created on Aug 21, 2011
 */

package com.bigdata.rdf.sail.sparql;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.openrdf.model.URI;
import org.openrdf.query.algebra.Exists;
import org.openrdf.query.algebra.Not;
import org.openrdf.query.parser.sparql.ast.ASTAggregate;
import org.openrdf.query.parser.sparql.ast.ASTAnd;
import org.openrdf.query.parser.sparql.ast.ASTAvg;
import org.openrdf.query.parser.sparql.ast.ASTBNodeFunc;
import org.openrdf.query.parser.sparql.ast.ASTBind;
import org.openrdf.query.parser.sparql.ast.ASTBlankNode;
import org.openrdf.query.parser.sparql.ast.ASTBound;
import org.openrdf.query.parser.sparql.ast.ASTCoalesce;
import org.openrdf.query.parser.sparql.ast.ASTCompare;
import org.openrdf.query.parser.sparql.ast.ASTCount;
import org.openrdf.query.parser.sparql.ast.ASTDatatype;
import org.openrdf.query.parser.sparql.ast.ASTExistsFunc;
import org.openrdf.query.parser.sparql.ast.ASTFalse;
import org.openrdf.query.parser.sparql.ast.ASTFunctionCall;
import org.openrdf.query.parser.sparql.ast.ASTGroupConcat;
import org.openrdf.query.parser.sparql.ast.ASTIRI;
import org.openrdf.query.parser.sparql.ast.ASTIRIFunc;
import org.openrdf.query.parser.sparql.ast.ASTIf;
import org.openrdf.query.parser.sparql.ast.ASTIn;
import org.openrdf.query.parser.sparql.ast.ASTIsBlank;
import org.openrdf.query.parser.sparql.ast.ASTIsIRI;
import org.openrdf.query.parser.sparql.ast.ASTIsLiteral;
import org.openrdf.query.parser.sparql.ast.ASTIsNumeric;
import org.openrdf.query.parser.sparql.ast.ASTLang;
import org.openrdf.query.parser.sparql.ast.ASTLangMatches;
import org.openrdf.query.parser.sparql.ast.ASTMath;
import org.openrdf.query.parser.sparql.ast.ASTMax;
import org.openrdf.query.parser.sparql.ast.ASTMin;
import org.openrdf.query.parser.sparql.ast.ASTNot;
import org.openrdf.query.parser.sparql.ast.ASTNotExistsFunc;
import org.openrdf.query.parser.sparql.ast.ASTNotIn;
import org.openrdf.query.parser.sparql.ast.ASTNumericLiteral;
import org.openrdf.query.parser.sparql.ast.ASTOr;
import org.openrdf.query.parser.sparql.ast.ASTQName;
import org.openrdf.query.parser.sparql.ast.ASTRDFLiteral;
import org.openrdf.query.parser.sparql.ast.ASTRegexExpression;
import org.openrdf.query.parser.sparql.ast.ASTSameTerm;
import org.openrdf.query.parser.sparql.ast.ASTSample;
import org.openrdf.query.parser.sparql.ast.ASTStr;
import org.openrdf.query.parser.sparql.ast.ASTStrDt;
import org.openrdf.query.parser.sparql.ast.ASTStrLang;
import org.openrdf.query.parser.sparql.ast.ASTString;
import org.openrdf.query.parser.sparql.ast.ASTSum;
import org.openrdf.query.parser.sparql.ast.ASTTrue;
import org.openrdf.query.parser.sparql.ast.ASTVar;
import org.openrdf.query.parser.sparql.ast.Node;
import org.openrdf.query.parser.sparql.ast.SimpleNode;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.rdf.aggregate.GROUP_CONCAT;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Visitor pattern builds {@link IValueExpressionNode}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see FunctionRegistry
 */
public class ValueExprBuilder extends BigdataASTVisitorBase {

    public ValueExprBuilder(final BigdataASTContext context) {

        super(context);
        
    }

    protected ValueExpressionNode left(final SimpleNode node)
            throws VisitorException {

        return (ValueExpressionNode) node.jjtGetChild(0).jjtAccept(this, null);

    }

    protected ValueExpressionNode right(final SimpleNode node)
            throws VisitorException {

        return (ValueExpressionNode) node.jjtGetChild(1).jjtAccept(this, null);

    }

    /**
     * Handle a simple function without any arguments.
     */
    protected FunctionNode noneary(final SimpleNode node, final URI functionURI)
            throws VisitorException {

        return new FunctionNode(context.lex, functionURI,
                null/* scalarValues */, new ValueExpressionNode[] {});

    }

    /**
     * Handle a simple unary function (the child of the node is the argument to
     * the function).
     */
    protected FunctionNode unary(final SimpleNode node, final URI functionURI)
            throws VisitorException {

        return new FunctionNode(context.lex, functionURI,
                null/* scalarValues */,
                new ValueExpressionNode[] { left(node) });

    }

    /**
     * Handle a simple binary function (both children of the node are arguments
     * to the function).
     */
    protected FunctionNode binary(final SimpleNode node, final URI functionURI)
            throws VisitorException {

        return new FunctionNode(context.lex, functionURI,
                null/* scalarValues */, new ValueExpressionNode[] { left(node),
                        right(node) });

    }

    /**
     * Handle a simple ternary function (there are three children of the node
     * which are the arguments to the function).
     */
    protected FunctionNode ternary(final SimpleNode node, final URI functionURI)
            throws VisitorException {

        return new FunctionNode(context.lex, functionURI,
                null/* scalarValues */, new ValueExpressionNode[] {
                        left(node),
                        right(node),
                        (ValueExpressionNode) node.jjtGetChild(2).jjtAccept(
                                this, null) });

    }

    /**
     * Handle a simple nary function (all children of the node are arguments to
     * the function).
     */
    protected FunctionNode nary(final SimpleNode node, final URI functionURI)
            throws VisitorException {

        final int nargs = node.jjtGetNumChildren();

        final ValueExpressionNode[] args = new ValueExpressionNode[nargs];

        for (int i = 0; i < nargs; i++) {

            final Node argNode = node.jjtGetChild(i);

            args[i] = (ValueExpressionNode) argNode.jjtAccept(this, null);

        }

        return new FunctionNode(context.lex, FunctionRegistry.COALESCE,
                null/* scalarValues */, args);

    }

    protected FunctionNode aggregate(final ASTAggregate node,
            final URI functionURI) throws VisitorException {

        Map<String, Object> scalarValues = null;

        if (node.isDistinct()) {
        
            scalarValues = Collections.singletonMap(
                    AggregateBase.Annotations.DISTINCT, (Object) Boolean.TRUE);
            
        }

        if (node instanceof ASTCount && ((ASTCount) node).isWildcard()) {

            /*
             * Note: The wildcard is dropped by openrdf.
             */
            
            return new FunctionNode(context.lex, functionURI, scalarValues,
                    new ValueExpressionNode[] { new VarNode("*") });

        }

        return new FunctionNode(context.lex, functionURI, scalarValues,
                new ValueExpressionNode[] { left(node) });

    }

    // TODO Verify with MikeP
    @SuppressWarnings("unchecked")
    protected IV<BigdataValue, ?> makeIV(final BigdataValue value) {

        IV iv = context.lexicon.getInlineIV(value);

        if (iv == null) {

            iv = (IV<BigdataValue, ?>) TermId.mockIV(VTE.valueOf(value));
            
            iv.setValue(value);
            
        }

        return iv;

    }
    
    //
    //
    //
    
    @Override
    public FunctionNode visit(ASTOr node, Object data) throws VisitorException {
        return binary(node, FunctionRegistry.OR);
    }

    @Override
    public Object visit(ASTAnd node, Object data) throws VisitorException {
        return binary(node, FunctionRegistry.AND);
    }

    @Override
    public FunctionNode visit(ASTNot node, Object data) throws VisitorException {
        return unary(node, FunctionRegistry.NOT);
    }

    @Override
    public FunctionNode visit(ASTCoalesce node, Object data)
            throws VisitorException {
        return nary(node, FunctionRegistry.COALESCE);
    }

    @Override
    public FunctionNode visit(ASTCompare node, Object data)
        throws VisitorException
    {

        final URI functionURI;
        switch (node.getOperator()) {
        case LT:
            functionURI = FunctionRegistry.LT;
            break;
        case GT:
            functionURI = FunctionRegistry.GT;
            break;
        case EQ:
            functionURI = FunctionRegistry.EQ;
            break;
        case LE:
            functionURI = FunctionRegistry.LE;
            break;
        case GE:
            functionURI = FunctionRegistry.GE;
            break;
        case NE:
            functionURI = FunctionRegistry.NE;
            break;
        default:
            throw new UnsupportedOperationException(node.getOperator()
                    .getSymbol());
        }

        return binary(node, functionURI);

    }

    @Override
    public FunctionNode visit(ASTSameTerm node, Object data)
            throws VisitorException {
        return binary(node, FunctionRegistry.SAME_TERM);
    }

    @Override
    public FunctionNode visit(ASTMath node, Object data)
        throws VisitorException
    {

        final URI functionURI;
        switch (node.getOperator()) {
        case PLUS:
            functionURI = FunctionRegistry.ADD;
            break;
        case MINUS:
            functionURI = FunctionRegistry.SUBTRACT;
            break;
        case MULTIPLY:
            functionURI = FunctionRegistry.MULTIPLY;
            break;
        case DIVIDE:
            functionURI = FunctionRegistry.DIVIDE;
            break;
        default:
            throw new UnsupportedOperationException(node.getOperator()
                    .getSymbol());
        }

        return binary(node, functionURI);

    }

    /** ASTFunctionCall (IRIRef, ArgList). */
    @Override
    public Object visit(ASTFunctionCall node, Object data)
        throws VisitorException
    {

        final ConstantNode uriNode = (ConstantNode) node.jjtGetChild(0)
                .jjtAccept(this, null);

        final BigdataURI functionURI = (BigdataURI) uriNode.getValue();

        final int nargs = node.jjtGetNumChildren() - 1;
        
        final ValueExpressionNode[] args = new ValueExpressionNode[nargs];
        
        for (int i = 0; i < nargs; i++) {
        
            final Node argNode = node.jjtGetChild(i + 1);
            
            args[i] = (ValueExpressionNode) argNode.jjtAccept(this, null);
            
        }

        return new FunctionNode(context.lex, functionURI,
                null/* scalarValues */, args);

    }

    @Override
    public FunctionNode visit(ASTStr node, Object data) throws VisitorException {
        return unary(node, FunctionRegistry.STR);
    }

    @Override
    public FunctionNode visit(ASTStrDt node, Object data)
            throws VisitorException {
        return binary(node, FunctionRegistry.STR_DT);
    }

    @Override
    public FunctionNode visit(ASTStrLang node, Object data)
            throws VisitorException {
        return binary(node, FunctionRegistry.STR_LANG);
    }

    @Override
    public FunctionNode visit(ASTIRIFunc node, Object data)
            throws VisitorException {
        return unary(node, FunctionRegistry.IRI);
    }

    @Override
    public FunctionNode visit(ASTLang node, Object data) throws VisitorException {
        return unary(node, FunctionRegistry.LANG);
    }

    @Override
    public FunctionNode visit(ASTDatatype node, Object data)
            throws VisitorException {
        return unary(node, FunctionRegistry.DATATYPE);
    }

    @Override
    public FunctionNode visit(ASTLangMatches node, Object data)
            throws VisitorException {
        return binary(node, FunctionRegistry.LANG_MATCHES);
    }

    @Override
    public FunctionNode visit(ASTBound node, Object data) throws VisitorException {
        return unary(node, FunctionRegistry.BOUND);
    }

    @Override
    public FunctionNode visit(ASTIsIRI node, Object data)
            throws VisitorException {
        return unary(node, FunctionRegistry.IS_IRI);
    }

    @Override
    public FunctionNode visit(ASTIsBlank node, Object data)
            throws VisitorException {
        return unary(node, FunctionRegistry.IS_BLANK);
    }

    @Override
    public FunctionNode visit(ASTIsLiteral node, Object data)
            throws VisitorException {
        return unary(node, FunctionRegistry.IS_LITERAL);
    }

    @Override
    public FunctionNode visit(ASTIsNumeric node, Object data)
            throws VisitorException {
        return unary(node, FunctionRegistry.IS_NUMERIC);
    }

    /** TODO Same functionURI for BNode() and BNode(Literal)? */
    @Override
    public FunctionNode visit(ASTBNodeFunc node, Object data)
            throws VisitorException {
        if (node.jjtGetNumChildren() == 0) {
            return noneary(node, FunctionRegistry.BNODE);
        }
        return unary(node, FunctionRegistry.BNODE);
    }

    @Override
    public FunctionNode visit(ASTRegexExpression node, Object data)
            throws VisitorException {
        if (node.jjtGetNumChildren() == 2) {
            return binary(node, FunctionRegistry.REGEX);
        }
        return ternary(node, FunctionRegistry.REGEX);
    }

    // FIXME EXISTS(GroupGraphPattern) is basically a subquery.
    @Override
    public Exists visit(ASTExistsFunc node, Object data)
        throws VisitorException
    {
        Exists e = new Exists();

        node.jjtGetChild(0).jjtAccept(this, e);
        return e;
    }

    // FIXME NOT EXISTS is NOT(EXISTS()).
    @Override
    public Not visit(ASTNotExistsFunc node, Object data)
        throws VisitorException
    {
        Exists e = new Exists();
        node.jjtGetChild(0).jjtAccept(this, e);
        return new Not(e);
    }

    @Override
    public FunctionNode visit(ASTIf node, Object data) throws VisitorException {
        return ternary(node, FunctionRegistry.IF);
    }

    /**
     * "IN" and "NOT IN" are infix notation operators. The syntax for "IN" is
     * [NumericExpression IN ArgList]. However, the IN operators declared by the
     * {@link FunctionRegistry} require that the outer NumericExpression is
     * their first argument.
     * <p>
     * Note: This will optimize for IN/0 (comparison with an empty list) and
     * IN/1 (comparison with a single value expression). The function registry
     * will further optimize for IN/nary, where the members of the set are
     * constants and the outer expression is a variable.
     * 
     * @see FunctionRegistry#IN
     * @see FunctionRegistry#NOT_IN
     */
    @Override
    public ValueExpressionNode visit(ASTIn node, Object data)
        throws VisitorException
    {

        final int nargs = node.jjtGetNumChildren();
        
        final ValueExpressionNode[] args = new ValueExpressionNode[nargs + 1];

        /*
         * Reach up to the parent's 1st child for the left argument to the infix
         * IN operator.
         */
        final ValueExpressionNode leftArg = (ValueExpressionNode) node
                .jjtGetParent().jjtGetChild(0).jjtAccept(this, null);
        
        args[0] = leftArg;

        /*
         * Handle the ArgList for IN.
         */

        for (int i = 0; i < nargs; i++) {

            final Node argNode = node.jjtGetChild(i);

            args[i + 1] = (ValueExpressionNode) argNode.jjtAccept(this, null);

        }

        return new FunctionNode(context.lex, FunctionRegistry.IN,
                null/* scalarValues */, args);

    }

    /**
     * See IN above.
     */
    @Override
    public ValueExpressionNode visit(ASTNotIn node, Object data)
            throws VisitorException {

        final int nargs = node.jjtGetNumChildren();

        final ValueExpressionNode[] args = new ValueExpressionNode[nargs + 1];

        /*
         * Reach up to the parent's 1st child for the left argument to the infix
         * NOT IN operator.
         */
        final ValueExpressionNode leftArg = (ValueExpressionNode) node
                .jjtGetParent().jjtGetChild(0).jjtAccept(this, null);

        args[0] = leftArg;

        /*
         * Handle the ArgList for NOT IN.
         */

        for (int i = 0; i < nargs; i++) {

            final Node argNode = node.jjtGetChild(i);

            args[i + 1] = (ValueExpressionNode) argNode.jjtAccept(this, null);

        }

        return new FunctionNode(context.lex, FunctionRegistry.NOT_IN,
                null/* scalarValues */, args);

    }

    /*
     * Note: openrdf uses the BlankNodeToVarConverter to create anonymous
     * variables from blank nodes and then flags those as anonymous variables in
     * this step. 
     */
    @Override
    public VarNode visit(ASTVar node, Object data) throws VisitorException {
        final VarNode var = new VarNode(node.getName());
        if (node.isAnonymous())
            var.setAnonymous(true);
        return var;
    }

    @Override
    public Object visit(ASTQName node, Object data) throws VisitorException {
        throw new VisitorException(
                "QNames must be resolved before building the query model");
    }

    @Override
    public Object visit(ASTBlankNode node, Object data) throws VisitorException {
        throw new VisitorException(
                "Blank nodes must be replaced with variables before building the query model");
    }

    @Override
    public Object visit(ASTBind node, Object data) throws VisitorException {
        return new AssignmentNode((VarNode) right(node), left(node));
    }

    @Override
    public ConstantNode visit(ASTIRI node, Object data) throws VisitorException {
        BigdataURI uri;
        try {
            uri = context.valueFactory.createURI(node.getValue());
        }
        catch (IllegalArgumentException e) {
            // invalid URI
            throw new VisitorException(e.getMessage());
        }

        return new ConstantNode(makeIV(uri));
    }

    @Override
    public ConstantNode visit(ASTRDFLiteral node, Object data)
        throws VisitorException
    {
        final String label = (String) node.getLabel().jjtAccept(this, null);
        final String lang = node.getLang();
        final ASTIRI datatypeNode = node.getDatatype();
        final BigdataValueFactory valueFactory = context.valueFactory;
        
        final BigdataLiteral literal;
        if (datatypeNode != null) {
            final BigdataURI datatype;
            try {
                datatype = valueFactory.createURI(datatypeNode.getValue());
            } catch (IllegalArgumentException e) {
                // invalid URI
                throw new VisitorException(e.getMessage());
            }
            literal = valueFactory.createLiteral(label, datatype);
        } else if (lang != null) {
            literal = valueFactory.createLiteral(label, lang);
        } else {
            literal = valueFactory.createLiteral(label);
        }

        return new ConstantNode(makeIV(literal));
    }

    @Override
    public ConstantNode visit(ASTNumericLiteral node, Object data)
            throws VisitorException {

        final BigdataLiteral literal = context.valueFactory.createLiteral(
                node.getValue(), node.getDatatype());
        
        return new ConstantNode(makeIV(literal));
        
    }

    @Override
    public ConstantNode visit(ASTTrue node, Object data)
            throws VisitorException {
        
        return new ConstantNode(
                makeIV(context.valueFactory.createLiteral(true)));
        
    }

    @Override
    public ConstantNode visit(ASTFalse node, Object data)
            throws VisitorException {
        
        return new ConstantNode(
                makeIV(context.valueFactory.createLiteral(false)));
        
    }

    @Override
    public String visit(ASTString node, Object data) throws VisitorException {
        return node.getValue();
    }

    /*
     * Aggregate functions. Each accepts a scalar boolean "distinct" argument.
     * In addition, COUNT may be used with "*" as the inner expression.
     */
    
    @Override
    public FunctionNode visit(ASTCount node, Object data)
            throws VisitorException {
        return aggregate(node, FunctionRegistry.COUNT);
    }

    @Override
    public Object visit(ASTMax node, Object data) throws VisitorException {
        return aggregate(node, FunctionRegistry.MAX);
    }

    @Override
    public Object visit(ASTMin node, Object data) throws VisitorException {
        return aggregate(node, FunctionRegistry.MIN);
    }

    @Override
    public Object visit(ASTSum node, Object data) throws VisitorException {
        return aggregate(node, FunctionRegistry.SUM);
    }

    @Override
    public Object visit(ASTAvg node, Object data) throws VisitorException {
        return aggregate(node, FunctionRegistry.AVERAGE);
    }

    @Override
    public Object visit(ASTSample node, Object data) throws VisitorException {
        return aggregate(node, FunctionRegistry.SAMPLE);
    }

    /**
     * TODO additional scalar values (sparql.jjt specifies "separator EQ" as a
     * constant in the grammar, but we support additional scalar values for
     * {@link GROUP_CONCAT}. Also, the grammar is permissive and allows
     * Expression for separator rather than a quoted string (per the W3C draft).
     */
    @Override
    public Object visit(ASTGroupConcat node, Object data)
        throws VisitorException
    {

        Map<String, Object> scalarValues = new LinkedHashMap<String, Object>();

        if (node.isDistinct()) {

            scalarValues.put(AggregateBase.Annotations.DISTINCT, Boolean.TRUE);

        }

        if (node.jjtGetNumChildren() > 1) {

            final ConstantNode separator = (ConstantNode) node.jjtGetChild(1)
                    .jjtAccept(this, data);

            scalarValues.put(GROUP_CONCAT.Annotations.SEPARATOR, separator
                    .getValue().stringValue());

        }

        return new FunctionNode(context.lex, FunctionRegistry.GROUP_CONCAT,
                scalarValues, new ValueExpressionNode[] { left(node) });

    }

}
