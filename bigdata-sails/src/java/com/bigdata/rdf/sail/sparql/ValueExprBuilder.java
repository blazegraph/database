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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.FN;
import org.openrdf.query.parser.sparql.ast.ASTAbs;
import org.openrdf.query.parser.sparql.ast.ASTAggregate;
import org.openrdf.query.parser.sparql.ast.ASTAnd;
import org.openrdf.query.parser.sparql.ast.ASTAvg;
import org.openrdf.query.parser.sparql.ast.ASTBNodeFunc;
import org.openrdf.query.parser.sparql.ast.ASTBound;
import org.openrdf.query.parser.sparql.ast.ASTCeil;
import org.openrdf.query.parser.sparql.ast.ASTCoalesce;
import org.openrdf.query.parser.sparql.ast.ASTCompare;
import org.openrdf.query.parser.sparql.ast.ASTConcat;
import org.openrdf.query.parser.sparql.ast.ASTContains;
import org.openrdf.query.parser.sparql.ast.ASTCount;
import org.openrdf.query.parser.sparql.ast.ASTDatatype;
import org.openrdf.query.parser.sparql.ast.ASTDay;
import org.openrdf.query.parser.sparql.ast.ASTEncodeForURI;
import org.openrdf.query.parser.sparql.ast.ASTExistsFunc;
import org.openrdf.query.parser.sparql.ast.ASTFloor;
import org.openrdf.query.parser.sparql.ast.ASTFunctionCall;
import org.openrdf.query.parser.sparql.ast.ASTGroupConcat;
import org.openrdf.query.parser.sparql.ast.ASTGroupCondition;
import org.openrdf.query.parser.sparql.ast.ASTHours;
import org.openrdf.query.parser.sparql.ast.ASTIRIFunc;
import org.openrdf.query.parser.sparql.ast.ASTIf;
import org.openrdf.query.parser.sparql.ast.ASTIn;
import org.openrdf.query.parser.sparql.ast.ASTInfix;
import org.openrdf.query.parser.sparql.ast.ASTIsBlank;
import org.openrdf.query.parser.sparql.ast.ASTIsIRI;
import org.openrdf.query.parser.sparql.ast.ASTIsLiteral;
import org.openrdf.query.parser.sparql.ast.ASTIsNumeric;
import org.openrdf.query.parser.sparql.ast.ASTLang;
import org.openrdf.query.parser.sparql.ast.ASTLangMatches;
import org.openrdf.query.parser.sparql.ast.ASTLowerCase;
import org.openrdf.query.parser.sparql.ast.ASTMD5;
import org.openrdf.query.parser.sparql.ast.ASTMath;
import org.openrdf.query.parser.sparql.ast.ASTMax;
import org.openrdf.query.parser.sparql.ast.ASTMin;
import org.openrdf.query.parser.sparql.ast.ASTMinutes;
import org.openrdf.query.parser.sparql.ast.ASTMonth;
import org.openrdf.query.parser.sparql.ast.ASTNot;
import org.openrdf.query.parser.sparql.ast.ASTNotExistsFunc;
import org.openrdf.query.parser.sparql.ast.ASTNotIn;
import org.openrdf.query.parser.sparql.ast.ASTNow;
import org.openrdf.query.parser.sparql.ast.ASTOr;
import org.openrdf.query.parser.sparql.ast.ASTRand;
import org.openrdf.query.parser.sparql.ast.ASTRegexExpression;
import org.openrdf.query.parser.sparql.ast.ASTRound;
import org.openrdf.query.parser.sparql.ast.ASTSHA1;
import org.openrdf.query.parser.sparql.ast.ASTSHA224;
import org.openrdf.query.parser.sparql.ast.ASTSHA256;
import org.openrdf.query.parser.sparql.ast.ASTSHA384;
import org.openrdf.query.parser.sparql.ast.ASTSHA512;
import org.openrdf.query.parser.sparql.ast.ASTSameTerm;
import org.openrdf.query.parser.sparql.ast.ASTSample;
import org.openrdf.query.parser.sparql.ast.ASTSeconds;
import org.openrdf.query.parser.sparql.ast.ASTStr;
import org.openrdf.query.parser.sparql.ast.ASTStrDt;
import org.openrdf.query.parser.sparql.ast.ASTStrEnds;
import org.openrdf.query.parser.sparql.ast.ASTStrLang;
import org.openrdf.query.parser.sparql.ast.ASTStrLen;
import org.openrdf.query.parser.sparql.ast.ASTStrStarts;
import org.openrdf.query.parser.sparql.ast.ASTSubstr;
import org.openrdf.query.parser.sparql.ast.ASTSum;
import org.openrdf.query.parser.sparql.ast.ASTTimezone;
import org.openrdf.query.parser.sparql.ast.ASTTz;
import org.openrdf.query.parser.sparql.ast.ASTUpperCase;
import org.openrdf.query.parser.sparql.ast.ASTYear;
import org.openrdf.query.parser.sparql.ast.Node;
import org.openrdf.query.parser.sparql.ast.SimpleNode;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.rdf.aggregate.GROUP_CONCAT;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.ExistsNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.NotExistsNode;
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

//    private static final Logger log = Logger.getLogger(ValueExprBuilder.class);

    /**
     * Used to manage collection and nesting of graph patterns. This is mostly
     * handled by {@link GroupGraphPatternBuilder}.
     * <p>
     * Note: Both {@link ASTExistsFunc} and {@link ASTNotExistsFunc} have an
     * inner graph pattern by they appear within value expressions.
     */
    protected GroupGraphPattern graphPattern;

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

        return new FunctionNode(functionURI,
                null/* scalarValues */, new ValueExpressionNode[] {});

    }

    /**
     * Handle a simple unary function (the child of the node is the argument to
     * the function).
     */
    protected FunctionNode unary(final SimpleNode node, final URI functionURI)
            throws VisitorException {

        return new FunctionNode(functionURI,
                null/* scalarValues */,
                new ValueExpressionNode[] { left(node) });

    }

    /**
     * Handle a simple binary function (both children of the node are arguments
     * to the function).
     */
    protected FunctionNode binary(final SimpleNode node, final URI functionURI)
            throws VisitorException {

        return new FunctionNode(functionURI,
                null/* scalarValues */, new ValueExpressionNode[] { left(node),
                        right(node) });

    }

    /**
     * Handle a simple ternary function (there are three children of the node
     * which are the arguments to the function).
     */
    protected FunctionNode ternary(final SimpleNode node, final URI functionURI)
            throws VisitorException {

        return new FunctionNode(functionURI,
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

        return new FunctionNode(FunctionRegistry.COALESCE,
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
            
            return new FunctionNode(functionURI, scalarValues,
                    new ValueExpressionNode[] { new VarNode("*") });

        }

        return new FunctionNode(functionURI, scalarValues,
                new ValueExpressionNode[] { left(node) });

    }

    //
    //
    //
    
//    @Override
//    final public FunctionNode visit(ASTBind node, Object data) throws VisitorException {
//        return new AssignmentNode((VarNode) right(node), left(node));
//    }

    @Override
    final public FunctionNode visit(ASTOr node, Object data) throws VisitorException {
        return binary(node, FunctionRegistry.OR);
    }

    @Override
    final public FunctionNode visit(ASTAnd node, Object data) throws VisitorException {
        return binary(node, FunctionRegistry.AND);
    }

    @Override
    final public FunctionNode visit(ASTNot node, Object data) throws VisitorException {
        return unary(node, FunctionRegistry.NOT);
    }

    @Override
    final public FunctionNode visit(ASTCoalesce node, Object data)
            throws VisitorException {
        return nary(node, FunctionRegistry.COALESCE);
    }

    @Override
    final public FunctionNode visit(ASTCompare node, Object data)
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
    final public FunctionNode visit(ASTSubstr node, Object data)
            throws VisitorException {
        return nary(node, FN.SUBSTRING);
    }

    @Override
    final public FunctionNode visit(ASTConcat node, Object data)
            throws VisitorException {
        return nary(node, FN.CONCAT);
    }

    @Override
    final public FunctionNode visit(ASTAbs node, Object data) throws VisitorException {
        return unary(node, FN.NUMERIC_ABS);
    }

    @Override
    final public FunctionNode visit(ASTCeil node, Object data)
            throws VisitorException {
        return unary(node, FN.NUMERIC_CEIL);
    }

    @Override
    final public FunctionNode visit(ASTContains node, Object data)
            throws VisitorException {
        return binary(node, FN.CONTAINS);
    }

    @Override
    final public FunctionNode visit(ASTFloor node, Object data)
            throws VisitorException {
        return unary(node, FN.NUMERIC_FLOOR);
    }

    @Override
    final public FunctionNode visit(ASTRound node, Object data)
            throws VisitorException {
        return unary(node, FN.NUMERIC_ROUND);
    }

    @Override
    final public FunctionNode visit(ASTRand node, Object data)
            throws VisitorException {
        return noneary(node, FunctionRegistry.RAND);
    }

    @Override
    final public FunctionNode visit(ASTSameTerm node, Object data)
            throws VisitorException {
        return binary(node, FunctionRegistry.SAME_TERM);
    }

    @Override
    final public FunctionNode visit(ASTMath node, Object data)
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
    final public FunctionNode visit(ASTFunctionCall node, Object data)
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

        return new FunctionNode(functionURI,
                null/* scalarValues */, args);

    }

    @Override
    final public FunctionNode visit(ASTEncodeForURI node, Object data)
        throws VisitorException
    {
        return unary(node, FN.ENCODE_FOR_URI);
    }

    @Override
    final public FunctionNode visit(ASTStr node, Object data) throws VisitorException {
        return unary(node, FunctionRegistry.STR);
    }

    @Override
    final public FunctionNode visit(ASTStrDt node, Object data)
            throws VisitorException {
        return binary(node, FunctionRegistry.STR_DT);
    }

    @Override
    final public FunctionNode visit(ASTStrStarts node, Object data)
            throws VisitorException {
        return binary(node, FN.STARTS_WITH);
    }

    @Override
    final public FunctionNode visit(ASTStrEnds node, Object data)
            throws VisitorException {
        return binary(node, FN.ENDS_WITH);
    }

    @Override
    final public FunctionNode visit(ASTStrLen node, Object data)
            throws VisitorException {
        return unary(node, FN.STRING_LENGTH);
    }

    @Override
    final public FunctionNode visit(ASTUpperCase node, Object data)
            throws VisitorException {
        return unary(node, FN.UPPER_CASE);
    }

    @Override
    final public FunctionNode visit(ASTLowerCase node, Object data)
            throws VisitorException {
        return unary(node, FN.LOWER_CASE);
    }

    @Override
    final public FunctionNode visit(ASTStrLang node, Object data)
            throws VisitorException {
        return binary(node, FunctionRegistry.STR_LANG);
    }

    @Override
    final public FunctionNode visit(ASTNow node, Object data) throws VisitorException {
        return noneary(node, FunctionRegistry.NOW);
    }

    @Override
    final public FunctionNode visit(ASTYear node, Object data)
            throws VisitorException {
        return unary(node, FN.YEAR_FROM_DATETIME);
    }

    @Override
    final public FunctionNode visit(ASTMonth node, Object data)
            throws VisitorException {
        return unary(node, FN.MONTH_FROM_DATETIME);
    }

    @Override
    final public FunctionNode visit(ASTDay node, Object data) throws VisitorException {
        return unary(node, FN.DAY_FROM_DATETIME);
    }

    @Override
    final public FunctionNode visit(ASTHours node, Object data)
            throws VisitorException {
        return unary(node, FN.HOURS_FROM_DATETIME);
    }

    @Override
    final public FunctionNode visit(ASTMinutes node, Object data)
            throws VisitorException {
        return unary(node, FN.MINUTES_FROM_DATETIME);
    }

    @Override
    final public FunctionNode visit(ASTSeconds node, Object data)
            throws VisitorException {
        return unary(node, FN.SECONDS_FROM_DATETIME);
    }

    @Override
    final public FunctionNode visit(ASTTimezone node, Object data)
            throws VisitorException {
        return unary(node, FN.TIMEZONE_FROM_DATETIME);
    }

    @Override
    final public FunctionNode visit(ASTTz node, Object data) throws VisitorException {
        return unary(node, FunctionRegistry.TIMEZONE);
    }

    @Override
    final public FunctionNode visit(ASTMD5 node, Object data) throws VisitorException {
        return unary(node, FunctionRegistry.MD5);
    }

    @Override
    final public FunctionNode visit(ASTSHA1 node, Object data)
            throws VisitorException {
        return unary(node, FunctionRegistry.SHA1);
    }

    @Override
    final public FunctionNode visit(ASTSHA224 node, Object data)
        throws VisitorException
 {
        return unary(node, FunctionRegistry.SHA224);
    }

    @Override
    final public FunctionNode visit(ASTSHA256 node, Object data)
            throws VisitorException {
        return unary(node, FunctionRegistry.SHA256);
    }

    @Override
    final public FunctionNode visit(ASTSHA384 node, Object data)
            throws VisitorException {
        return unary(node, FunctionRegistry.SHA384);
    }

    @Override
    final public FunctionNode visit(ASTSHA512 node, Object data)
            throws VisitorException {
        return unary(node, FunctionRegistry.SHA512);
    }

    @Override
    final public FunctionNode visit(ASTIRIFunc node, Object data)
            throws VisitorException {
        return unary(node, FunctionRegistry.IRI);
    }

    @Override
    final public FunctionNode visit(ASTLang node, Object data) throws VisitorException {
        return unary(node, FunctionRegistry.LANG);
    }

    @Override
    final public FunctionNode visit(ASTDatatype node, Object data)
            throws VisitorException {
        return unary(node, FunctionRegistry.DATATYPE);
    }

    @Override
    final public FunctionNode visit(ASTLangMatches node, Object data)
            throws VisitorException {
        return binary(node, FunctionRegistry.LANG_MATCHES);
    }

    @Override
    final public FunctionNode visit(ASTBound node, Object data) throws VisitorException {
        return unary(node, FunctionRegistry.BOUND);
    }

    @Override
    final public FunctionNode visit(ASTIsIRI node, Object data)
            throws VisitorException {
        return unary(node, FunctionRegistry.IS_IRI);
    }

    @Override
    final public FunctionNode visit(ASTIsBlank node, Object data)
            throws VisitorException {
        return unary(node, FunctionRegistry.IS_BLANK);
    }

    @Override
    final public FunctionNode visit(ASTIsLiteral node, Object data)
            throws VisitorException {
        return unary(node, FunctionRegistry.IS_LITERAL);
    }

    @Override
    final public FunctionNode visit(ASTIsNumeric node, Object data)
            throws VisitorException {
        return unary(node, FunctionRegistry.IS_NUMERIC);
    }

    /** TODO Same functionURI for BNode() and BNode(Literal)? */
    @Override
    final public FunctionNode visit(ASTBNodeFunc node, Object data)
            throws VisitorException {
        if (node.jjtGetNumChildren() == 0) {
            return noneary(node, FunctionRegistry.BNODE);
        }
        return unary(node, FunctionRegistry.BNODE);
    }

    @Override
    final public FunctionNode visit(ASTRegexExpression node, Object data)
            throws VisitorException {
        if (node.jjtGetNumChildren() == 2) {
            return binary(node, FunctionRegistry.REGEX);
        }
        return ternary(node, FunctionRegistry.REGEX);
    }

    /**
     * Note: EXISTS is basically an ASK subquery.
     * 
     * @see ExistsNode
     */
    @Override
    final public ExistsNode visit(final ASTExistsFunc node, Object data)
            throws VisitorException {

        final VarNode anonvar = context.createAnonVar("-exists-");

        /*
         * Use a new (empty) graph pattern to prevent the accept of the child
         * from being attached into the parent's graph pattern context.
         */

        final GroupGraphPattern parentGP = graphPattern;

        graphPattern = new GroupGraphPattern();

        final GroupNodeBase innerGraphPattern = (GroupNodeBase) node
                .jjtGetChild(0).jjtAccept(this/* visitor */, null);

        final ExistsNode fn = new ExistsNode(anonvar,
                innerGraphPattern);

        // Restore the parent's context.
        graphPattern = parentGP;

        return fn;

    }

    /**
     * See EXISTS above.
     * 
     * @see NotExistsNode
     */
    @Override
    final public NotExistsNode visit(final ASTNotExistsFunc node, Object data)
            throws VisitorException {

        final VarNode anonvar = context.createAnonVar("-exists-");

        /*
         * Use a new (empty) graph pattern to prevent the accept of the child
         * from being attached into the parent's graph pattern context.
         */

        final GroupGraphPattern parentGP = graphPattern;

        graphPattern = new GroupGraphPattern();

        final GroupNodeBase innerGraphPattern = (GroupNodeBase) node
                .jjtGetChild(0).jjtAccept(this/* visitor */, null);

        final NotExistsNode fn = new NotExistsNode(anonvar,
                innerGraphPattern);

        // Restore the parent's context.
        graphPattern = parentGP;

        return fn;

    }

    @Override
    final public FunctionNode visit(ASTIf node, Object data) throws VisitorException {
        return ternary(node, FunctionRegistry.IF);
    }

    /**
     * Unwrap an {@link ASTInfix} node, returning the inner {@link FunctionNode}
     * constructed for it.
     * 
     * @see http://www.openrdf.org/issues/browse/SES-818
     */
    @Override
    final public FunctionNode visit(ASTInfix node, Object data) throws VisitorException {

        final Node left = node.jjtGetChild(0);

        final Node op = node.jjtGetChild(1);
        
        op.jjtInsertChild(left, 0);

        return (FunctionNode) op.jjtAccept(this, data);

    }

    /**
     * "IN" and "NOT IN" are infix notation operators. The syntax for "IN" is
     * [NumericExpression IN ArgList]. However, the IN operators declared by the
     * {@link FunctionRegistry} require that the outer NumericExpression is
     * their first argument. The function registry optimizes several different
     * cases, including where the set is empty, where it has one member, and
     * where the members of the set are constants and the outer expression is a
     * variable.
     * 
     * @see FunctionRegistry#IN
     * @see FunctionRegistry#NOT_IN
     * 
     * @see http://www.openrdf.org/issues/browse/SES-818
     */
    @Override
    final public FunctionNode visit(ASTIn node, Object data)
        throws VisitorException
    {

//        final int nargs = node.jjtGetNumChildren();
//        
//        final ValueExpressionNode[] args = new ValueExpressionNode[nargs + 1];
//
//        /*
//         * Reach up to the parent's 1st child for the left argument to the infix
//         * IN operator.
//         */
//        final ValueExpressionNode leftArg = (ValueExpressionNode) node
//                .jjtGetParent().jjtGetChild(0).jjtAccept(this, null);
//        
//        args[0] = leftArg;
//
//        /*
//         * Handle the ArgList for IN.
//         */
//
//        for (int i = 0; i < nargs; i++) {
//
//            final Node argNode = node.jjtGetChild(i);
//
//            args[i + 1] = (ValueExpressionNode) argNode.jjtAccept(this, null);
//
//        }

        /*
         * Handle the ArgList for IN.
         * 
         * Note: This assumes that the left argument of IN has been rotated into
         * place as its first child.
         * 
         * @see InfixProcessor
         */

        final int nargs = node.jjtGetNumChildren();

        final ValueExpressionNode[] args = new ValueExpressionNode[nargs];

        for (int i = 0; i < nargs; i++) {

            final Node argNode = node.jjtGetChild(i);

            args[i] = (ValueExpressionNode) argNode.jjtAccept(this, null);

        }

        return new FunctionNode(FunctionRegistry.IN,
                null/* scalarValues */, args);

    }

    /**
     * See IN above.
     */
    @Override
    final public FunctionNode visit(ASTNotIn node, Object data)
            throws VisitorException {
//
//        final int nargs = node.jjtGetNumChildren();
//
//        final ValueExpressionNode[] args = new ValueExpressionNode[nargs + 1];
//
//        /*
//         * Reach up to the parent's 1st child for the left argument to the infix
//         * NOT IN operator.
//         */
//        final ValueExpressionNode leftArg = (ValueExpressionNode) node
//                .jjtGetParent().jjtGetChild(0).jjtAccept(this, null);
//
//        args[0] = leftArg;
//
//        /*
//         * Handle the ArgList for NOT IN.
//         */
//
//        for (int i = 0; i < nargs; i++) {
//
//            final Node argNode = node.jjtGetChild(i);
//
//            args[i + 1] = (ValueExpressionNode) argNode.jjtAccept(this, null);
//
//        }
        
        /*
         * Handle the ArgList for NOT_IN.
         * 
         * Note: This assumes that the left argument of the NOT_IN has been
         * rotated into place as its first child.
         */

        final int nargs = node.jjtGetNumChildren();

        final ValueExpressionNode[] args = new ValueExpressionNode[nargs];

        for (int i = 0; i < nargs; i++) {

            final Node argNode = node.jjtGetChild(i);

            args[i] = (ValueExpressionNode) argNode.jjtAccept(this, null);

        }

        return new FunctionNode(FunctionRegistry.NOT_IN,
                null/* scalarValues */, args);

    }

    /**
     * Aggregate value expressions in GROUP BY clause. The content of this
     * production can be a {@link VarNode}, {@link AssignmentNode}, or
     * {@link FunctionNode} (which handles both built-in functions and extension
     * functions). However, we always wrap it as an {@link AssignmentNode} and
     * return that. A {@link VarNode} will just bind itself. A bare
     * {@link FunctionNode} will bind an anonymous variable.
     */
    @Override
    final public AssignmentNode visit(final ASTGroupCondition node, Object data)
            throws VisitorException {
        
        final IValueExpressionNode ve = (IValueExpressionNode) node.jjtGetChild(0)
                .jjtAccept(this, data);

        if (node.jjtGetNumChildren() == 2) {

            /*
             * For some reason, the grammar appears to have removed the "AS"
             * leaving us with just [var, expr] rather than [var, AS, expr] or
             * [AS(var,expr)] (the latter would be my preference).
             */
            
            final IValueExpressionNode ve2 = (IValueExpressionNode) node
                    .jjtGetChild(1).jjtAccept(this, data);

            return new AssignmentNode((VarNode) ve, ve2);

        }

//        if (ve instanceof AssignmentNode) {
//
//            // Already an assignment.
//            return (AssignmentNode) ve;
//
//        }

        if (ve instanceof VarNode) {

            // Assign to self.
            return new AssignmentNode((VarNode) ve, (VarNode) ve);

        }

        // Wrap with assignment to an anonymous variable.
        return new AssignmentNode(context.createAnonVar("-groupBy-"), ve);

//        TupleExpr arg = group.getArg();
//
//        Extension extension = null;
//        if (arg instanceof Extension) {
//            extension = (Extension) arg;
//        } else {
//            extension = new Extension();
//        }
//
//        String name = null;
//        ValueExpr ve = (ValueExpr) node.jjtGetChild(0).jjtAccept(this, data);
//        if (ve instanceof Var) {
//            name = ((Var) ve).getName();
//        } else {
//            if (node.jjtGetNumChildren() > 1) {
//                Var v = (Var) node.jjtGetChild(1).jjtAccept(this, data);
//                name = v.getName();
//            } else {
//                // create an alias on the spot
//                name = createConstVar(null).getName();
//            }
//
//            ExtensionElem elem = new ExtensionElem(ve, name);
//            extension.addElement(elem);
//        }
//
//        if (extension.getElements().size() > 0 && !(arg instanceof Extension)) {
//            extension.setArg(arg);
//            group.setArg(extension);
//        }

    }
    
    /*
     * Aggregate functions. Each accepts a scalar boolean "distinct" argument.
     * In addition, COUNT may be used with "*" as the inner expression.
     */
    
    @Override
    final public FunctionNode visit(ASTCount node, Object data)
            throws VisitorException {
        return aggregate(node, FunctionRegistry.COUNT);
    }

    @Override
    final public FunctionNode visit(ASTMax node, Object data) throws VisitorException {
        return aggregate(node, FunctionRegistry.MAX);
    }

    @Override
    final public FunctionNode visit(ASTMin node, Object data) throws VisitorException {
        return aggregate(node, FunctionRegistry.MIN);
    }

    @Override
    final public FunctionNode visit(ASTSum node, Object data) throws VisitorException {
        return aggregate(node, FunctionRegistry.SUM);
    }

    @Override
    final public FunctionNode visit(ASTAvg node, Object data) throws VisitorException {
        return aggregate(node, FunctionRegistry.AVERAGE);
    }

    @Override
    final public FunctionNode visit(ASTSample node, Object data) throws VisitorException {
        return aggregate(node, FunctionRegistry.SAMPLE);
    }

    /**
     * TODO additional scalar values (sparql.jjt specifies "separator EQ" as a
     * constant in the grammar, but we support additional scalar values for
     * {@link GROUP_CONCAT}. Also, the grammar is permissive and allows
     * Expression for separator rather than a quoted string (per the W3C draft).
     */
    @Override
    final public FunctionNode visit(ASTGroupConcat node, Object data)
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

        return new FunctionNode(FunctionRegistry.GROUP_CONCAT,
                scalarValues, new ValueExpressionNode[] { left(node) });

    }

}
