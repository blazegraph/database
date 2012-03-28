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
 * Created on Mar 10, 2012
 */
/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2011.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.sparql;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOpUtility;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.sail.sparql.ast.ASTAdd;
import com.bigdata.rdf.sail.sparql.ast.ASTClear;
import com.bigdata.rdf.sail.sparql.ast.ASTCopy;
import com.bigdata.rdf.sail.sparql.ast.ASTCreate;
import com.bigdata.rdf.sail.sparql.ast.ASTDeleteClause;
import com.bigdata.rdf.sail.sparql.ast.ASTDeleteData;
import com.bigdata.rdf.sail.sparql.ast.ASTDeleteWhere;
import com.bigdata.rdf.sail.sparql.ast.ASTDrop;
import com.bigdata.rdf.sail.sparql.ast.ASTGraphOrDefault;
import com.bigdata.rdf.sail.sparql.ast.ASTGraphPatternGroup;
import com.bigdata.rdf.sail.sparql.ast.ASTGraphRefAll;
import com.bigdata.rdf.sail.sparql.ast.ASTIRI;
import com.bigdata.rdf.sail.sparql.ast.ASTInsertClause;
import com.bigdata.rdf.sail.sparql.ast.ASTInsertData;
import com.bigdata.rdf.sail.sparql.ast.ASTLoad;
import com.bigdata.rdf.sail.sparql.ast.ASTModify;
import com.bigdata.rdf.sail.sparql.ast.ASTMove;
import com.bigdata.rdf.sail.sparql.ast.ASTQuadsNotTriples;
import com.bigdata.rdf.sail.sparql.ast.ASTSolutionsRef;
import com.bigdata.rdf.sail.sparql.ast.Node;
import com.bigdata.rdf.sail.sparql.ast.VisitorException;
import com.bigdata.rdf.sparql.ast.AddGraph;
import com.bigdata.rdf.sparql.ast.ClearGraph;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.CopyGraph;
import com.bigdata.rdf.sparql.ast.CreateGraph;
import com.bigdata.rdf.sparql.ast.DeleteData;
import com.bigdata.rdf.sparql.ast.DeleteInsertGraph;
import com.bigdata.rdf.sparql.ast.DropGraph;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.InsertData;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.LoadGraph;
import com.bigdata.rdf.sparql.ast.MoveGraph;
import com.bigdata.rdf.sparql.ast.QuadData;
import com.bigdata.rdf.sparql.ast.QuadsDataOrNamedSolutionSet;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;

/**
 * Extension of {@link BigdataExprBuilder} that builds Update Expressions.
 * 
 * @author Jeen Broekstra
 * @author Bryan Thompson
 */
public class UpdateExprBuilder extends BigdataExprBuilder {

//    private static final Logger log = Logger.getLogger(UpdateExprBuilder.class);

    public UpdateExprBuilder(final BigdataASTContext context) {
        
        super(context);
    }

    // TODO This is not required as far as I can tell.
//	@Override
//    public Update visit(final ASTUpdate node, final Object data)
//            throws VisitorException {
//
//        if (node instanceof ASTModify) {
//
//            return this.visit((ASTModify) node, data);
//
//        } else if (node instanceof ASTInsertData) {
//
//            return this.visit((ASTInsertData) node, data);
//
//        }
//
//		return null;
//		
//	}

    /**
     * Note: Variables in QuadDatas are disallowed in INSERT DATA requests (see
     * Notes 8 in the grammar). That is, the INSERT DATA statement only allows
     * to insert ground triples. Blank nodes in QuadDatas are assumed to be
     * disjoint from the blank nodes in the Graph Store, i.e., will be inserted
     * with "fresh" blank nodes.
     */
    @Override
    public InsertData visit(final ASTInsertData node, final Object data)
            throws VisitorException {

        final InsertData op = new InsertData();

        op.setData(doQuadsData(node, data));

        return op;

    }

    /**
     * Note: For DELETE DATA, QuadData denotes triples to be removed and is as
     * described in INSERT DATA, with the difference that in a DELETE DATA
     * operation neither variables nor blank nodes are allowed (see Notes 8+9 in
     * the grammar).
     */
    @Override
    public DeleteData visit(final ASTDeleteData node, final Object data)
            throws VisitorException {

        final DeleteData op = new DeleteData();

        op.setData(doQuadsData(node, data));

	    return op;
   
	}

	@Override
	public QuadData visit(final ASTQuadsNotTriples node, final Object data)
		throws VisitorException
	{
		
	    final GroupGraphPattern parentGP = graphPattern;
		
	    graphPattern = new GroupGraphPattern();

        final TermNode contextNode = (TermNode) node.jjtGetChild(0).jjtAccept(
                this, data);

		graphPattern.setContextVar(contextNode);

		graphPattern.setStatementPatternScope(Scope.NAMED_CONTEXTS);

        for (int i = 1; i < node.jjtGetNumChildren(); i++) {

            node.jjtGetChild(i).jjtAccept(this, data);

        }

        final QuadData group = graphPattern.buildGroup(new QuadData());

        parentGP.add(group);
        
		graphPattern = parentGP;

		return group;
		
	}

    /**
     * This handles the "DELETE WHERE" syntax short hand.
     * 
     * <pre>
     * DELETE WHERE  QuadPattern
     * </pre>
     * 
     * This is similar to a CONSTRUCT without an explicit template. The WHERE
     * clause provides both the pattern to match and the template for the quads
     * to be removed.
     */
	@Override
    public DeleteInsertGraph visit(final ASTDeleteWhere node, final Object data)
            throws VisitorException {
	    
		// Collect construct triples
		final GroupGraphPattern parentGP = graphPattern;
		graphPattern = new GroupGraphPattern();

		// inherit scope & context
		graphPattern.setStatementPatternScope(parentGP.getStatementPatternScope());
		graphPattern.setContextVar(parentGP.getContext());

		for (int i = 0; i < node.jjtGetNumChildren(); i++) {

		    node.jjtGetChild(i).jjtAccept(this, data);
		    
		}

        final JoinGroupNode whereClause = graphPattern.buildGroup(new JoinGroupNode());

        graphPattern = parentGP;

        final DeleteInsertGraph op = new DeleteInsertGraph();

        op.setWhereClause(whereClause);
        
        return op;
        
	}

	@Override
    public LoadGraph visit(final ASTLoad node, final Object data)
            throws VisitorException {

        final ConstantNode sourceGraph = (ConstantNode) node.jjtGetChild(0)
                .jjtAccept(this, data);

        final LoadGraph op = new LoadGraph();

        op.setSourceGraph(sourceGraph);

        if (node.isSilent())
            op.setSilent(true);

        if (node.jjtGetNumChildren() > 1) {

            final ConstantNode targetGraph = (ConstantNode) node.jjtGetChild(1)
                    .jjtAccept(this, data);

            op.setTargetGraph(targetGraph);

        }

        return op;

    }

    /**
     * Note: DROP and CLEAR have the identical semantics for bigdata since it
     * does not support empty graphs.
     */
	@Override
	public ClearGraph visit(final ASTClear node, final Object data)
		throws VisitorException
	{
	
        final ClearGraph op = new ClearGraph();

        if (node.isSilent())
            op.setSilent(true);

        final ASTGraphRefAll graphRef = node.jjtGetChild(ASTGraphRefAll.class);

        if (graphRef.jjtGetNumChildren() > 0) {

            final TermNode targetGraph = (TermNode) graphRef
                    .jjtGetChild(0).jjtAccept(this, data);

            if(targetGraph instanceof ConstantNode) {

                op.setTargetGraph((ConstantNode) targetGraph);
                
            } else {
                
                op.setTargetSolutionSet(targetGraph.getValueExpression()
                        .getName());
                
            }

        } else {

            if (graphRef.isDefault()) {

                op.setScope(Scope.DEFAULT_CONTEXTS);

            } else if (graphRef.isNamed()) {

                op.setScope(Scope.NAMED_CONTEXTS);

            }
            
            if(graphRef.isAllGraphs()) {
                
                op.setAllGraphs(true);
                
            }

            if(graphRef.isAllSolutions()) {
                
                op.setAllSolutionSets(true);
                
            }

        }
		
		return op;
		
	}

    /**
     * Note: DROP and CLEAR have the identical semantics for bigdata since it
     * does not support empty graphs.
     */
    @Override
    public DropGraph visit(final ASTDrop node, final Object data)
            throws VisitorException {

        final DropGraph op = new DropGraph();
        
        if(node.isSilent())
            op.setSilent(true);

        final ASTGraphRefAll graphRef = node.jjtGetChild(ASTGraphRefAll.class);

        if (graphRef.jjtGetNumChildren() > 0) {

            final TermNode targetGraph = (TermNode) graphRef
                    .jjtGetChild(0).jjtAccept(this, data);
            
            if (targetGraph instanceof ConstantNode) {
             
                op.setTargetGraph((ConstantNode) targetGraph);
                
            } else {
                
                op.setTargetSolutionSet(targetGraph.getValueExpression()
                        .getName());
                
            }

        } else {
            
            if (graphRef.isDefault()) {
            
                op.setScope(Scope.DEFAULT_CONTEXTS);
                
            } else if (graphRef.isNamed()) {
                
                op.setScope(Scope.NAMED_CONTEXTS);
                
            }

            if(graphRef.isAllGraphs()) {
                
                op.setAllGraphs(true);
                
            }

            if(graphRef.isAllSolutions()) {
                
                op.setAllSolutionSets(true);
                
            }

        }
        
        return op;
        
    }

    @Override
    public CreateGraph visit(final ASTCreate node, final Object data)
            throws VisitorException {

        final TermNode target = (TermNode) node.jjtGetChild(0).jjtAccept(this,
                data);

        final CreateGraph op = new CreateGraph();

        if (target instanceof ConstantNode) {

            op.setTargetGraph((ConstantNode) target);

        } else {

            op.setTargetSolutionSet(target.getValueExpression().getName());
            
            final ISPO[] params = doQuadsData(node, data);

            if (params != null && params.length > 0) {
                
                op.setParams(params);
                
            }
            
        }
        
        if (node.isSilent())
            op.setSilent(true);
        
        return op;
        
    }

    @Override
    public CopyGraph visit(final ASTCopy node, final Object data)
            throws VisitorException {

        final CopyGraph op = new CopyGraph();

        if (node.isSilent())
            op.setSilent(true);

        final ASTGraphOrDefault sourceNode = (ASTGraphOrDefault) node
                .jjtGetChild(0);

        if (sourceNode.jjtGetNumChildren() > 0) {

            final ConstantNode sourceGraph = (ConstantNode) sourceNode
                    .jjtGetChild(0).jjtAccept(this, data);

            op.setSourceGraph(sourceGraph);

        }

        final ASTGraphOrDefault destinationNode = (ASTGraphOrDefault) node
                .jjtGetChild(1);

        if (destinationNode.jjtGetNumChildren() > 0) {

            final ConstantNode targetGraph = (ConstantNode) destinationNode
                    .jjtGetChild(0).jjtAccept(this, data);

            op.setTargetGraph(targetGraph);

        }

        return op;

    }

    @Override
    public MoveGraph visit(final ASTMove node, final Object data)
            throws VisitorException {

        final MoveGraph op = new MoveGraph();
        
        if (node.isSilent())
            op.setSilent(true);

        final ASTGraphOrDefault sourceNode = (ASTGraphOrDefault) node
                .jjtGetChild(0);
        
        if (sourceNode.jjtGetNumChildren() > 0) {
        
            final ConstantNode sourceGraph = (ConstantNode) sourceNode
                    .jjtGetChild(0).jjtAccept(this, data);
            
            op.setSourceGraph(sourceGraph);
        
        }

        final ASTGraphOrDefault targetNode = (ASTGraphOrDefault) node
                .jjtGetChild(1);
        
        if (targetNode.jjtGetNumChildren() > 0) {
        
            final ConstantNode targetGraph = (ConstantNode) targetNode
                    .jjtGetChild(0).jjtAccept(this, data);
            
            op.setTargetGraph(targetGraph);
        }
        
        return op;

    }

    @Override
    public AddGraph visit(final ASTAdd node, final Object data)
            throws VisitorException {

        final AddGraph op = new AddGraph();

        if (node.isSilent())
            op.setSilent(true);

        final ASTGraphOrDefault sourceNode = (ASTGraphOrDefault) node
                .jjtGetChild(0);

        if (sourceNode.jjtGetNumChildren() > 0) {

            final ConstantNode sourceGraph = (ConstantNode) sourceNode
                    .jjtGetChild(0).jjtAccept(this, data);

            op.setSourceGraph(sourceGraph);

        }

        final ASTGraphOrDefault targetNode = (ASTGraphOrDefault) node
                .jjtGetChild(1);

        if (targetNode.jjtGetNumChildren() > 0) {

            final ConstantNode targetGraph = (ConstantNode) targetNode
                    .jjtGetChild(0).jjtAccept(this, data);

            op.setTargetGraph(targetGraph);

        }

        return op;

    }

    /*
     * DELETE/INSERT
     */
    
    /**
     * The DELETE/INSERT operation can be used to remove or add triples from/to
     * the Graph Store based on bindings for a query pattern specified in a
     * WHERE clause:
     * 
     * <pre>
     * ( WITH IRIref )?
     * ( ( DeleteClause InsertClause? ) | InsertClause )
     * ( USING ( NAMED )? IRIref )*
     * WHERE GroupGraphPattern
     * </pre>
     * 
     * The DeleteClause and InsertClause forms can be broken down as follows:
     * 
     * <pre>
     * DeleteClause ::= DELETE  QuadPattern 
     * InsertClause ::= INSERT  QuadPattern
     * </pre>
     */
    @Override
    public DeleteInsertGraph visit(final ASTModify node, final Object data)
            throws VisitorException
	{

        final DeleteInsertGraph op = new DeleteInsertGraph();
        
        ConstantNode with = null;
        {
            
            final ASTIRI withNode = node.getWithClause();
            
            if (withNode != null) {
                
                with = (ConstantNode) withNode.jjtAccept(this, data);
                
                /*
                 * Set the default context for the WHERE clause, DELETE clause,
                 * and/or INSERT clauser.
                 */
                
                graphPattern.setContextVar(with);
                
                graphPattern.setStatementPatternScope(Scope.NAMED_CONTEXTS);
                
            }

        }

        {
            final ASTGraphPatternGroup whereClause = node.getWhereClause();

            if (whereClause != null) {

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> ret = (GraphPatternGroup<IGroupMemberNode>) whereClause
                        .jjtAccept(this, null/* data */);

                op.setWhereClause(ret);
                
            }
            
        }

        {

            final ASTDeleteClause deleteNode = node.getDeleteClause();

            if (deleteNode != null) {

                if (deleteNode.getName() == null) {

                    // This is a QuadPattern (versus ground data)
                    final QuadData quadData = (QuadData) deleteNode
                            .jjtAccept(this, data);

                    op.setDeleteClause(new QuadsDataOrNamedSolutionSet(quadData));

                } else {

                    final QuadsDataOrNamedSolutionSet deleteClause = new QuadsDataOrNamedSolutionSet(
                            deleteNode.getName());

                    handleSelect(deleteNode.getSelect(), deleteClause);
                    
                    op.setDeleteClause(deleteClause);

                }

            }

        }

        {

            final ASTInsertClause insertNode = node.getInsertClause();

            if (insertNode != null) {

                if (insertNode.getName() == null) {

                    // This is a QuadPattern (versus ground data)
                    final QuadData quadData = (QuadData) insertNode
                            .jjtAccept(this, data);

                    op.setInsertClause(new QuadsDataOrNamedSolutionSet(quadData));

                } else {

                    final QuadsDataOrNamedSolutionSet insertClause = new QuadsDataOrNamedSolutionSet(
                            insertNode.getName());

                    handleSelect(insertNode.getSelect(), insertClause);

                    op.setInsertClause(insertClause);

                }

            }
            
        }

		return op;
		
	}

	@Override
	public QuadData visit(final ASTDeleteClause node, final Object data)
		throws VisitorException
	{

	    return doQuadsPatternClause(node, data);
	    
    }

    @Override
    public QuadData visit(final ASTInsertClause node, final Object data)
            throws VisitorException {

        return doQuadsPatternClause(node, data);

    }

    /**
     * Collect 'QuadData' for an INSERT DATA or DELETE DATA operation. This form
     * does not allow variables in the quads data.
     * <p>
     * Note: The QuadData is basically modeled by the bigdata AST as recursive
     * {@link StatementPatternNode} containers. This visits the parser AST nodes
     * and then flattens them into an {@link ISPO}[]. The {@link ISPO}s are
     * {@link BigdataStatement}s at this point, but they can be converted to
     * {@link SPO}s when the INSERT DATA or DELETE DATA operation runs.
     * 
     * @return A flat {@link ISPO}[] modeling the quad data.
     * 
     * @throws VisitorException
     */
    private ISPO[] doQuadsData(final Node node, final Object data)
            throws VisitorException {

        final GroupGraphPattern parentGP = graphPattern;
        graphPattern = new GroupGraphPattern();

        // inherit scope & context (from optional WITH clause)
        graphPattern.setStatementPatternScope(parentGP.getStatementPatternScope());
 
        graphPattern.setContextVar(parentGP.getContext());

        /*
         * Visit [QuadsData].
         * 
         * TODO What is this doing? It is making some decision based on just the
         * first child...
         */
        {

            // first child.
            final Node child0 = node.jjtGetChild(0);

            final Object algebraExpr = child0.jjtAccept(this, data);

            if (algebraExpr instanceof ConstantNode) { // named graph identifier

                final ConstantNode context = (ConstantNode) algebraExpr;
                graphPattern.setContextVar(context);
                graphPattern.setStatementPatternScope(Scope.NAMED_CONTEXTS);

            }

            // remaining children (i=1...).
            for (int i = 1; i < node.jjtGetNumChildren(); i++) {

                node.jjtGetChild(i).jjtAccept(this, data);

            }

        }

        final QuadData quadData = graphPattern.buildGroup(new QuadData());

        graphPattern = parentGP;

        /*
         * Flatten the QuadData into a simple ISPO[].
         */
        final List<ISPO> stmts = new LinkedList<ISPO>();

        final Iterator<StatementPatternNode> itr = BOpUtility.visitAll(
                quadData, StatementPatternNode.class);

        while (itr.hasNext()) {

            final StatementPatternNode sp = itr.next();

            final ISPO spo = new SPO(//
                    sp.s().getValue().getIV(), //
                    sp.p().getValue().getIV(), //
                    sp.o().getValue().getIV(),//
                    (sp.c() != null ? sp.c().getValue().getIV() : null),//
                    StatementEnum.Explicit//
            );

            stmts.add(spo);

        }

        final ISPO[] a = stmts.toArray(new ISPO[stmts.size()]);

        return a;

    }

    /**
     * Collect quads patterns for a DELETE/INSERT operation. This form allows
     * variables in the quads patterns.
     * 
     * @param data
     * 
     * @return The {@link QuadData} (aka template). The {@link QuadData} as
     *         returned by this method is not flattened, but the context has
     *         been applied within any GRAPH section.
     */
    private QuadData doQuadsPatternClause(final Node node, final Object data)
            throws VisitorException {

        // Collect construct triples
        final GroupGraphPattern parentGP = graphPattern;

        graphPattern = new GroupGraphPattern();

        // inherit scope & context
        graphPattern.setStatementPatternScope(parentGP.getStatementPatternScope());
        graphPattern.setContextVar(parentGP.getContext());

        for (int i = 0; i < node.jjtGetNumChildren(); i++) {

            node.jjtGetChild(i).jjtAccept(this, data);

        }

        final QuadData quadData = graphPattern.buildGroup(new QuadData());

        graphPattern = parentGP;

        return quadData;

    }

    /**
     * Solution set names get modeled as variables which report
     * <code>true</code> for {@link VarNode#isSolutionSet()}.
     */
    @Override
    final public VarNode visit(final ASTSolutionsRef node, final Object data)
            throws VisitorException {

        final String name = node.getName();

        final VarNode c = new VarNode(name);

        c.setSolutionSet(true);

        return c;

    }

}