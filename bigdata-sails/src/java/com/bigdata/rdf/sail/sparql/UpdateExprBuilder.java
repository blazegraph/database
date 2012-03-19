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
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;

/**
 * Extension of TupleExprBuilder that builds Update Expressions.
 * 
 * @author Jeen Broekstra
 * @author Bryan Thompson
 */
public class UpdateExprBuilder extends BigdataExprBuilder {

//    private static final Logger log = Logger.getLogger(UpdateExprBuilder.class);

//  private Set<Var> getProjectionVars(Collection<StatementPattern> statementPatterns) {
//  Set<Var> vars = new LinkedHashSet<Var>(statementPatterns.size() * 2);
//
//  for (StatementPattern sp : statementPatterns) {
//      vars.add(sp.getSubjectVar());
//      vars.add(sp.getPredicateVar());
//      vars.add(sp.getObjectVar());
//      if (sp.getContextVar() != null) {
//          vars.add(sp.getContextVar());
//      }
//  }
//
//  return vars;
//}
    
    public UpdateExprBuilder(final BigdataASTContext context) {
        
        super(context);
    }

    // TODO Is this necessary?
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
            throws VisitorException
	{

	    final InsertData op = new InsertData();

        op.setData( doQuadsData(node, data) );
        
//      TupleExpr insertExpr = graphPattern.buildTupleExpr();
//		// Retrieve all StatementPatterns from the insert expression
//		List<StatementPattern> statementPatterns = StatementPatternCollector.process(insertExpr);
//
//		Set<Var> projectionVars = getProjectionVars(statementPatterns);
//
//		// Create BNodeGenerators for all anonymous variables
//		Map<Var, ExtensionElem> extElemMap = new HashMap<Var, ExtensionElem>();
//
//		for (Var var : projectionVars) {
//			if (var.isAnonymous() && !extElemMap.containsKey(var)) {
//				ValueExpr valueExpr;
//
//				if (var.hasValue()) {
//					valueExpr = new ValueConstant(var.getValue());
//				}
//				else {
//					valueExpr = new BNodeGenerator();
//				}
//
//				extElemMap.put(var, new ExtensionElem(valueExpr, var.getName()));
//			}
//		}
//
//      TupleExpr result = new SingletonSet();
//
//		if (!extElemMap.isEmpty()) {
//			result = new Extension(result, extElemMap.values());
//		}
//
//		// Create a Projection for each StatementPattern in the clause
//		List<ProjectionElemList> projList = new ArrayList<ProjectionElemList>();
//
//		for (StatementPattern sp : statementPatterns) {
//			ProjectionElemList projElemList = new ProjectionElemList();
//
//			projElemList.addElement(new ProjectionElem(sp.getSubjectVar().getName(), "subject"));
//			projElemList.addElement(new ProjectionElem(sp.getPredicateVar().getName(), "predicate"));
//			projElemList.addElement(new ProjectionElem(sp.getObjectVar().getName(), "object"));
//
//			if (sp.getContextVar() != null) {
//				projElemList.addElement(new ProjectionElem(sp.getContextVar().getName(), "context"));
//			}
//
//			projList.add(projElemList);
//		}
//
//		if (projList.size() == 1) {
//			result = new Projection(result, projList.get(0));
//		}
//		else if (projList.size() > 1) {
//			result = new MultiProjection(result, projList);
//		}
//		else {
//			// Empty constructor
//			result = new EmptySet();
//		}
//
//		result = new Reduced(result);

//		return new InsertData(result);
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
		throws VisitorException
	{
	    
        final DeleteData op = new DeleteData();

        op.setData(doQuadsData(node, data));
	    
	    return op;

//		TupleExpr result = new SingletonSet();
//
//		// Collect construct triples
//		GraphPattern parentGP = graphPattern;
//		graphPattern = new GraphPattern();
//
//		// inherit scope & context
//		graphPattern.setStatementPatternScope(parentGP.getStatementPatternScope());
//		graphPattern.setContextVar(parentGP.getContextVar());
//
//		Object algebraExpr = node.jjtGetChild(0).jjtAccept(this, data);
//
//		if (algebraExpr instanceof ValueExpr) { // named graph identifier
//			Var contextVar = valueExpr2Var((ValueExpr)algebraExpr);
//			graphPattern.setContextVar(contextVar);
//			graphPattern.setStatementPatternScope(Scope.NAMED_CONTEXTS);
//		}
//
//		for (int i = 1; i < node.jjtGetNumChildren(); i++) {
//			node.jjtGetChild(i).jjtAccept(this, data);
//		}
//
//		TupleExpr deleteExpr = graphPattern.buildTupleExpr();
//
//		graphPattern = parentGP;
//
//		// Retrieve all StatementPatterns from the insert expression
//		List<StatementPattern> statementPatterns = StatementPatternCollector.process(deleteExpr);
//
//		Set<Var> projectionVars = getProjectionVars(statementPatterns);
//
//		// Create BNodeGenerators for all anonymous variables
//		Map<Var, ExtensionElem> extElemMap = new HashMap<Var, ExtensionElem>();
//
//		for (Var var : projectionVars) {
//			if (var.isAnonymous() && !extElemMap.containsKey(var)) {
//				ValueExpr valueExpr;
//
//				if (var.hasValue()) {
//					valueExpr = new ValueConstant(var.getValue());
//				}
//				else {
//					valueExpr = new BNodeGenerator();
//				}
//
//				extElemMap.put(var, new ExtensionElem(valueExpr, var.getName()));
//			}
//		}
//
//		if (!extElemMap.isEmpty()) {
//			result = new Extension(result, extElemMap.values());
//		}
//
//		// Create a Projection for each StatementPattern in the clause
//		List<ProjectionElemList> projList = new ArrayList<ProjectionElemList>();
//
//		for (StatementPattern sp : statementPatterns) {
//			ProjectionElemList projElemList = new ProjectionElemList();
//
//			projElemList.addElement(new ProjectionElem(sp.getSubjectVar().getName(), "subject"));
//			projElemList.addElement(new ProjectionElem(sp.getPredicateVar().getName(), "predicate"));
//			projElemList.addElement(new ProjectionElem(sp.getObjectVar().getName(), "object"));
//
//			if (sp.getContextVar() != null) {
//				projElemList.addElement(new ProjectionElem(sp.getContextVar().getName(), "context"));
//			}
//
//			projList.add(projElemList);
//		}
//
//		if (projList.size() == 1) {
//			result = new Projection(result, projList.get(0));
//		}
//		else if (projList.size() > 1) {
//			result = new MultiProjection(result, projList);
//		}
//		else {
//			// Empty constructor
//			result = new EmptySet();
//		}
//
//		result = new Reduced(result);
//
//		return new DeleteData(result);
	    
	}

//    @Override
//    public TupleExpr visit(ASTQuadsNotTriples node, Object data)
//        throws VisitorException
//    {
//        GraphPattern parentGP = graphPattern;
//        graphPattern = new GraphPattern();
//
//        ValueExpr contextNode = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, data);
//
//        Var contextVar = valueExpr2Var(contextNode);
//        graphPattern.setContextVar(contextVar);
//        graphPattern.setStatementPatternScope(Scope.NAMED_CONTEXTS);
//
//        for (int i = 1; i < node.jjtGetNumChildren(); i++) {
//            node.jjtGetChild(i).jjtAccept(this, data);
//        }
//
//        TupleExpr result = graphPattern.buildTupleExpr();
//        parentGP.addRequiredTE(result);
//
//        graphPattern = parentGP;
//
//        return result;
//    }
    
	@Override
	public QuadData visit(final ASTQuadsNotTriples node, Object data)
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
        
//		TupleExpr whereExpr = graphPattern.buildTupleExpr();
//		graphPattern = parentGP;
//
//		TupleExpr deleteExpr = whereExpr.clone();
//		Modify modify = new Modify(deleteExpr, null, whereExpr);
//
//		return modify;
	}

	@Override
	public LoadGraph visit(final ASTLoad node, Object data)
		throws VisitorException
	{

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
	public ClearGraph visit(final ASTClear node, Object data)
		throws VisitorException
	{
	
	    final ClearGraph op = new ClearGraph();
		
		if(node.isSilent())
		    op.setSilent(true);

        final ASTGraphRefAll graphRef = node.jjtGetChild(ASTGraphRefAll.class);

		if (graphRef.jjtGetNumChildren() > 0) {

		    final ConstantNode targetGraph = (ConstantNode) graphRef
                    .jjtGetChild(0).jjtAccept(this, data);
			
		    op.setTargetGraph(targetGraph);

        } else {
            
            if (graphRef.isDefault()) {
            
                op.setScope(Scope.DEFAULT_CONTEXTS);
                
            } else if (graphRef.isNamed()) {
                
                op.setScope(Scope.NAMED_CONTEXTS);
                
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

            final ConstantNode targetGraph = (ConstantNode) graphRef
                    .jjtGetChild(0).jjtAccept(this, data);
            
            op.setTargetGraph(targetGraph);

        } else {
            
            if (graphRef.isDefault()) {
            
                op.setScope(Scope.DEFAULT_CONTEXTS);
                
            } else if (graphRef.isNamed()) {
                
                op.setScope(Scope.NAMED_CONTEXTS);
                
            }

        }
        
        return op;
        
    }

    @Override
    public CreateGraph visit(final ASTCreate node, Object data)
            throws VisitorException {

        final ConstantNode targetGraph = (ConstantNode) node.jjtGetChild(0)
                .jjtAccept(this, data);

        final CreateGraph op = new CreateGraph();
        
        op.setTargetGraph(targetGraph);
        
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
                
                graphPattern.setContextVar(with);
                
                graphPattern.setStatementPatternScope(Scope.NAMED_CONTEXTS);
                
            }

        }

        {
            final ASTGraphPatternGroup whereClause = node.getWhereClause();

            if (whereClause != null) {

                graphPattern = new GroupGraphPattern();

                final GraphPatternGroup<IGroupMemberNode> ret = (GraphPatternGroup<IGroupMemberNode>) whereClause
                        .jjtAccept(this, null/* data */);

                op.setWhereClause(ret);
                
            }
            
//          ASTGraphPatternGroup whereClause = node.getWhereClause();
//                  TupleExpr where = null;
//                  if (whereClause != null) {
//                      where = (TupleExpr)whereClause.jjtAccept(this, data);
//                  }

        }

        {
            
            final ASTDeleteClause deleteNode = node.getDeleteClause();
            
            if (deleteNode != null) {
            
                final QuadData deleteClause = (QuadData) deleteNode.jjtAccept(
                        this, data);
            
                op.setDeleteClause(deleteClause);
                
            }
            
        }

        {

            final ASTInsertClause insertNode = node.getInsertClause();
            
            if (insertNode != null) {
            
                final QuadData deleteClause = (QuadData) insertNode.jjtAccept(
                        this, data);
            
                op.setInsertClause(deleteClause);
                
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
     * Collect 'QuadData' for an INSERT DATA or DELETE DATA operation. This
     * form does not allow variables in the quads data.
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

        // inherit scope & context : TODO When would it ever be non-null?
        graphPattern.setStatementPatternScope(parentGP
                .getStatementPatternScope());
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

//            final BigdataStatement spo = context.valueFactory.createStatement(
//                    (Resource) sp.s().getValue(), (URI) sp.p().getValue(),
//                    (Value) sp.o().getValue(), (Resource) (sp.c() != null ? sp
//                            .c().getValue() : null), StatementEnum.Explicit);

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
     * 
     *         TODO [data] appears to be where to put the data. Look at how this
     *         is getting passed in to the caller (ASTModify and maybe
     *         recursively in this method).
     */
    private QuadData doQuadsPatternClause(final Node node, final Object data)
            throws VisitorException {

//	    TupleExpr result = (TupleExpr)data;

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

//	@Override
//	public TupleExpr visit(ASTInsertClause node, Object data)
//		throws VisitorException
//	{
//		TupleExpr result = (TupleExpr)data;
//
//		// Collect construct triples
//		GraphPattern parentGP = graphPattern;
//		graphPattern = new GraphPattern();
//
//		// inherit scope & context
//		graphPattern.setStatementPatternScope(parentGP.getStatementPatternScope());
//		graphPattern.setContextVar(parentGP.getContextVar());
//
//		for (int i = 0; i < node.jjtGetNumChildren(); i++) {
//			node.jjtGetChild(i).jjtAccept(this, data);
//		}
//
//		TupleExpr insertExpr = graphPattern.buildTupleExpr();
//
//		graphPattern = parentGP;
//
//		return insertExpr;
//
//		/*
//		// Retrieve all StatementPatterns from the insert expression
//		List<StatementPattern> statementPatterns = StatementPatternCollector.process(insertExpr);
//
//		Set<Var> projectionVars = getProjectionVars(statementPatterns);
//
//		// Create BNodeGenerators for all anonymous variables
//		Map<Var, ExtensionElem> extElemMap = new HashMap<Var, ExtensionElem>();
//
//		for (Var var : projectionVars) {
//			if (var.isAnonymous() && !extElemMap.containsKey(var)) {
//				ValueExpr valueExpr;
//
//				if (var.hasValue()) {
//					valueExpr = new ValueConstant(var.getValue());
//				}
//				else {
//					valueExpr = new BNodeGenerator();
//				}
//
//				extElemMap.put(var, new ExtensionElem(valueExpr, var.getName()));
//			}
//		}
//
//		if (!extElemMap.isEmpty()) {
//			result = new Extension(result, extElemMap.values());
//		}
//
//		// Create a Projection for each StatementPattern in the clause
//		List<ProjectionElemList> projList = new ArrayList<ProjectionElemList>();
//
//		for (StatementPattern sp : statementPatterns) {
//			ProjectionElemList projElemList = new ProjectionElemList();
//
//			projElemList.addElement(new ProjectionElem(sp.getSubjectVar().getName(), "subject"));
//			projElemList.addElement(new ProjectionElem(sp.getPredicateVar().getName(), "predicate"));
//			projElemList.addElement(new ProjectionElem(sp.getObjectVar().getName(), "object"));
//
//			if (sp.getContextVar() != null) {
//				projElemList.addElement(new ProjectionElem(sp.getContextVar().getName(), "context"));
//			}
//
//			projList.add(projElemList);
//		}
//
//		if (projList.size() == 1) {
//			result = new Projection(result, projList.get(0));
//		}
//		else if (projList.size() > 1) {
//			result = new MultiProjection(result, projList);
//		}
//		else {
//			// Empty constructor
//			result = new EmptySet();
//		}
//
//		*/
//	}

}