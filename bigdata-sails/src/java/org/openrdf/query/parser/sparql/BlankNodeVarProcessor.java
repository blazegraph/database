/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2006.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.sparql.ast.ASTBasicGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTBlankNode;
import org.openrdf.query.parser.sparql.ast.ASTBlankNodePropertyList;
import org.openrdf.query.parser.sparql.ast.ASTCollection;
import org.openrdf.query.parser.sparql.ast.ASTOperationContainer;
import org.openrdf.query.parser.sparql.ast.ASTVar;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilderTreeConstants;
import org.openrdf.query.parser.sparql.ast.VisitorException;

/**
 * Processes blank nodes in the query body, replacing them with variables while
 * retaining scope.
 * 
 * @author Arjohn Kampman
 */
public class BlankNodeVarProcessor extends ASTVisitorBase {

	public static void process(ASTOperationContainer qc)
		throws MalformedQueryException
	{
		try {
			qc.jjtAccept(new BlankNodeToVarConverter(), null);
		}
		catch (VisitorException e) {
			throw new MalformedQueryException(e);
		}
	}

	/*-------------------------------------*
	 * Inner class BlankNodeToVarConverter *
	 *-------------------------------------*/

	private static class BlankNodeToVarConverter extends ASTVisitorBase {

		private int anonVarNo = 1;

		private Map<String, String> conversionMap = new HashMap<String, String>();

		private Set<String> usedBNodeIDs = new HashSet<String>();

		private String createAnonVarName() {
			return "-anon-" + anonVarNo++;
		}

		@Override
		public Object visit(ASTBasicGraphPattern node, Object data)
			throws VisitorException
		{
			// The same Blank node ID cannot be used across Graph Patterns
			usedBNodeIDs.addAll(conversionMap.keySet());

			// Blank nodes are scope to Basic Graph Patterns
			conversionMap.clear();

			return super.visit(node, data);
		}

		@Override
		public Object visit(ASTBlankNode node, Object data)
			throws VisitorException
		{
			String bnodeID = node.getID();
			String varName = findVarName(bnodeID);

			if (varName == null) {
				varName = createAnonVarName();

				if (bnodeID != null) {
					conversionMap.put(bnodeID, varName);
				}
			}

			ASTVar varNode = new ASTVar(SyntaxTreeBuilderTreeConstants.JJTVAR);
			varNode.setName(varName);
			varNode.setAnonymous(true);

			node.jjtReplaceWith(varNode);

			return super.visit(node, data);
		}

		private String findVarName(String bnodeID) throws VisitorException {
			if (bnodeID == null)
				return null;
			String varName = conversionMap.get(bnodeID);
			if (varName == null && usedBNodeIDs.contains(bnodeID))
				throw new VisitorException(
						"BNodeID already used in another scope: " + bnodeID);
			return varName;
		}

		@Override
		public Object visit(ASTBlankNodePropertyList node, Object data)
			throws VisitorException
		{
			node.setVarName(createAnonVarName());
			return super.visit(node, data);
		}

		@Override
		public Object visit(ASTCollection node, Object data)
			throws VisitorException
		{
			node.setVarName(createAnonVarName());
			return super.visit(node, data);
		}
	}
}
