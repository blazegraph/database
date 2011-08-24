/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2006.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql;

import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.sparql.ast.ASTDescribe;
import org.openrdf.query.parser.sparql.ast.ASTDescribeQuery;
import org.openrdf.query.parser.sparql.ast.ASTProjectionElem;
import org.openrdf.query.parser.sparql.ast.ASTQuery;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ASTSelect;
import org.openrdf.query.parser.sparql.ast.ASTSelectQuery;
import org.openrdf.query.parser.sparql.ast.ASTVar;
import org.openrdf.query.parser.sparql.ast.ASTWhereClause;
import org.openrdf.query.parser.sparql.ast.Node;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilderTreeConstants;
import org.openrdf.query.parser.sparql.ast.VisitorException;

/**
 * Processes 'wildcard' projections, making them explicit by adding the
 * appropriate variable nodes to them.
 * 
 * @author arjohn
 * @author Jeen Broekstra
 */
public class WildcardProjectionProcessor extends ASTVisitorBase {

	public static void process(ASTQueryContainer qc)
		throws MalformedQueryException
	{
		ASTQuery queryNode = qc.getQuery();

		// scan for (possibly nested) select clauses
		if (queryNode != null) {
			SelectClauseCollector collector = new SelectClauseCollector();
			try {
				queryNode.jjtAccept(collector, null);

				Set<ASTSelect> selectClauses = collector.getSelectClauses();

				for (ASTSelect selectClause : selectClauses) {
					if (selectClause.isWildcard()) {
						ASTSelectQuery q = (ASTSelectQuery)selectClause.jjtGetParent();

						addQueryVars(q.getWhereClause(), selectClause);
						selectClause.setWildcard(false);
					}
				}

			}
			catch (VisitorException e) {
				throw new MalformedQueryException(e);
			}
		}

		// check for possible wildcard in DESCRIBE query
		if (queryNode instanceof ASTDescribeQuery) {
			ASTDescribeQuery describeQuery = (ASTDescribeQuery)queryNode;
			ASTDescribe describeClause = describeQuery.getDescribe();

			if (describeClause.isWildcard()) {
				addQueryVars(describeQuery.getWhereClause(), describeClause);
				describeClause.setWildcard(false);
			}
		}
	}

	private static void addQueryVars(ASTWhereClause queryBody, Node wildcardNode)
		throws MalformedQueryException
	{
		QueryVariableCollector visitor = new QueryVariableCollector();

		try {
			// Collect variable names from query
			queryBody.jjtAccept(visitor, null);

			// Adds ASTVar nodes to the ASTProjectionElem nodes and to the parent
			for (String varName : visitor.getVariableNames()) {
				ASTVar varNode = new ASTVar(SyntaxTreeBuilderTreeConstants.JJTVAR);
				ASTProjectionElem projectionElemNode = new ASTProjectionElem(
						SyntaxTreeBuilderTreeConstants.JJTPROJECTIONELEM);

				varNode.setName(varName);
				projectionElemNode.jjtAppendChild(varNode);
				varNode.jjtSetParent(projectionElemNode);

				wildcardNode.jjtAppendChild(projectionElemNode);
				projectionElemNode.jjtSetParent(wildcardNode);

			}

		}
		catch (VisitorException e) {
			throw new MalformedQueryException(e);
		}
	}

	/*------------------------------------*
	 * Inner class QueryVariableCollector *
	 *------------------------------------*/

	private static class QueryVariableCollector extends ASTVisitorBase {

		private Set<String> variableNames = new LinkedHashSet<String>();

		public Set<String> getVariableNames() {
			return variableNames;
		}

		@Override
		public Object visit(ASTVar node, Object data)
			throws VisitorException
		{
			if (!node.isAnonymous()) {
				variableNames.add(node.getName());
			}
			return super.visit(node, data);
		}
	}

	/*------------------------------------*
	 * Inner class SelectClauseCollector  *
	 *------------------------------------*/

	private static class SelectClauseCollector extends ASTVisitorBase {

		private Set<ASTSelect> selectClauses = new LinkedHashSet<ASTSelect>();

		public Set<ASTSelect> getSelectClauses() {
			return selectClauses;
		}

		@Override
		public Object visit(ASTSelect node, Object data)
			throws VisitorException
		{
			selectClauses.add(node);
			return super.visit(node, data);
		}
	}
}
