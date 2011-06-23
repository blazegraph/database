/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2006.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.sparql;

import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.sparql.ast.ASTDescribe;
import org.openrdf.query.parser.sparql.ast.ASTDescribeQuery;
import org.openrdf.query.parser.sparql.ast.ASTQuery;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ASTSelect;
import org.openrdf.query.parser.sparql.ast.ASTSelectQuery;
import org.openrdf.query.parser.sparql.ast.ASTVar;
import org.openrdf.query.parser.sparql.ast.Node;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilderTreeConstants;
import org.openrdf.query.parser.sparql.ast.VisitorException;

/**
 * Processes 'wildcard' projections, making them explicit by adding the
 * appropriate variable nodes to them.
 * 
 * @author arjohn
 */
public class WildcardProjectionProcessor extends ASTVisitorBase {

    public static void process(ASTQueryContainer qc)
        throws MalformedQueryException
    {
        ASTQuery queryNode = qc.getQuery();

        if (queryNode instanceof ASTSelectQuery) {
            ASTSelect selectNode = ((ASTSelectQuery)queryNode).getSelect();

            if (selectNode.isWildcard()) {
                addQueryVars(qc, selectNode);
                selectNode.setWildcard(false);
            }
        }
        else if (queryNode instanceof ASTDescribeQuery) {
            ASTDescribe describeNode = ((ASTDescribeQuery)queryNode).getDescribe();

            if (describeNode.isWildcard()) {
                addQueryVars(qc, describeNode);
                describeNode.setWildcard(false);
            }
        }
    }

    private static void addQueryVars(ASTQueryContainer qc, Node wildcardNode)
        throws MalformedQueryException
    {
        QueryVariableCollector visitor = new QueryVariableCollector();
        try {
            // Collect variable names from query
            qc.jjtAccept(visitor, null);

            // Adds ASTVar nodes to the wildcard node
            for (String varName : visitor.getVariableNames()) {
                ASTVar varNode = new ASTVar(SyntaxTreeBuilderTreeConstants.JJTVAR);
                varNode.setName(varName);
                wildcardNode.jjtAppendChild(varNode);
                varNode.jjtSetParent(wildcardNode);
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
}
