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
/*
 * Portions of this code are:
 *
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.sparql;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.ParsedOperation;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedUpdate;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.bigdata.bop.BOpUtility;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.sparql.ast.ASTPrefixDecl;
import com.bigdata.rdf.sail.sparql.ast.ASTQueryContainer;
import com.bigdata.rdf.sail.sparql.ast.ASTUpdate;
import com.bigdata.rdf.sail.sparql.ast.ASTUpdateContainer;
import com.bigdata.rdf.sail.sparql.ast.ASTUpdateSequence;
import com.bigdata.rdf.sail.sparql.ast.ParseException;
import com.bigdata.rdf.sail.sparql.ast.SyntaxTreeBuilder;
import com.bigdata.rdf.sail.sparql.ast.TokenMgrError;
import com.bigdata.rdf.sail.sparql.ast.VisitorException;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.Update;
import com.bigdata.rdf.sparql.ast.UpdateRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.hints.QueryHintScope;
import com.bigdata.rdf.sparql.ast.optimizers.ASTQueryHintOptimizer;

/**
 * Overridden version of the openrdf {@link SPARQLParser} class which extracts
 * additional information required by bigdata and associates it with the
 * {@link ParsedQuery} or {@link ParsedUpdate}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @openrdf
 */
public class Bigdata2ASTSPARQLParser implements QueryParser {

    private static final Logger log = Logger
            .getLogger(Bigdata2ASTSPARQLParser.class);

    static private final URI queryScope = new URIImpl(QueryHints.NAMESPACE
            + QueryHintScope.Query);

    static private final URI queryIdHint = new URIImpl(QueryHints.NAMESPACE
            + QueryHints.QUERYID);

    public Bigdata2ASTSPARQLParser() {
      
    }

    /**
     * Parse either a SPARQL QUERY or a SPARQL UPDATE request.
     * @param operation The request.
     * @param baseURI The base URI.
     * 
     * @return The {@link ParsedOperation}
     */
    public ParsedOperation parseOperation(final String operation,
            final String baseURI) throws MalformedQueryException {

        final String strippedOperation = QueryParserUtil
                .removeSPARQLQueryProlog(operation).toUpperCase();
        
        final ParsedOperation parsedOperation;
        
        if (strippedOperation.startsWith("SELECT")
                || strippedOperation.startsWith("CONSTRUCT")
                || strippedOperation.startsWith("DESCRIBE")
                || strippedOperation.startsWith("ASK")) {

            parsedOperation = parseQuery(operation, baseURI);
            
        } else {
            
            parsedOperation = parseUpdate(operation, baseURI);
            
        }

        return parsedOperation;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * The use of the alternative {@link #parseQuery2(String, String)} is
     * strongly encouraged.
     * 
     * @return An object which aligns the {@link ASTContainer} with the
     *         {@link ParsedQuery} interface.
     */
    @Override
    public BigdataParsedQuery parseQuery(final String queryStr,
            final String baseURI) throws MalformedQueryException {

        return new BigdataParsedQuery(parseQuery2(queryStr, baseURI));

    }

    /**
     * {@inheritDoc}
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/448">
     *      SPARQL 1.1 Update </a>
     */
    @Override
    public ParsedUpdate parseUpdate(final String updateStr, final String baseURI)
            throws MalformedQueryException {

        return new BigdataParsedUpdate(parseUpdate2(updateStr, baseURI));

    }

    /**
     * Parse a SPARQL 1.1 UPDATE request.
     * 
     * @return The Bigdata AST model for that request.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/448">
     *      SPARQL 1.1 Update </a>
     */
    public ASTContainer parseUpdate2(final String updateStr,
            final String baseURI) throws MalformedQueryException {

    	final long startTime = System.nanoTime();

        if (log.isInfoEnabled())
            log.info(updateStr);

        try {

            /*
             * Note: The update sequence is *above* the update container. We
             * turn the ASTUpdateSequence into an UpdateRoot and each
             * ASTUpdateContainer into a bigdata Update which is a child of that
             * UpdateRoot. The bigdata Update is an abstract class. There is a
             * concrete implementation of Update for each of the SPARQL UPDATE
             * operations (ADD, DROP, CREATE, MOVE, COPY, INSERT DATA, REMOVE
             * DATA, DELETE/INSERT, etc).
             */
            final ASTUpdateSequence updateSequence = SyntaxTreeBuilder
                    .parseUpdateSequence(updateStr);

            final UpdateRoot updateRoot = new UpdateRoot();

            final ASTContainer astContainer = new ASTContainer(updateRoot);
            
            // Set the query string on the AST.
            astContainer.setQueryString(updateStr);

            // Set the parse tree on the AST.
            astContainer.setParseTree(updateSequence);

            // Class builds bigdata Update operators from SPARQL UPDATE ops.
            final UpdateExprBuilder updateExprBuilder = new UpdateExprBuilder(
                    new BigdataASTContext(new LinkedHashMap<Value, BigdataValue>()));

            // The sequence of UPDATE operations to be processed.
            final List<ASTUpdateContainer> updateOperations = updateSequence
                    .getUpdateContainers();

            List<ASTPrefixDecl> sharedPrefixDeclarations = null;

            // For each UPDATE operation in the sequence.
            for (int i = 0; i < updateOperations.size(); i++) {

                final ASTUpdateContainer uc = updateOperations.get(i);

                if (uc.jjtGetNumChildren() == 0 && i > 0 && i < updateOperations.size() - 1) {
                    // empty update in the middle of the sequence
                    throw new MalformedQueryException("empty update in sequence not allowed");
                }

                StringEscapesProcessor.process(uc);

                BaseDeclProcessor.process(uc, baseURI);

                /*
                 * Do a special dance to handle prefix declarations in
                 * sequences: if the current operation has its own prefix
                 * declarations, use those. Otherwise, try and use prefix
                 * declarations from a previous operation in this sequence.
                 */
                final List<ASTPrefixDecl> prefixDeclList = uc
                        .getPrefixDeclList();
                {

                    if (prefixDeclList == null || prefixDeclList.isEmpty()) {
                 
                        if (sharedPrefixDeclarations != null) {
                        
                            for (final ASTPrefixDecl prefixDecl : sharedPrefixDeclarations) {
                            
                                uc.jjtAppendChild(prefixDecl);
                                
                            }

                        }
                    
                    } else {
                        
                        sharedPrefixDeclarations = prefixDeclList;

                    }
                
                }

                PrefixDeclProcessor.process(uc);

                /*
                 * Note: In the query part of an update, blank nodes are treated
                 * as anonymous vars. In the data part of the update, like in a
                 * construct node, if a blank node is seen, for each binding set
                 * in the solution list, a new blank node is generated. If it is
                 * an update, that generated bnode is stored in the server, or
                 * if it's a constructnode, that new bnode is returned as the
                 * results.
                 */
                BlankNodeVarProcessor.process(uc);

                /*
                 * Prepare deferred batch IV resolution of ASTRDFValue to BigdataValues.
                 * @see https://jira.blazegraph.com/browse/BLZG-1176
                 * 
                 * Note: IV resolution must proceed separately (or be
                 * re-attempted) for each UPDATE operation in a sequence since
                 * some operations can cause new IVs to be declared in the
                 * lexicon. Resolution before those IVs have been declared would
                 * produce a different result than resolution afterward (it will
                 * be a null IV before the Value is added to the lexicon and a
                 * TermId or BlobIV afterward).
                 * 
                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/558
                 */
                new ASTDeferredIVResolutionInitializer()
                        .process(uc);

                final ASTUpdate updateNode = uc.getUpdate();

                if (updateNode != null) {

                    /*
                     * Translate an UPDATE operation.
                     */
                    final Update updateOp = (Update) updateNode.jjtAccept(
                            updateExprBuilder, null/* data */);
                    
                    updateOp.setDatasetClauses(updateNode.getDatasetClauseList());

                    updateRoot.addChild(updateOp);

                }
                
            } // foreach

            astContainer.setQueryParseTime(System.nanoTime() - startTime);
            
            return astContainer;
            
        } catch (final ParseException e) {
            throw new MalformedQueryException(e.getMessage(), e);
        } catch (final TokenMgrError e) {
            throw new MalformedQueryException(e.getMessage(), e);
        } catch (final VisitorException e) {
            throw new MalformedQueryException(e.getMessage(), e);
        }

    }

    /**
     * Parse a SPARQL query.
     * 
     * @param queryStr
     *            The query.
     * @param baseURI
     *            The base URI.
     * 
     * @return The AST model for that query.
     * 
     * @throws MalformedQueryException
     */
    public ASTContainer parseQuery2(final String queryStr, final String baseURI)
            throws MalformedQueryException {

        final long startTime = System.nanoTime();
        
        if(log.isInfoEnabled())
            log.info(queryStr);

        try {
            
            final ASTQueryContainer qc = SyntaxTreeBuilder.parseQuery(queryStr);
            
            StringEscapesProcessor.process(qc);
            
            BaseDeclProcessor.process(qc, baseURI);
            
            final Map<String, String> prefixes = PrefixDeclProcessor.process(qc);
            
//            WildcardProjectionProcessor.process(qc);

            BlankNodeVarProcessor.process(qc);

            /*
             * Prepare deferred batch IV resolution of ASTRDFValue to BigdataValues.
             * @see https://jira.blazegraph.com/browse/BLZG-1176
             */
            final ASTDeferredIVResolutionInitializer resolver = new ASTDeferredIVResolutionInitializer();
            
            resolver.process(qc);

            /*
             * Build the bigdata AST from the parse tree.
             */
            final QueryRoot queryRoot = buildQueryModel(qc, resolver.getValues());

            final ASTContainer ast = new ASTContainer(queryRoot);
            
            // Set the query string on the AST.
            ast.setQueryString(queryStr);

            // Set the parse tree on the AST.
            ast.setParseTree(qc);

            doQueryIdHint(ast, queryRoot);
            
//            final Properties queryHints = getQueryHints(qc);
//
//            if (queryHints != null) {
//
//               queryRoot.setQueryHints(queryHints);
//
//            }

            /*
             * Attach namespace declarations.
             */
            queryRoot.setPrefixDecls(prefixes);
            
            VerifyAggregates.verifyAggregate(queryRoot);

            ast.setQueryParseTime(System.nanoTime() - startTime);
            
            return ast;

        } catch (final IllegalArgumentException e) {
        
            throw new MalformedQueryException(e.getMessage(), e);
            
        } catch (final VisitorException e) {
        
            throw new MalformedQueryException(e.getMessage(), e);
            
        } catch (final ParseException e) {
        
            throw new MalformedQueryException(e.getMessage(), e);
            
        } catch (final TokenMgrError e) {
            
            throw new MalformedQueryException(e.getMessage(), e);
            
        }

    }

    /**
     * IApplies the {@link BigdataExprBuilder} visitor to interpret the parse
     * tree, building up a bigdata {@link ASTBase AST}.
     * 
     * @param qc
     *            The root of the parse tree.
     * @param values
     *            Previously cached RDF values
     * @param context
     *            The context used to interpret that parse tree.
     * 
     * @return The root of the bigdata AST generated by interpreting the parse
     *         tree.
     * 
     * @throws MalformedQueryException
     */
    private QueryRoot buildQueryModel(final ASTQueryContainer qc,
            final Map<Value, BigdataValue> values) throws MalformedQueryException {

        final BigdataExprBuilder exprBuilder = new BigdataExprBuilder(new BigdataASTContext(values));

        try {

            return (QueryRoot) qc.jjtAccept(exprBuilder, null);

        } catch (final VisitorException e) {

            throw new MalformedQueryException(e.getMessage(), e);

        }

    }

    /**
     * Looks for the {@link QueryHints#QUERYID} and copies it to the
     * {@link ASTContainer}, which is where other code will look for a caller
     * given QueryID.
     * <p>
     * Note: This needs to be done up very early on in the processing of the
     * query since several things expect this information to already be known
     * before the query is handed over to the {@link AST2BOpUtility}.
     * 
     * @param ast
     *            The {@link ASTContainer}.
     * @param queryRoot
     *            The root of the query.
     * 
     * @throws MalformedQueryException
     * 
     *             TODO This does not actually modify the AST. It could be
     *             modified to do that, but the code would have to be robust to
     *             modification (of the AST children) during traversal. For the
     *             moment I am just leaving the query hint in place here. It
     *             will be stripped out when the {@link ASTQueryHintOptimizer}
     *             runs.
     */
    private void doQueryIdHint(final ASTContainer ast, final QueryRoot queryRoot)
            throws MalformedQueryException {

        final Iterator<StatementPatternNode> itr = BOpUtility.visitAll(
                queryRoot, StatementPatternNode.class);

        while (itr.hasNext()) {
        
            final StatementPatternNode sp = itr.next();
            
            if (queryIdHint.equals(sp.p().getValue())) {
            
                if (!queryScope.equals(sp.s().getValue())) {
                
                    throw new MalformedQueryException(QueryHints.QUERYID
                            + " must be in scope " + QueryHintScope.Query);
                
                }
                
                final String queryIdStr = sp.o().getValue().stringValue();
                
                try {
                    // Parse (validates that this is a UUID).
                    UUID.fromString(queryIdStr);
                } catch (final IllegalArgumentException ex) {
                    throw new MalformedQueryException("Not a valid UUID: "
                            + queryIdStr);
                }

                // Set the hint on the ASTContainer.
                ast.setQueryHint(QueryHints.QUERYID, queryIdStr);

                return;
            
            }
            
        }

    }
    
//    public static void main(String[] args)
//        throws java.io.IOException
//    {
//        System.out.println("Your SPARQL query:");
//
//        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
//
//        StringBuilder buf = new StringBuilder();
//        String line = null;
//        while ((line = in.readLine()) != null) {
//            if (line.length() > 0) {
//                buf.append(' ').append(line).append('\n');
//            }
//            else {
//                String queryStr = buf.toString().trim();
//                if (queryStr.length() > 0) {
//                    try {
//                        SPARQLParser parser = new SPARQLParser();
//                        parser.parseQuery(queryStr, null);
//                    }
//                    catch (Exception e) {
//                        System.err.println(e.getMessage());
//                        e.printStackTrace();
//                    }
//                }
//                buf.setLength(0);
//            }
//        }
//    }
}
