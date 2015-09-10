/**
Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
import java.util.Properties;
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
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.IDataSetNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.Update;
import com.bigdata.rdf.sparql.ast.UpdateRoot;
import com.bigdata.rdf.sparql.ast.ASTContainer.Annotations;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.hints.QueryHintScope;
import com.bigdata.rdf.sparql.ast.optimizers.ASTQueryHintOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSetValueExpressionsOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTUnresolvedTermsOptimizer;
import com.bigdata.rdf.store.AbstractTripleStore;

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

    private final BigdataASTContext context;
	private final Properties updateProperties;

	public Bigdata2ASTSPARQLParser(AbstractTripleStore store) {
        
        this(store.getProperties(), new BigdataASTContext(store));
        
    }

    public Bigdata2ASTSPARQLParser(Properties updateProperties, BigdataASTContext context) {
        
    	this.updateProperties = updateProperties;
        this.context = context;
      
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

        long startTime = System.nanoTime();

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
                    new BigdataASTContext(new LinkedHashMap<Value, BigdataValue>(), updateProperties));

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
                        
                            for (ASTPrefixDecl prefixDecl : sharedPrefixDeclarations) {
                            
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
                 * Batch resolve ASTRDFValue to BigdataValues with their
                 * associated IVs.
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
                new BatchRDFValueResolver(true/* readOnly */)
                        .process(uc);

                final ASTUpdate updateNode = uc.getUpdate();

                if (updateNode != null) {

                    /*
                     * Translate an UPDATE operation.
                     */
                    final Update updateOp = (Update) updateNode.jjtAccept(
                            updateExprBuilder, null/* data */);
                    
                    updateOp.setUpdateContainer(uc);

                    updateRoot.addChild(updateOp);

                }
                
            } // foreach

            astContainer.setQueryParseTime(System.nanoTime() - startTime);
            return astContainer;
            
        } catch (ParseException e) {
            throw new MalformedQueryException(e.getMessage(), e);
        } catch (TokenMgrError e) {
            throw new MalformedQueryException(e.getMessage(), e);
        } catch (VisitorException e) {
            throw new MalformedQueryException(e.getMessage(), e);
        }

    }

//    public void preUpdate(AbstractTripleStore store, ASTContainer ast) throws MalformedQueryException {
//
//        UpdateRoot queryRoot = (QueryRoot)ast.getProperty(Annotations.ORIGINAL_AST);
////        Object parseTree = ast.getProperty(Annotations.PARSE_TREE);
//        if (parseTree instanceof ASTUpdateSequence) {
//            for (ASTUpdateContainer uc: ((ASTUpdateSequence)parseTree).getUpdateContainers()) {
//                preUpdate(store,ast,uc);
//            }
//            return;
//        } else if (ast instanceof UpdateRoot) {
//            ASTUpdateContainer qc = (ASTUpdateContainer)parseTree;
//            preUpdate(store,ast,qc);
//        }
//    }
    public void preUpdate(AbstractTripleStore store, ASTContainer ast) throws MalformedQueryException {

        UpdateRoot qc = (UpdateRoot)ast.getProperty(Annotations.ORIGINAL_AST);
        
        /*
         * Handle dataset declaration. It only appears for DELETE/INSERT
         * (aka ASTModify). It is attached to each DeleteInsertNode for
         * which it is given.
         */
        for (Update update: qc.getChildren()) {
            ASTUpdateContainer uc = update.getUpdateContainer();
            final DatasetNode dataSetNode = new DatasetDeclProcessor().process(uc, new BigdataASTContext(store));
            
                if (dataSetNode != null) {
        
                        /*
                         * Attach the data set (if present)
                         * 
                         * Note: The data set can only be attached to a
                         * DELETE/INSERT operation in SPARQL 1.1 UPDATE.
                         */
        
                        ((IDataSetNode) update).setDataset(dataSetNode);
        
                }
        }
        final AST2BOpContext context2 = new AST2BOpContext(ast, context.tripleStore);
        final ASTUnresolvedTermsOptimizer termsResolver = new ASTUnresolvedTermsOptimizer();
        UpdateRoot queryRoot3 = (UpdateRoot) termsResolver.optimize(context2, new QueryNodeWithBindingSet(qc, null)).getQueryNode();
        if (ast.getOriginalUpdateAST().getPrefixDecls()!=null && !ast.getOriginalUpdateAST().getPrefixDecls().isEmpty()) {
            queryRoot3.setPrefixDecls(ast.getOriginalUpdateAST().getPrefixDecls());
        }
        ast.setOriginalUpdateAST(queryRoot3);

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

        long startTime = System.nanoTime();
        
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
             * Batch resolve ASTRDFValue to BigdataValues with their associated
             * IVs.
             */
            BatchRDFValueResolver resolver = new BatchRDFValueResolver(true/* readOnly */);
            
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

        } catch (IllegalArgumentException e) {
        
            throw new MalformedQueryException(e.getMessage(), e);
            
        } catch (VisitorException e) {
        
            throw new MalformedQueryException(e.getMessage(), e);
            
        } catch (ParseException e) {
        
            throw new MalformedQueryException(e.getMessage(), e);
            
        } catch (TokenMgrError e) {
            
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
     *            Previously resolved RDF values
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

        final BigdataExprBuilder exprBuilder = new BigdataExprBuilder(new BigdataASTContext(values, updateProperties));

        try {

            return (QueryRoot) qc.jjtAccept(exprBuilder, null);

        } catch (VisitorException e) {

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
                } catch (IllegalArgumentException ex) {
                    throw new MalformedQueryException("Not a valid UUID: "
                            + queryIdStr);
                }

                // Set the hint on the ASTContainer.
                ast.setQueryHint(QueryHints.QUERYID, queryIdStr);

                return;
            
            }
            
        }

    }

    
    public void preEvaluate(AbstractTripleStore store, ASTContainer ast) throws MalformedQueryException {

        //QueryRoot queryRoot = (QueryRoot)ast.getProperty(Annotations.ORIGINAL_AST);
        ASTQueryContainer qc = (ASTQueryContainer)ast.getProperty(Annotations.PARSE_TREE);
        if (qc==null) {
            System.err.println("No parse tree" + ast);
            return;
        }
        
        BatchRDFValueResolver resolver = new BatchRDFValueResolver(true/* readOnly */);
        
        BigdataASTContext context = new BigdataASTContext(store);
        resolver.processOnPrepareEvaluate(qc, context);

        // TODO: DO NOT!
//        final QueryRoot queryRoot = buildQueryModel(ac, resolver.getValues());
        final QueryRoot queryRoot = ast.getOriginalAST();

        /*
         * Handle dataset declaration
         * 
         * Note: Filters can be attached in order to impose ACLs on the
         * query. This has to be done at the application layer at this
         * point, but it might be possible to extend the grammar for this.
         * The SPARQL end point would have to be protected from external
         * access if this were done. Perhaps the better way to do this is to
         * have the NanoSparqlServer impose the ACL filters. There also
         * needs to be an authenticated identity to make this work and that
         * could be done via an integration within the NanoSparqlServer web
         * application container.
         * 
         * Note: This handles VIRTUAL GRAPH resolution.
         */
        final DatasetNode dataSetNode = new DatasetDeclProcessor()
                .process(qc, context);

        if (dataSetNode != null) {

            queryRoot.setDataset(dataSetNode);

        }
        
        /*
         * I think here we could set the value expressions and do last- minute
         * validation.
         */
        final ASTSetValueExpressionsOptimizer opt = new ASTSetValueExpressionsOptimizer();

        final AST2BOpContext context2 = new AST2BOpContext(ast, context.tripleStore);

        final QueryRoot queryRoot2 = (QueryRoot) opt.optimize(context2, new QueryNodeWithBindingSet(queryRoot, null)).getQueryNode();

        try {

            BigdataExprBuilder.verifyAggregate(queryRoot2);

        } catch (VisitorException e) {

            throw new MalformedQueryException(e.getMessage(), e);

        }
        
        final ASTUnresolvedTermsOptimizer termsResolver = new ASTUnresolvedTermsOptimizer();
        QueryRoot queryRoot3 = (QueryRoot) termsResolver.optimize(context2, new QueryNodeWithBindingSet(queryRoot, null)).getQueryNode();
        
        queryRoot3.setPrefixDecls(ast.getOriginalAST().getPrefixDecls());
        ast.setOriginalAST(queryRoot3);
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
