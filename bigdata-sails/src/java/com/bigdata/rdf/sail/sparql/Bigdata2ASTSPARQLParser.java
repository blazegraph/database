/**
Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Portions of this code are:
 *
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.sparql;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.UpdateExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedUpdate;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.BaseDeclProcessor;
import org.openrdf.query.parser.sparql.BlankNodeVarProcessor;
import org.openrdf.query.parser.sparql.DatasetDeclProcessor;
import org.openrdf.query.parser.sparql.PrefixDeclProcessor;
import org.openrdf.query.parser.sparql.StringEscapesProcessor;
import org.openrdf.query.parser.sparql.UpdateExprBuilder;
import org.openrdf.query.parser.sparql.ast.ASTPrefixDecl;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ASTUpdate;
import org.openrdf.query.parser.sparql.ast.ASTUpdateContainer;
import org.openrdf.query.parser.sparql.ast.ASTUpdateSequence;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import com.bigdata.rdf.sail.IBigdataParsedQuery;
import com.bigdata.rdf.sail.QueryHints;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Overridden version of the openrdf 2.3 SPARQLParser class which extracts
 * additional information required by bigdata and associates it with the
 * {@link ParsedQuery}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: BigdataSPARQLParser.java 4793 2011-06-24 17:29:25Z thompsonbry
 *          $
 * 
 *          TODO Update the BigdataSPARQLSyntaxTest to use this version of the
 *          parser.  That will verify parser compliance against some manifest
 *          driven test suites.
 */
public class Bigdata2ASTSPARQLParser implements QueryParser {

    private final BigdataASTContext context;

    public Bigdata2ASTSPARQLParser(final AbstractTripleStore tripleStore) {
        
        this.context = new BigdataASTContext(tripleStore);
        
    }

    /*
     * FIXME SPARQL 1.1 UPDATE
     */
    public ParsedUpdate parseUpdate(String updateStr, String baseURI)
            throws MalformedQueryException
        {
            try {

                ParsedUpdate update = new ParsedUpdate();

                ASTUpdateSequence updateSequence = SyntaxTreeBuilder.parseUpdateSequence(updateStr);

                List<ASTUpdateContainer> updateOperations = updateSequence.getUpdateContainers();

                List<ASTPrefixDecl> sharedPrefixDeclarations = null;
                
                for (ASTUpdateContainer uc : updateOperations) {

                    
                    StringEscapesProcessor.process(uc);
                    BaseDeclProcessor.process(uc, baseURI);
                    
                    // do a special dance to handle prefix declarations in sequences: if the current
                    // operation has its own prefix declarations, use those. Otherwise, try and use
                    // prefix declarations from a previous operation in this sequence.
                    List<ASTPrefixDecl> prefixDeclList = uc.getPrefixDeclList();
                    if (prefixDeclList == null || prefixDeclList.size() == 0) {
                        if (sharedPrefixDeclarations != null) {
                            for (ASTPrefixDecl prefixDecl: sharedPrefixDeclarations) {
                                uc.jjtAppendChild(prefixDecl);
                            }
                        }
                    }
                    else {
                        sharedPrefixDeclarations = prefixDeclList;
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

                    UpdateExprBuilder updateExprBuilder = new UpdateExprBuilder(new ValueFactoryImpl());

                    // Handle dataset declaration
                    Dataset dataset = DatasetDeclProcessor.process(uc);
                    if (dataset != null) {
                        update.setDataset(dataset);
                    }

                    ASTUpdate updateNode = uc.getUpdate();
                    update.addUpdateExpr((UpdateExpr)updateNode.jjtAccept(updateExprBuilder, null));
                }
                
                return update;
            }
            catch (ParseException e) {
                throw new MalformedQueryException(e.getMessage(), e);
            }
            catch (TokenMgrError e) {
                throw new MalformedQueryException(e.getMessage(), e);
            }
            catch (VisitorException e) {
                throw new MalformedQueryException(e.getMessage(), e);
            }

        }

    /**
     * {@inheritDoc}
     * 
     * @return An object which implements {@link IBigdataParsedQuery}.
     *         Additional information is available by casting the returned
     *         object to that interface.
     */
    public BigdataParsedQuery parseQuery(final String queryStr, final String baseURI)
            throws MalformedQueryException {

        final QueryRoot queryRoot = parseQuery2(queryStr, baseURI);
        
        return new BigdataParsedQuery(queryRoot);
        
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
    public QueryRoot parseQuery2(final String queryStr, final String baseURI)
            throws MalformedQueryException {
        
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
            
            new BatchRDFValueResolver(context).process(qc);
            
            /*
             * Build the bigdata AST from the parse tree.
             */
            final QueryRoot queryRoot = buildQueryModel(qc, context);

            // Set the query string on the AST.
            queryRoot.setQueryString(queryStr);

            // Set the parse tree on the AST.
            queryRoot.setParseTree(qc);

            final Properties queryHints = getQueryHints(qc);

            if (queryHints != null) {

                queryRoot.setQueryHints(queryHints);

            }

            /*
             * Attach namespace declarations.
             */
            queryRoot.setPrefixDecls(prefixes);
            
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
             */
            final Dataset dataset = DatasetDeclProcessor.process(qc);
            
            if (dataset != null) {
            
                queryRoot.setDataset(new DatasetNode(dataset));
                
            }

            return queryRoot;

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
     * @param context
     *            The context used to interpret that parse tree.
     * 
     * @return The root of the bigdata AST generated by interpreting the parse
     *         tree.
     * 
     * @throws MalformedQueryException
     */
    private QueryRoot buildQueryModel(final ASTQueryContainer qc,
            final BigdataASTContext context) throws MalformedQueryException {

        final BigdataExprBuilder exprBuilder = new BigdataExprBuilder(context);

        try {

            return (QueryRoot) qc.jjtAccept(exprBuilder, null);

        } catch (VisitorException e) {

            throw new MalformedQueryException(e.getMessage(), e);

        }

    }

    /**
     * TODO This should be handled by an AST visitor. It is just much simpler.
     * That will also allow us to handle query hints embedded into comments. 
     */
    static private Properties getQueryHints(final ASTQueryContainer qc)
            throws MalformedQueryException {
        
        final Properties queryHints = new Properties();
        
        final Map<String, String> prefixes = PrefixDeclProcessor.process(qc);
        
        // iterate the namespaces
        for (Map.Entry<String, String> prefix : prefixes.entrySet()) {
            // if we see one that matches the magic namespace, try
            // to parse it
            if (prefix.getKey().equalsIgnoreCase(QueryHints.PREFIX)) {
                String hints = prefix.getValue();
                // has to have a # and it can't be at the end
                int i = hints.indexOf('#');
                if (i < 0 || i == hints.length() - 1) {
                    throw new MalformedQueryException("bad query hints: "
                            + hints);
                }
                hints = hints.substring(i + 1);
                // properties are separated by &
                final StringTokenizer st = new StringTokenizer(hints, "&");
                while (st.hasMoreTokens()) {
                    final String hint = st.nextToken();
                    i = hint.indexOf('=');
                    if (i < 0 || i == hint.length() - 1) {
                        throw new MalformedQueryException("bad query hint: "
                                + hint);
                    }
                    final String key = hint.substring(0, i);
                    final String val = hint.substring(i + 1);
                    queryHints.put(key, val);
                }
            }
        }
     
        return queryHints;
        
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
