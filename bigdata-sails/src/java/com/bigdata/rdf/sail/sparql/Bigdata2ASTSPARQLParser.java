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

import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.ast.ASTQuery;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import com.bigdata.rdf.sail.IBigdataParsedQuery;
import com.bigdata.rdf.sail.QueryHints;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Overridden version of the openrdf 2.3 SPARQLParser class which extracts
 * additional information required by bigdata and associates it with the
 * {@link ParsedQuery}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: BigdataSPARQLParser.java 4793 2011-06-24 17:29:25Z thompsonbry
 *          $
 */
public class Bigdata2ASTSPARQLParser implements QueryParser {

    private final AbstractTripleStore tripleStore;

    public Bigdata2ASTSPARQLParser(final AbstractTripleStore tripleStore) {
        
        this.tripleStore = tripleStore;
        
    }

    /**
     * {@inheritDoc}
     * 
     * @return An object which implements {@link IBigdataParsedQuery}.
     *         Additional information is available by casting the returned
     *         object to that interface.
     */
    public ParsedQuery parseQuery(final String queryStr, final String baseURI)
            throws MalformedQueryException {
        try {
            final ASTQueryContainer qc = SyntaxTreeBuilder.parseQuery(queryStr);
            StringEscapesProcessor.process(qc);
            BaseDeclProcessor.process(qc, baseURI);
            final Map<String, String> prefixes = PrefixDeclProcessor.process(qc);
            /*
             * FIXME Add a batch resolution step in which we collect all RDF
             * Values, batch resolve them against the lexicon, and then
             * annotation (or rewrite) the AST with the BigdataValue's and their
             * resolved IVs. Update the ValueExprBuilder to leverage that
             * required batch resolve step.
             */
//            WildcardProjectionProcessor.process(qc);
            BlankNodeVarProcessor.process(qc);
            final IQueryNode queryRoot = buildQueryModel(qc, tripleStore);

            ParsedQuery query;

            // Note: Bigdata override.
            final Properties queryHints = getQueryHints(qc);
            
            // Note: Constructors in if then else are overridden too.
            ASTQuery queryNode = qc.getQuery();
//            if (queryNode instanceof ASTSelectQuery) {
//                query = new BigdataParsedTupleQuery(tupleExpr,
//                        QueryType.SELECT, queryHints);
//            }
//            else if (queryNode instanceof ASTConstructQuery) {
//                query = new BigdataParsedGraphQuery(tupleExpr, prefixes,
//                        QueryType.CONSTRUCT, queryHints);
//            }
//            else if (queryNode instanceof ASTAskQuery) {
//                query = new BigdataParsedBooleanQuery(tupleExpr, QueryType.ASK,
//                        queryHints);
//            }
//            else if (queryNode instanceof ASTDescribeQuery) {
//                query = new BigdataParsedGraphQuery(tupleExpr, prefixes,
//                        QueryType.DESCRIBE, queryHints);
//            }
//            else {
//                throw new RuntimeException("Unexpected query type: " + queryNode.getClass());
//            }
//
//            // Handle dataset declaration
//            Dataset dataset = DatasetDeclProcessor.process(qc);
//            if (dataset != null) {
//                query.setDataset(dataset);
//            }
//
//            return query;
            throw new RuntimeException("Finished parse.");
        }
        catch (ParseException e) {
            throw new MalformedQueryException(e.getMessage(), e);
        }
        catch (TokenMgrError e) {
            throw new MalformedQueryException(e.getMessage(), e);
        }
    }

    private IQueryNode buildQueryModel(final ASTQueryContainer qc,
            final AbstractTripleStore tripleStore)
            throws MalformedQueryException {

        final BigdataExprBuilder exprBuilder = new BigdataExprBuilder(
                new BigdataASTContext(tripleStore));

        try {

            return (IQueryNode) qc.jjtAccept(exprBuilder, null);
            
        } catch (VisitorException e) {

            throw new MalformedQueryException(e.getMessage(), e);

        }

    }

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
     
        return queryHints; // Note: MAY be null.
        
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
