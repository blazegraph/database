/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.sparql;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.Value;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.sparql.ast.ASTDatasetClause;
import com.bigdata.rdf.sail.sparql.ast.ASTIRI;
import com.bigdata.rdf.sail.sparql.ast.ASTOperation;
import com.bigdata.rdf.sail.sparql.ast.ASTOperationContainer;
import com.bigdata.rdf.sail.sparql.ast.ASTUpdateContainer;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.QuadsOperationInTriplesModeException;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.BD;
import com.bigdata.relation.accesspath.IAccessPath;

/**
 * Extracts a SPARQL {@link Dataset} from an ASTQueryContainer, if one is
 * contained.
 * 
 * @author Simon Schenk
 * @author Arjohn Kampman
 * @author Bryan Thompson
 * @openrdf
 */
public class DatasetDeclProcessor {

    private final AST2BOpContext context;

    /*
     * Lazily instantiated sets for the default and named graphs.
     */

    private Set<IV<?,?>> defaultGraphs = null;
    
    private Set<IV<?,?>> namedGraphs = null;

    private IV virtualGraph;

    /**
     * Add a graph to the set of default or named graphs.
     * 
     * @param graph
     *            The {@link IV} of the graph.
     * @param named
     *            When <code>true</code>, it will be added to the set of named
     *            graphs. Otherwise, it will be added to the set of graphs in
     *            the default graph.
     */
    private void addGraph(final IV<?,?> graph, final boolean named) {

        if (graph == null)
            throw new IllegalArgumentException();
        
        if (named) {

            if (namedGraphs == null)
                namedGraphs = new LinkedHashSet<IV<?,?>>();

            namedGraphs.add(graph);

        } else {

            if (defaultGraphs == null)
                defaultGraphs = new LinkedHashSet<IV<?,?>>();

            defaultGraphs.add(graph);

        }

    }

    public DatasetDeclProcessor(final AST2BOpContext context, final IV virtualGraph) {
        
        this.context = context;
        this.virtualGraph = virtualGraph;
        
    }
    
        /**
     * Extracts a SPARQL {@link Dataset} from an ASTQueryContainer, if one is
     * contained. Returns null otherwise.
     * 
     * @param qc
     *            The query model to resolve relative URIs in.
     * 
     * @throws MalformedQueryException
     *             If DatasetClause does not contain a valid URI.
     */
    public DatasetNode process(final List<ASTDatasetClause> datasetClauses, boolean update)
            throws MalformedQueryException {

        if (datasetClauses!=null && !datasetClauses.isEmpty()) {

//          dataset = new DatasetImpl();
            
            if (!context.getAbstractTripleStore().isQuads()) {
                throw new QuadsOperationInTriplesModeException(
                    "NAMED clauses in queries are not supported in"
                    + " triples mode.");
            }
           
            for (ASTDatasetClause dc : datasetClauses) {
            
                final ASTIRI astIri = dc.jjtGetChild(ASTIRI.class);

                try {
                    
                    /*
                     * Note: This is the URI with the IV already resolved.
                     */
//                  URI uri = new URIImpl(astIri.getValue());
                    BigdataURI uri = context.getAbstractTripleStore().getValueFactory().asValue((BigdataURI) astIri.getRDFValue());
                    if (uri!=null && uri.getIV()==null) {
                        IV resolvedIV = context.getAbstractTripleStore().getIV(uri);
                        if (resolvedIV!=null) {
                            uri.setIV(resolvedIV);
                            resolvedIV.setValue(uri);
                        } else {
                            uri = (BigdataURI) astIri.getRDFValue();
                        }
                        
                    }


                    if (dc.isVirtual()) {

                        if (uri.getIV().isNullIV()) {
                            /*
                             * A virtual graph was referenced which is not
                             * declared in the database. This virtual graph will
                             * not have any members.
                             */
                            throw new RuntimeException("Not declared: " + uri);
                        }

                        if (virtualGraph == null) {

                            throw new RuntimeException("Not declared: "
                                    + BD.VIRTUAL_GRAPH);
                            
                        }

                        final IAccessPath<ISPO> ap = context.getAbstractTripleStore()
                                .getSPORelation()
                                .getAccessPath(//
                                        uri.getIV(),// the virtual graph "name"
                                        virtualGraph, null/* o */, null/* c */);
                        
                        final Iterator<ISPO> itr = ap.iterator();
                        
                        while(itr.hasNext()) {

                            final IV memberGraph = itr.next().o();
                            BigdataValue value = context.getAbstractTripleStore().getLexiconRelation().getTerm(memberGraph);
                            memberGraph.setValue(value);
                            addGraph(memberGraph, dc.isNamed());

                        }
                        
                    } else {

                        if (uri.getIV() != null)
                            addGraph(uri.getIV(), dc.isNamed());

                    }

                }
                catch (IllegalArgumentException e) {
                    throw new MalformedQueryException(e.getMessage(), e);
                }
            }
        }

        if (defaultGraphs == null && namedGraphs == null) {

            return null;
            
        }

        // Note: Cast required to shut up the compiler.
        @SuppressWarnings("unchecked")
        final DatasetNode dsn = new DatasetNode((Set) defaultGraphs, (Set) namedGraphs, update);

        return dsn;
        
    }

}
