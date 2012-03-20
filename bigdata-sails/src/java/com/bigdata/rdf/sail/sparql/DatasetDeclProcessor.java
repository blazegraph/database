/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.sparql;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sail.sparql.ast.ASTDatasetClause;
import com.bigdata.rdf.sail.sparql.ast.ASTIRI;
import com.bigdata.rdf.sail.sparql.ast.ASTOperationContainer;
import com.bigdata.rdf.sail.sparql.ast.ASTUpdateContainer;
import com.bigdata.rdf.sparql.ast.DatasetNode;
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
 */
public class DatasetDeclProcessor {

    private final BigdataASTContext context;

    /*
     * Lazily instantiated sets for the default and named graphs.
     */

    private Set<IV<?,?>> defaultGraphs = null;
    
    private Set<IV<?,?>> namedGraphs = null;

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

    public DatasetDeclProcessor(final BigdataASTContext context) {
        
        this.context = context;
        
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
    public DatasetNode process(final ASTOperationContainer qc)
            throws MalformedQueryException {

        final boolean update = qc instanceof ASTUpdateContainer;
        
        final List<ASTDatasetClause> datasetClauses = qc.getOperation()
                .getDatasetClauseList();

        // Lazily resolved.
        BigdataURI virtualGraph = null;

        if (!datasetClauses.isEmpty()) {

//		    dataset = new DatasetImpl();

			for (ASTDatasetClause dc : datasetClauses) {
			
			    final ASTIRI astIri = dc.jjtGetChild(ASTIRI.class);

				try {
					
				    /*
				     * Note: This is the URI with the IV already resolved.
				     */
//				    URI uri = new URIImpl(astIri.getValue());
                    final BigdataURI uri = (BigdataURI) astIri.getRDFValue();

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
                        
                            /*
                             * Resolve the IV for the virtual graph membership
                             * relation.
                             */
                            
                            virtualGraph = (BigdataURI) context.vocab
                                    .get(BD.VIRTUAL_GRAPH);

                            if (virtualGraph == null
                                    || virtualGraph.getIV() == null) {

                                throw new RuntimeException("Not declared: "
                                        + BD.VIRTUAL_GRAPH);
                                
                            }
                            
                        }

                        final IAccessPath<ISPO> ap = context.tripleStore
                                .getSPORelation()
                                .getAccessPath(//
                                        uri.getIV(),// the virtual graph "name"
                                        virtualGraph.getIV(), null/* o */, null/* c */);
                        
                        final Iterator<ISPO> itr = ap.iterator();
                        
                        while(itr.hasNext()) {

                            final IV<?,?> memberGraph = itr.next().o();
                            
                            addGraph(memberGraph, dc.isNamed());

                        }
                        
                    } else {

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
        return new DatasetNode((Set) defaultGraphs, (Set) namedGraphs, update);

	}

}
