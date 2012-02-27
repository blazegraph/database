/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.sparql;

import java.util.List;

import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.impl.DatasetImpl;

import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sail.sparql.ast.ASTDatasetClause;
import com.bigdata.rdf.sail.sparql.ast.ASTIRI;
import com.bigdata.rdf.sail.sparql.ast.ASTOperationContainer;

/**
 * Extracts a SPARQL {@link Dataset} from an ASTQueryContainer, if one is
 * contained.
 * 
 * @author Simon Schenk
 * @author Arjohn Kampman
 */
public class DatasetDeclProcessor {

	/**
	 * Extracts a SPARQL {@link Dataset} from an ASTQueryContainer, if one is
	 * contained. Returns null otherwise.
	 * 
	 * @param qc
	 *        The query model to resolve relative URIs in.
	 * @throws MalformedQueryException
	 *         If DatasetClause does not contain a valid URI.
	 */
	public static Dataset process(final ASTOperationContainer qc)
		throws MalformedQueryException
	{
		DatasetImpl dataset = null;

		final List<ASTDatasetClause> datasetClauses = qc.getOperation().getDatasetClauseList();

		if (!datasetClauses.isEmpty()) {

		    dataset = new DatasetImpl();

			for (ASTDatasetClause dc : datasetClauses) {
			
			    final ASTIRI astIri = dc.jjtGetChild(ASTIRI.class);

				try {
					
				    /*
				     * Note: This is the URI with the IV already resolved.
				     */
//				    URI uri = new URIImpl(astIri.getValue());
				    final BigdataURI uri = (BigdataURI)astIri.getRDFValue();
					
					if (dc.isNamed()) {
						dataset.addNamedGraph(uri);
					}
					else {
						dataset.addDefaultGraph(uri);
					}
				}
				catch (IllegalArgumentException e) {
					throw new MalformedQueryException(e.getMessage(), e);
				}
			}
		}

		return dataset;
	}
}
