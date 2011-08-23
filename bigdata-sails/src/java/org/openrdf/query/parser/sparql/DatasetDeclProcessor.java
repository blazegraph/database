/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql;

import java.util.List;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.query.parser.sparql.ast.ASTDatasetClause;
import org.openrdf.query.parser.sparql.ast.ASTIRI;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;

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
	public static Dataset process(ASTQueryContainer qc)
		throws MalformedQueryException
	{
		DatasetImpl dataset = null;

		List<ASTDatasetClause> datasetClauses = qc.getQuery().getDatasetClauseList();

		if (!datasetClauses.isEmpty()) {
			dataset = new DatasetImpl();

			for (ASTDatasetClause dc : datasetClauses) {
				ASTIRI astIri = dc.jjtGetChild(ASTIRI.class);

				try {
					URI uri = new URIImpl(astIri.getValue());
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
