package edu.lehigh.swat.bench.ubt.bigdata;

import edu.lehigh.swat.bench.ubt.api.Repository;
import edu.lehigh.swat.bench.ubt.api.RepositoryFactory;

/**
 * Factory for SPARQL endpoint tests.
 */
public class SparqlRepositoryFactory extends RepositoryFactory {

	public Repository create() {
		
		return new SparqlRepository();
		
	}

}
