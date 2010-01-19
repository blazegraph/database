package benchmark.testdriver;

import benchmark.qualification.QueryResult;

public interface ServerConnection {
	/*
	 * Execute Query with Query Object
	 */
	public void executeQuery(Query query, byte queryType);
	
	public void executeQuery(CompiledQuery query, CompiledQueryMix queryMix);

	public QueryResult executeValidation(Query query, byte queryType);
	
	public void close();
}

