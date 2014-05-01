package com.bigdata.blueprints;

import java.util.List;


public interface QueryManager {

	/* 
	 * Set the SPARQL endpoint to exchange with.
	 */
	public void setEndpoint( String endpointURL );
	
	/*
	 * Set the Vertices and Edges to form deletion queries from
	 */
	public void setDeleteElements( List<BigdataElement> elements );

	/*
	 * Set the Vertices and Edges to form insertion queries from
	 */
	public void setInsertElements( List<BigdataElement> elements );
	
	/*
	 * Resets private query variables to null.
	 */
	public void reset();
	
	/*
	 * Returns the aggregate INSERT{ } clause for asserted triples.
	 */
	public String getInsertClause();
	
	/*
	 * Returns the aggregate DELETE{ } clause for asserted triples.
	 */
	public String getDeleteClause();
	
	/*
	 * Returns an array of DELETE{ <URI> ?p ?o } WHERE{ <URI> ?p ?o } queries to delete every
	 * triple of an Element.
	 */
	public String[] getDeleteQueries();
	
	/*
	 * Build the internal representation of the queries.
	 */
	public void buildQUeries();
	
	/*
	 * Submits the update query to the server, no result set returned.
	 */
	public void commitUpdate();
	
}
