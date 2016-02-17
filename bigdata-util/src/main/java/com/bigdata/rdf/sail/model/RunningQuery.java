/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.rdf.sail.model;

import java.util.UUID;

/**
 * Metadata about running {@link com.bigdata.rdf.sail.webapp.BigdataRDFContext.AbstractQueryTask}s (this includes both
 * queries and update requests).
 * 
 * Used to serialize the results for the REST interface in JSON, XML, etc.
 */
public class RunningQuery {


	/**
	 * The unique identifier for this query as assigned by the Embedded
	 * Graph implementation end point (rather than the {@link com.bigdata.bop.engine.QueryEngine}).
	 */
	private String extQueryId;

	/**
	 * The unique identifier for this query for the {@link com.bigdata.bop.engine.QueryEngine}
	 * (non-<code>null</code>).
	 * 
	 * @see com.bigdata.bop.engine.QueryEngine#getRunningQuery(UUID)
	 */
	private UUID queryUuid;

	/** The timestamp when the query was accepted (ns). */
	private long begin;
	
	/**
	 * Is the query an update query.
	 */
	private boolean isUpdateQuery;
	
	/**
	 * Was the query cancelled.
	 */
	protected boolean isCancelled = false;
	
	public RunningQuery(final String extQueryId, final UUID queryUuid,
			final long begin, final boolean isUpdateQuery) {

		if (queryUuid == null)
			throw new IllegalArgumentException();

		this.extQueryId = extQueryId;

		this.queryUuid = queryUuid;

		this.begin = begin;
		
		this.isUpdateQuery = isUpdateQuery;
		
		this.setCancelled(false);

	}
	
	/**
	 * Constructor used for serialization
	 */
	public RunningQuery() {
		
		this(null, UUID.randomUUID(), -1, false);
	}
	
	public void setElapsedTimeNS() {
		//Do nothing.  This is for JSON Serialization
	}
	
	public long getElapsedTimeNS() {
		return (System.nanoTime() - this.begin);
	}
	
	public String getExtQueryId() {
		return extQueryId;
	}
	
	public UUID getQueryUuid() {
		return queryUuid;
	}

	public long getBegin() {
		return begin;
	}
	
	public boolean getIsUpdateQuery() {
		return isUpdateQuery;
	}

	public boolean isCancelled() {
		return isCancelled;
	}

	public void setCancelled(boolean isCancelled) {
		this.isCancelled = isCancelled;
		
	}

	public void setExtQueryId(String extQueryId) {
		this.extQueryId = extQueryId;
	}

	public void setQueryUuid(UUID queryUuid) {
		this.queryUuid = queryUuid;
	}

	public void setBegin(long begin) {
		this.begin = begin;
	}

	public void setUpdateQuery(boolean isUpdateQuery) {
		this.isUpdateQuery = isUpdateQuery;
	}
}
