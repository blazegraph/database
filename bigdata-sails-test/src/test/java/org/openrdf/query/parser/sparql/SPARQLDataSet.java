/* 
 * Licensed to Aduna under one or more contributor license agreements.  
 * See the NOTICE.txt file distributed with this work for additional 
 * information regarding copyright ownership. 
 *
 * Aduna licenses this file to you under the terms of the Aduna BSD 
 * License (the "License"); you may not use this file except in compliance 
 * with the License. See the LICENSE.txt file distributed with this work 
 * for the full License.
 *
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
 * implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package org.openrdf.query.parser.sparql;

import java.util.HashMap;
import java.util.Set;

public class SPARQLDataSet {

	private HashMap<String, String> namedGraphs = new HashMap<String, String>();

	private String defaultGraph;

	public SPARQLDataSet() {
	}

	public SPARQLDataSet(String defaultGraph) {
		this();
		setDefaultGraph(defaultGraph);
	}

	public void setDefaultGraph(String defaultGraph) {
		this.defaultGraph = defaultGraph;
	}

	public String getDefaultGraph() {
		return defaultGraph;
	}

	public void addNamedGraph(String graphName, String graphLocation) {
		namedGraphs.put(graphName, graphLocation);
	}

	public boolean hasNamedGraphs() {
		return (!namedGraphs.isEmpty());
	}

	public Set<String> getGraphNames() {
		return namedGraphs.keySet();
	}

	public String getGraphLocation(String graphName) {
		return namedGraphs.get(graphName);
	}
}
