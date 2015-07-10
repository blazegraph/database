package com.bigdata.rdf.sail.remote;

import org.openrdf.http.protocol.Protocol;
import org.openrdf.model.URI;
import org.openrdf.query.Binding;
import org.openrdf.query.impl.AbstractQuery;

import com.bigdata.rdf.sail.webapp.client.IPreparedQuery;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryDecls;

public abstract class AbstractBigdataRemoteQuery extends AbstractQuery {

	
	private String baseURI;

	public AbstractBigdataRemoteQuery(String baseURI) {
		super();
		this.baseURI = baseURI;
	}

	/**
	 * @see org.openrdf.http.client.HTTPClient#getQueryMethodParameters(QueryLanguage, String, String, Dataset, boolean, int, Binding...)
	 */
	protected void configureConnectOptions(IPreparedQuery q) {
		if (baseURI != null) {
			q.addRequestParam(Protocol.BASEURI_PARAM_NAME, baseURI);
		}
		q.addRequestParam(RemoteRepositoryDecls.INCLUDE_INFERRED,
				Boolean.toString(includeInferred));
		if (maxQueryTime > 0) {
			q.addRequestParam(Protocol.TIMEOUT_PARAM_NAME, Integer.toString(maxQueryTime));
		}

		if (dataset != null) {
			String[] defaultGraphs = new String[dataset.getDefaultGraphs().size()];
			int i=0;
			for (URI defaultGraphURI : dataset.getDefaultGraphs()) {
				defaultGraphs[i++] = String.valueOf(defaultGraphURI);
			}
			q.addRequestParam(Protocol.DEFAULT_GRAPH_PARAM_NAME, defaultGraphs);
			
			String[] namedGraphs = new String[dataset.getNamedGraphs().size()];
			i=0;
			for (URI namedGraphURI : dataset.getNamedGraphs()) {
				namedGraphs[i++] = String.valueOf(String.valueOf(namedGraphURI));
			}
			q.addRequestParam(Protocol.NAMED_GRAPH_PARAM_NAME, namedGraphs);
		}
		for (Binding binding: bindings) {
			String paramName = Protocol.BINDING_PREFIX + binding.getName();
			String paramValue = Protocol.encodeValue(binding.getValue());
			q.addRequestParam(paramName, paramValue);
		}
	}

}
