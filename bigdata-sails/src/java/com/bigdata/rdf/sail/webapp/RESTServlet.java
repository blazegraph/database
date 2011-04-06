package com.bigdata.rdf.sail.webapp;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * The RESTServlet maps a REST interface described by Eric Wilde and Michael
 * Hausenblas onto the implementation servlets with more targetted URLs.
 *
 * This provides an equivalent interface to the original NanoSparqlServer.
 * 
 * @author Martyn Cutcher
 *
 */
public class RESTServlet extends BigdataServlet {

	/**
	 * GET is only allowed with query requests, so delegate to the QueryServlet.
	 */
	public void doGet(final HttpServletRequest req, final HttpServletResponse resp) {
		getContext().getQueryServlet().doGet(req, resp);
	}

	/**
	 * A query can be submitted with a POST if a query parameter is provided.
	 * 
	 * Otherwise delegate to the UpdateServlet
	 */
	public void doPost(final HttpServletRequest req, final HttpServletResponse resp) {
		if (req.getParameter("query") != null) {
			getContext().getQueryServlet().doGet(req, resp);
		} else {
			getContext().getUpdateServlet().doPut(req, resp);
		}
	}
	
	/**
	 * A PUT request always delegates to the UpdateServlet
	 */
	public void doPut(final HttpServletRequest req, final HttpServletResponse resp) {
		getContext().getUpdateServlet().doPut(req, resp);
	}
	
	/**
	 * A DELETE request will delete statements indicated by a provided namespace
	 * URI and an optional query parameter.
	 * 
	 * Delegate to the DeleteServlet.
	 */
    public void doDelete(final HttpServletRequest req, final HttpServletResponse resp) {
    	getContext().getDeleteServlet().doDelete(req, resp);
    }
}
