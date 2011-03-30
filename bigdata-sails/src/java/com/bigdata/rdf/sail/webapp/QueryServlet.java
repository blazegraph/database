package com.bigdata.rdf.sail.webapp;

import info.aduna.xml.XMLWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.rdfxml.RDFXMLParser;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import org.openrdf.sail.SailException;

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailGraphQuery;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSailTupleQuery;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.webapp.BigdataContext.AbstractQueryTask;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.util.httpd.NanoHTTPD;

public class QueryServlet extends BigdataServlet {

	/**
	 * The logger for the concrete {@link BigdataServlet} class.
	 */
	static private final Logger log = Logger.getLogger(BigdataServlet.class); 

	/**
	 * @todo use to decide ASK, DESCRIBE, CONSTRUCT, SELECT, EXPLAIN, etc.
	 */
	private final QueryParser m_engine;


    public QueryServlet() {
		// used to parse qeries.
        m_engine = new SPARQLParserFactory().getParser();
        
        getContext().registerServlet(this);
    }

    public void doPost(final HttpServletRequest req, final HttpServletResponse resp) {
    	doGet(req, resp); // POST is allowed for query
	}

	public void doGet(final HttpServletRequest req, final HttpServletResponse resp) {
		final String namespace = getNamespace(req.getRequestURI());

		final long timestamp = getTimestamp(req.getRequestURI(), req);

		final String uriqueryStr = req.getQueryString();
		final String url = req.getRequestURL().toString();
		
		final String queryStr = req.getParameter("query");

		if (queryStr == null) {
			resp.setContentType("text/test;charset=utf-8");
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			try {
				resp.getWriter().println("Specify query using ?query=....");
			} catch (IOException e) {
				e.printStackTrace();
			}

			return;
		}

		/*
		 * Setup pipes. The [os] will be passed into the task that executes
		 * the query. The [is] will be passed into the Response. The task is
		 * executed on a thread pool.
		 * 
		 * Note: If the client closes the connection, then the InputStream
		 * passed into the Response will be closed and the task will
		 * terminate rather than running on in the background with a
		 * disconnected client.
		 */
		OutputStream os;
		try {
			os = resp.getOutputStream();
		} catch (IOException e2) {
			e2.printStackTrace();
			
			throw new RuntimeException(e2);
		}
		try {

			final AbstractQueryTask queryTask = BigdataContext.getContext().getQueryTask(namespace, timestamp, queryStr, req, os);

			if (log.isTraceEnabled())
				log.trace("Running query: " + queryStr);

			queryTask.call();

			// Setup the response.
			// TODO Move charset choice into conneg logic.
			buildResponse(resp, HTTP_OK, queryTask.mimeType + "; charset='" + charset + "'");

		} catch (Throwable e) {
			try {
				throw launderThrowable(e, os, queryStr);
			} catch (Exception e1) {
				throw new RuntimeException(e);
			}
		}

		
	}

}
