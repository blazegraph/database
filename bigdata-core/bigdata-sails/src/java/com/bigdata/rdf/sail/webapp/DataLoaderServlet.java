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
package com.bigdata.rdf.sail.webapp;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.properties.PropertiesFormat;
import com.bigdata.rdf.properties.PropertiesParser;
import com.bigdata.rdf.properties.PropertiesParserFactory;
import com.bigdata.rdf.properties.PropertiesParserRegistry;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;
import com.bigdata.rdf.store.DataLoader.MyLoadStats;
import com.bigdata.rdf.store.LocalTripleStore;

/**
 * 
 * Provides {@link com.bigdata.rdf.store.DataLoader} via the REST API.
 * 
 * See BLZG-1713
 * 
 * @author beebs@systap.com
 */
public class DataLoaderServlet extends BigdataRDFServlet {

	/**
     * 
     */
	private static final long serialVersionUID = 1L;

	static private final transient Logger log = Logger
			.getLogger(DataLoaderServlet.class);

	/**
	 * Delegate for the sparql end point expressed by
	 * <code>.../namespace/NAMESPACE/sparql</code>.
	 */
	private RESTServlet m_restServlet;

	/**
	 * Overridden to create and initialize the delegate {@link Servlet}
	 * instances.
	 */
	@Override
	public void init() throws ServletException {

		super.init();

		m_restServlet = new RESTServlet();

		m_restServlet.init(getServletConfig());

	}

	/**
	 * Handle namespace create.
	 */
	@Override
	protected void doPost(final HttpServletRequest req,
			final HttpServletResponse resp) throws IOException {

		if (req.getRequestURI().endsWith("/dataloader")) {
			// See BLZG-1713

			// Invoke the Data Loader
			
			if(log.isDebugEnabled()) {
				log.debug("");
			}

			doBulkLoad(req, resp);

			return;
		}

		/*
		 * Pass through to the SPARQL end point REST API.
		 * 
		 * Note: This also handles CANCEL QUERY, which is a POST.
		 */
		m_restServlet.doPost(req, resp);

	}

	/**
	 * 
	 * Provides {@link com.bigdata.rdf.store.DataLoader} via the REST API.
	 * 
	 * @author beebs@systap.com
	 * 
	 * @param req
	 * @param resp
	 * @throws IOException
	 */

	/*
	 * The properties for invoking the DataLoader via the SERVLET are below.
	 * This file should be POSTED to the SERVLET.
	 * 
	  <?xml version="1.0" encoding="UTF-8" standalone="no"?> 
	  <!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd"> 
	  <properties>
	  <!-- --> 
	  <!-- RDF Format (Default is rdf/xml) --> 
	  <!-- --> 
	  <entry key="format">rdf/xml</entry> 
	  <!-- --> 
	  <!-- Base URI (Optional) --> 
	  <!-- -->
	  <entry key="baseURI">http://baseuri/</entry> 
	  <!-- --> 
	  <!-- Default Graph URI (Optional -- Required for quads mode namespace) --> 
	  <!-- --> 
	  <entry key="defaultGraph">http://defaultgraph/</entry> 
	  <!-- -->
	  <!-- Suppress all stdout messages (Optional) --> 
	  <!-- --> 
	  <entry key="quiet">true</entry> 
	  <!-- --> 
	  <!-- Show additional messages detailing the load performance. (Optional) --> 
	  <!-- --> 
	  <entry key="verbose">false</entry> 
	  <!-- --> 
	  <!-- Compute the RDF(S)+ closure. (Optional) --> 
	  <!-- --> 
	  <entry key="closure">false</entry> 
	  <!-- --> 
	  <!-- Files will be renamed to either <code>.good</code> or <code>.fail</code>
	  as they are processed. The files will remain in the same directory. -->
	  <!-- --> 
	  <entry key="durableQueues">true</entry> 
	  <!-- --> 
	  <!-- The
	  namespace of the KB instance. Defaults to kb. --> 
	  <!-- --> 
	  <entry key="namespace">kb</entry> 
	  <!-- --> 
	  <!-- The configuration file for the database instance. It must be readable by the web application. --> 
	  <!-- --> 
	  <entry key="propertyFile">kb</entry> 
	  <!-- --> 
	  <!-- Zero or more files or directories containing the data to be loaded. This should be a comma
	  delimited list. The files must be readable by the web application. -->
	  <!-- --> 
	  <entry key="fileOrDirs">file1,dir1,file2,dir2</entry>
	  </properties>
	 */

	private void doBulkLoad(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {

		if (!isWritable(getServletContext(), req, resp)) {
			// Service must be writable.
			return;
		}

		final BigdataRDFContext context = getBigdataRDFContext();

		final IIndexManager indexManager = context.getIndexManager();
		
		final PrintStream os = new PrintStream(resp.getOutputStream());

		/*
		 * Read the request entity, which must be some kind of Properties
		 * object. The namespace, propertyFile, and fileOrDirs properties are
		 * required.
		 */
		final Properties props;
		{

			final String contentType = req.getContentType();

			if (log.isInfoEnabled())
				log.info("Request body: " + contentType);

			final PropertiesFormat format = PropertiesFormat
					.forMIMEType(contentType);

			if (format == null) {

				buildAndCommitResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
						"Content-Type not recognized as Properties: "
								+ contentType);
				return;

			}

			if (log.isInfoEnabled())
				log.info("Format=" + format);

			final PropertiesParserFactory parserFactory = PropertiesParserRegistry
					.getInstance().get(format);

			if (parserFactory == null) {

				buildAndCommitResponse(resp, HTTP_INTERNALERROR,
						MIME_TEXT_PLAIN,
						"Parser factory not found: Content-Type=" + contentType
								+ ", format=" + format);

				return;

			}

			/*
			 * There is a request body, so let's try and parse it.
			 */

			final PropertiesParser parser = parserFactory.getParser();

			// The given Properties.
			props = parser.parse(req.getInputStream());

		}

		// RDF Format
		final RDFFormat rdfFormat = RDFFormat.valueOf(props.getProperty(
				"format", "rdf/xml"));

		// baseURI
		final String baseURI = props.getProperty("baseURI");

		// defaultGraph  -- Required if namespace is in quads mode
		final String defaultGraph = props.getProperty("defaultGraph");

		// Path to the configuration file for the database instance. Must be
		// readable by the web application
		final String propertyFile = props.getProperty("propertyFile");

		if (propertyFile == null) {
			// Required property
			throw new RuntimeException(
					"propertyFile is required for the DataLoader");
		}

		// Suppress all stdout messages (Optional)
		final boolean quiet = getBooleanProperty(props, "quiet", true);

		// Integer to show additional messages detailing the load performance.
		// Higher
		// is more verbose. (Optional)

		final int verbose = getIntProperty(props, "verbose", 0);

		// Compute the RDF(S)+ closure. (Optional)
		final boolean closure = getBooleanProperty(props, "closure", false);

		/**
		 * Files will be renamed to either <code>.good</code> or
		 * <code>.fail</code> as they are processed. The files will remain in
		 * the same directory.
		 */
		final boolean durableQueues = getBooleanProperty(props,
				"durableQueues", true);

		// The namespace of the KB instance. Defaults to "kb". -->
		final String namespace = props.getProperty("namespace",
				BigdataSail.Options.DEFAULT_NAMESPACE);

		/**
		 * Zero or more files or directories containing the data to be loaded.
		 * This should be a comma delimited list. The files must be readable by
		 * the web application.
		 */
		final String fileOrDirs = props.getProperty("fileOrDirs");

		if (fileOrDirs == null) {
			// Required property
			throw new RuntimeException(
					"fileOrDirs is required for the DataLoader");
		}

		if (log.isInfoEnabled()) {

			log.info("DataLoader called ( rdfFormat = " + rdfFormat
					+ " ; baseURI = " + baseURI + " ; defaultGraph = "
					+ defaultGraph + " ; quiet = " + quiet + " ; verbose = "
					+ verbose + " ; " + "durableQueues = " + durableQueues
					+ " ; namespace = " + namespace + "propertyFile = "
					+ propertyFile + " ; fileOrDirs = " + fileOrDirs + " )");

		}
		

		Properties properties = DataLoader.processProperties(propertyFile,
				quiet, verbose, durableQueues);

		AbstractTripleStore kb = (AbstractTripleStore) indexManager
				.getResourceLocator().locate(namespace, ITx.UNISOLATED);

		if (kb == null) {

			kb = new LocalTripleStore(indexManager, namespace,
					Long.valueOf(ITx.UNISOLATED), properties);

			kb.create();

			if (log.isInfoEnabled()) {
				log.info("Created namespace:  " + namespace);
			}

		}

		final DataLoader dataLoader = new DataLoader(properties, kb, os);

		final MyLoadStats totals = dataLoader.newLoadStats();

		final String[] fileToLoad = fileOrDirs.split(",");

		for (int i = 0; i < fileToLoad.length; i++) {

			final File nextFile = new File(fileToLoad[i]);

			if (!nextFile.exists()) {
				if (log.isInfoEnabled()) {
					log.info(nextFile.getName() + " does not exist.  Skipping.");
				}
				continue;
			}

			if (nextFile.isHidden()) {
				if (log.isInfoEnabled()) {
					log.info(nextFile.getName() + " is hidden.  Skipping.");
				}
				continue;
			}

			dataLoader.loadFiles(totals, 0/* depth */, nextFile, baseURI,
					rdfFormat, defaultGraph, DataLoader.getFilenameFilter(),
					true/* endOfBatch */
			);

		}

		dataLoader.endSource();

		if (!quiet)
			os.println("Load: " + totals);

		if (dataLoader.getClosureEnum() == ClosureEnum.None && closure) {

			if (verbose > 0)
				dataLoader.logCounters(dataLoader.getDatabase());

			if (!quiet)
				os.println("Computing closure.");

			if (log.isInfoEnabled())
			    log.info("Computing closure.");

			final ClosureStats stats = dataLoader.doClosure();

			if (!quiet)
				os.println("Closure: " + stats.toString());

			if (log.isInfoEnabled())
				log.info("Closure: " + stats.toString());

		}

		kb.commit(); // database commit

		totals.commit(); // Note: durable queues pattern.

		if (verbose > 1)
			dataLoader.logCounters(dataLoader.getDatabase());
		
		os.flush();
	}

	private boolean getBooleanProperty(final Properties props,
			final String property, final boolean defaultValue) {

		final String propVal = props.getProperty(property);

		if (propVal != null) {

			final boolean retVal = Boolean.parseBoolean(propVal);

			return retVal;
		}

		return defaultValue;

	}

	private int getIntProperty(final Properties props, final String property,
			final int defaultValue) {

		final String propVal = props.getProperty(property);

		if (propVal != null) {

			final int retVal = Integer.parseInt(propVal);

			return retVal;
		}

		return defaultValue;

	}

}
