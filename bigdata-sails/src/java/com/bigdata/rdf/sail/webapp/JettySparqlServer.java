package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.BigdataContext.Config;

/**
 * 
 * The {@link JettySparqlServer} enables easy embedding of the {@link RESTServlet}
 * 
 * To invoke from the command line use the {@link SparqlCommand}.
 * 
 * The server provides an embeddable alternative to a standard web application
 * deployment.
 * 
 * @author martyn Cutcher
 */
public class JettySparqlServer extends Server {

	static private final Logger log = Logger.getLogger(JettySparqlServer.class);

	protected static final boolean directServletAccess = true;

	int m_port = -1; // allow package visibility from JettySparqlCommand

	final TreeMap<String, Handler> m_handlerMap = new TreeMap<String, Handler>();

	/**
	 * 
	 * @param port
	 *            The port at which the service will run.
	 */
	public JettySparqlServer(final int port) {

		super(port); // creates Connector for specified port
		
	}

	public void handle(final String target, final Request baseRequest,
			final HttpServletRequest req, final HttpServletResponse resp) {

		try {
		
			if (log.isDebugEnabled())
				log.debug("Lookingup " + target);
			
			final Handler handler = m_handlerMap.get(lookup(target));
			
			if (handler != null) {
			
				handler.handle(target, baseRequest, req, resp);
				
				baseRequest.setHandled(true);
				
			} else {

				if (log.isDebugEnabled())
					log.debug("Calling default handler");
				
				super.handle(target, baseRequest, req, resp);
				
			}

		} catch (IOException e) {
			// FIXME Auto-generated catch block
			e.printStackTrace();
		} catch (ServletException e) {
			// FIXME Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private Object lookup(final String target) {
	
		final int spi = target.indexOf("/", 1);

		final String ret = spi == -1 ? target : target.substring(0, spi);
		
		return ret;
		
	}

	public void setupHandlers(final Config config,
			final IIndexManager indexManager) throws SailException,
			RepositoryException, IOException {

		final ResourceHandler resource_handler = new ResourceHandler();
		
		resource_handler.setDirectoriesListed(false); // Nope!

		resource_handler.setWelcomeFiles(new String[] { "index.html" });

		resource_handler.setResourceBase(config.resourceBase);
		
		BigdataContext.establishContext(config, indexManager);

		// embedded setup
		m_handlerMap.put("/status", new ServletHandler(new StatusServlet()));
		
		if (directServletAccess) {
			m_handlerMap.put("/query", new ServletHandler(new QueryServlet()));
			m_handlerMap.put("/update", new ServletHandler(new UpdateServlet()));
			m_handlerMap.put("/delete", new ServletHandler(new DeleteServlet()));
		} else {
			// create implementation servlets
			new QueryServlet();
			new UpdateServlet();
			
			// still need delete endpoint for delete with body
			m_handlerMap.put("/delete", new ServletHandler(new DeleteServlet()));
		}
		m_handlerMap.put("/", new ServletHandler(new RESTServlet()));
		
		// the "stop" handler is only relevant for the embedded server
		m_handlerMap.put("/stop", new AbstractHandler() {
			public void handle(String arg0, Request arg1, HttpServletRequest arg2, HttpServletResponse resp)
					throws IOException, ServletException {
				try {
					resp.getWriter().println("Server Stop request received");
					shutdownNow();
				} catch (InterruptedException e) {
					// okay
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

		final HandlerList handlers = new HandlerList();
		
		handlers.setHandlers(new Handler[] { resource_handler, new DefaultHandler() });

		setHandler(handlers);
		
	}

	public void startup(final Config config, final IIndexManager indexManager) throws Exception {

		setupHandlers(config, indexManager);

        if (log.isInfoEnabled())
            log.info("Calling start");
		
		start();

		m_port = getConnectors()[0].getLocalPort();

		m_running = true;
		
        if (log.isInfoEnabled())
            log.info("Running on port: " + m_port);
	
	}

	static Handler makeContextHandler(Handler handler, String spec) {
		ContextHandler context = new ContextHandler();
		context.setContextPath(spec);
		context.setResourceBase(".");

		context.setClassLoader(Thread.currentThread().getContextClassLoader());

		context.setHandler(handler);

		return context;

	}

	static class ServletHandler extends AbstractHandler {
		final HttpServlet m_servlet;

		ServletHandler(HttpServlet servlet) {
			m_servlet = servlet;
		}

		public void handle(String arg0, Request jettyRequest, HttpServletRequest req, HttpServletResponse resp)
				throws IOException, ServletException {

			m_servlet.service(req, resp);

			jettyRequest.setHandled(true);
		}

	}

	boolean m_running = false;

	public boolean isOpen() {
		return m_running;
	}

	/**
	 * Shutdown the context releasing any resources and stop the server.
	 * @throws Exception
	 */
	public void shutdownNow() throws Exception {

	    BigdataContext.clear();

		stop();

		m_port = -1;
		m_running = false;
	}

	public int getPort() {
		return m_port;
	}

}
