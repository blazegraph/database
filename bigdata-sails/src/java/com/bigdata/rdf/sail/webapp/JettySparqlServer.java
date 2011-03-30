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
 * The JettySparqlServer enables easy embedding of the SparqlServlet.
 * 
 * To invoke from the commandline use the JettySparqlCommand.
 * 
 * The server provides an embeddable alternative to a standard web application
 * deployment.
 * 
 * @author martyn Cutcher
 *
 */
public class JettySparqlServer extends Server {

	static private final Logger log = Logger.getLogger(JettySparqlServer.class);

	int m_port = -1; // allow package visibility from JettySparqlCommand

	final TreeMap<String, Handler> m_handlerMap = new TreeMap<String, Handler>();

	public JettySparqlServer(int port) {
		super(port); // creates Connector for specified port
	}

	public void handle(String target, Request baseRequest, HttpServletRequest req, HttpServletResponse resp) {
		try {
			if (log.isDebugEnabled())
				log.debug("Lookingup " + target);
			
			Handler handler = m_handlerMap.get(lookup(target));
			if (handler != null) {
				handler.handle(target, baseRequest, req, resp);
				baseRequest.setHandled(true);
			} else {

				if (log.isDebugEnabled())
					log.debug("Calling default handler");
				
				super.handle(target, baseRequest, req, resp);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ServletException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private Object lookup(String target) {
		int spi = target.indexOf("/", 1);

		String ret = spi == -1 ? target : target.substring(0, spi);
		
		return ret;
	}

	public void setupHandlers(final Config config, final IIndexManager indexManager) throws SailException,
			RepositoryException, IOException {
		ResourceHandler resource_handler = new ResourceHandler();
		resource_handler.setDirectoriesListed(true);
		resource_handler.setWelcomeFiles(new String[] { "index.html" });

		resource_handler.setResourceBase(config.resourceBase);
		
		BigdataContext.establishContext(config, indexManager);

		// embedded setup
		m_handlerMap.put("/status", new ServletHandler(new StatusServlet()));
		m_handlerMap.put("/query", new ServletHandler(new QueryServlet()));
		m_handlerMap.put("/update", new ServletHandler(new UpdateServlet()));
		m_handlerMap.put("/delete", new ServletHandler(new DeleteServlet()));
		m_handlerMap.put("/REST", new ServletHandler(new RESTServlet()));
		
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

		HandlerList handlers = new HandlerList();
		handlers.setHandlers(new Handler[] { resource_handler, new DefaultHandler() });

		setHandler(handlers);
	}

	public void startup(final Config config, final IIndexManager indexManager) throws Exception {
		setupHandlers(config, indexManager);

		log.info("Calling start");
		
		start();

		m_port = getConnectors()[0].getLocalPort();

		m_running = true;
		
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
