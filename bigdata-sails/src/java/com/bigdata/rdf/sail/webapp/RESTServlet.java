package com.bigdata.rdf.sail.webapp;

import java.io.IOException;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Default dispatch pattern for a core REST API.
 * 
 * @author Martyn Cutcher
 */
public class RESTServlet extends BigdataRDFServlet {

//    private static final transient Logger log = Logger
//            .getLogger(RESTServlet.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    /*
     * The delegates to which we dispatch the various requests.
     */
    private QueryServlet m_queryServlet;
    private DeleteServlet m_deleteServlet;
    private UpdateServlet m_updateServlet;

    public RESTServlet() {
    }

    /**
     * Overridden to create and initialize the delegate {@link Servlet}
     * instances.
     */
    @Override
    public void init() throws ServletException {
        
        super.init();
        
        m_queryServlet = new QueryServlet();
        m_updateServlet = new UpdateServlet();
        m_deleteServlet = new DeleteServlet();
        
        m_queryServlet.init(getServletConfig());
        m_updateServlet.init(getServletConfig());
        m_deleteServlet.init(getServletConfig());
        
    }
    
    /**
     * Overridden to destroy the delegate {@link Servlet} instances.
     */
    @Override
    public void destroy() {
        
        if (m_queryServlet != null) {
            m_queryServlet.destroy();
            m_queryServlet = null;
        }
        
        if (m_updateServlet != null) {
            m_updateServlet.destroy();
            m_updateServlet = null;
        }
        
        if (m_deleteServlet != null) {
            m_deleteServlet.destroy();
            m_deleteServlet = null;
        }
        
        super.destroy();
        
    }

	/**
	 * GET is only allowed with query requests, so delegate to the QueryServlet.
	 */
    @Override
    protected void doGet(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        m_queryServlet.doGet(req, resp);

    }

	/**
	 * A query can be submitted with a POST if a query parameter is provided.
	 * 
	 * Otherwise delegate to the UpdateServlet
	 */
    @Override
    protected void doPost(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        if (req.getParameter("delete") != null) {
            
            m_deleteServlet.doPost(req, resp);
            
        } else if (req.getParameter("query") != null) {
		
	        m_queryServlet.doGet(req, resp);
	        
		} else {
			
		    m_updateServlet.doPut(req, resp);
		    
		}

	}
	
	/**
	 * A PUT request always delegates to the UpdateServlet
	 */
    @Override
    protected void doPut(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        m_updateServlet.doPut(req, resp);

    }

    /**
     * A DELETE request will delete statements indicated by a provided namespace
     * URI and an optional query parameter.
     * 
     * Delegate to the DeleteServlet.
     */
    @Override
    protected void doDelete(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        m_deleteServlet.doDelete(req, resp);

    }

}
