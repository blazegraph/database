package com.bigdata.rdf.sail.webapp;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

/**
 * Handler for UPDATE operations (PUT).
 * 
 * @author martyncutcher
 * 
 *         FIXME The UPDATE API is not finished yet. It will provide
 *         DELETE+INSERT semantics.
 */
public class UpdateServlet extends BigdataRDFServlet {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    static private final transient Logger log = Logger
            .getLogger(UpdateServlet.class);

    public UpdateServlet() {

    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) {
        throw new UnsupportedOperationException();
    }

}
