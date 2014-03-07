/**
Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;

/**
 * Incremental truth maintenance servlet. Turns incremental truth maintenance
 * on and off, and allows database at once closure.
 * 
 * @see <a href="http://trac.bigdata.com/ticket/840">ticket</a>
 * 
 * @author tobycraig
 */
public class InferenceServlet extends BigdataRDFServlet {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    static private final transient Logger log = Logger.getLogger(MultiTenancyServlet.class); 

    /**
     * Set to true or false, this parameter indicates the state that incremental
     * truth maintenance should be set to.
     */
    private static final String TOGGLE = "toggle";

    /**
     * If present, compute closure.
     */
    private static final String DO_CLOSURE= "doClosure";

    public InferenceServlet() {

    }

    /**
     * Handle incremental truth maintenance.
     */
    @Override
    protected void doPost(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

    	final boolean toggle = req.getParameter(TOGGLE) != null;
    	final boolean doClosure= req.getParameter(DO_CLOSURE) != null;
    	
    	if(toggle) {
    		doToggle(req, resp);
    	}

    	if(doClosure) {
    		doClosure(req, resp);
    	}
    }
    
    /*
     * Set incremental truth maintenance according to request
     */
    private void doToggle(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

    	final String toggle = req.getParameter(TOGGLE);
        final String namespace = getNamespace(req);
    	try {
    		BigdataSailRepositoryConnection conn = getBigdataRDFContext()
    				.getUnisolatedConnection(namespace);
    		BigdataSailConnection sailConn = conn.getSailConnection(); 

    		if(toggle.equals("true")) {
        		sailConn.setTruthMaintenance(true);
        	} else if(toggle.equals("false")) {
        		sailConn.setTruthMaintenance(false);
        	}
    	} catch (Exception e) {
    		throw new RuntimeException();
    	}
    	
    }

    /*
     * Set off database at once closure. 
     */
    private void doClosure(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        final String namespace = getNamespace(req);
    	try {
    		BigdataSailRepositoryConnection conn = getBigdataRDFContext()
    				.getUnisolatedConnection(namespace);
    		BigdataSailConnection sailConn = conn.getSailConnection();
    		sailConn.computeClosure();
    	} catch (Exception e) {
    		throw new RuntimeException();
    	}

    }

}
