package com.bigdata.gom.web;

import java.util.UUID;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.bigdata.gom.om.ObjectManager;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext;
import com.bigdata.rdf.sail.webapp.BigdataRDFServletContextListener;

/**
 * Extends the BigdataRDFServletContextListener to add a local ObjectManager
 * initialization.
 * 
 * @author Martyn Cutcher
 *
 */
public class GOMListener extends BigdataRDFServletContextListener {
	@Override
	public void contextDestroyed(ServletContextEvent ev) {
		super.contextDestroyed(ev);
	}

	@Override
	public void contextInitialized(ServletContextEvent ev) {
		super.contextInitialized(ev);
		
        final ServletContext context = ev.getServletContext();

        final UUID uuid = UUID.fromString(context.getInitParameter("om-uuid"));
        
        final BigdataRDFContext rdfContext = getBigdataRDFContext();
        
        final String namespace = rdfContext.getConfig().namespace;
        try {
			final ObjectManager om = new ObjectManager(uuid, rdfContext.getUnisolatedConnection(namespace));
			context.setAttribute(ObjectManager.class.getName(), om);
		} catch (SailException e) {
			throw new RuntimeException(e);
		} catch (RepositoryException e) {
			throw new RuntimeException(e);
		}
	}

}
