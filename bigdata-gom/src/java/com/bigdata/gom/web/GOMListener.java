package com.bigdata.gom.web;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.bigdata.gom.om.IObjectManager;
import com.bigdata.gom.om.ObjectManager;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.BigdataRDFServletContextListener;
import com.bigdata.rdf.sail.webapp.ConfigParams;
import com.bigdata.rdf.store.AbstractTripleStore;

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
        
        final String namespace = rdfContext.getConfig().namespace;
        try {
            final AbstractTripleStore tripleStore = (AbstractTripleStore) rdfContext.getIndexManager()
            .getResourceLocator().locate(namespace, ITx.UNISOLATED);

		    if (tripleStore == null) {
		        throw new RuntimeException("Not found: namespace=" + namespace);
		    }
		
		    // Wrap with SAIL.
		    final BigdataSail sail = new BigdataSail(tripleStore);
		
		    final BigdataSailRepository repo = new BigdataSailRepository(sail);
		
		    repo.initialize();
		
		    final ObjectManager om = new ObjectManager(uuid, repo);
			context.setAttribute(ObjectManager.class.getName(), om);
		} catch (RepositoryException e) {
			throw new RuntimeException(e);
		}
	}

}
