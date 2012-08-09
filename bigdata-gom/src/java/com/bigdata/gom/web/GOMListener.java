package com.bigdata.gom.web;

import java.util.UUID;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

import org.openrdf.repository.RepositoryException;

import com.bigdata.gom.om.ObjectManager;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext;
import com.bigdata.rdf.sail.webapp.BigdataRDFServletContextListener;
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
        
        final BigdataRDFContext rdfContext = getBigdataRDFContext();
        
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
