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
/*
 * Created on Mar 19, 2012
 */
package com.bigdata.gom;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import junit.extensions.proxy.IProxyTest;
import junit.framework.Test;
import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.bigdata.gom.gpo.IGPO;

/**
 * This tests a skin to help process an OWL specification.
 * 
 * The idea is to be able to define a usable interface to OWL that in itself
 * will support the development of the Alchemist to generate such skins for
 * other models.
 * 
 * @author Martyn Cutcher
 * 
 */
public class TestOwlGOM extends ProxyGOMTest {

    private static final Logger log = Logger.getLogger(TestOwlGOM.class);

	final static String OWL_NAMESPACE = "http://www.w3.org/2002/07/owl#";
	final static String RDFS_NAMESPACE = "http://www.w3.org/2000/01/rdf-schema#";
	final static String RDF_NAMESPACE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

	URI owlURI(final String nme) {
		return m_vf.createURI(OWL_NAMESPACE + nme);
	}

	URI rdfsURI(final String nme) {
		return m_vf.createURI(RDFS_NAMESPACE + nme);
	}

	URI rdfURI(final String nme) {
		return m_vf.createURI(RDF_NAMESPACE + nme);
	}

	public void testOwlLoad1() throws RDFParseException, RepositoryException,
			IOException {

	    doLoad("testowl.xml");
	    
	}

    public void testOwlLoad2() throws RDFParseException, RepositoryException,
            IOException {

        doLoad("testowl2.xml");

    }

    private void doLoad(final String owlFile) throws RDFParseException,
            RepositoryException, IOException {

        ((IGOMProxy) m_delegate).load(TestGOM.class.getResource(owlFile), RDFFormat.RDFXML);

        final IGPO owl = om.getGPO(OWL.ONTOLOGY);

        if (log.isInfoEnabled())
            log.info(owl.pp());

        {
            // Iterator<IGPO> ontos =
            // owl.getLinksIn(rdfURI("type")).iterator();
            final Iterator<IGPO> ontos = owl.getLinksIn().iterator();
            while (ontos.hasNext()) {
                final IGPO onto = ontos.next();

                showOntology(onto);
            }
        }

        final ArrayList<IGPO> rootClasses = new ArrayList<IGPO>();
        final IGPO classClass = om.getGPO(OWL.CLASS);
        if (log.isInfoEnabled()) {
            log.info("ClassClass: " + classClass.pp());
            log.info("RDFS.SUBCLASSOF: " + RDFS.SUBCLASSOF);
        }

        {
            final Iterator<IGPO> owlClasses = classClass.getLinksIn(RDF.TYPE)
                    .iterator();
            while (owlClasses.hasNext()) {
                final IGPO owlClass = owlClasses.next();
                if (log.isInfoEnabled()) {
                    log.info("OWL Class: " + owlClass.getId().stringValue());
                }
                if (owlClass.getValue(RDFS.SUBCLASSOF) == null) {
                    rootClasses.add(owlClass);
                }
            }
        }

        showClassHierarchy(rootClasses.iterator(), 0);

        // TODO This is not *testing* anything.
        {
            final Iterator<IGPO> supers = classClass.getLinksOut(
                    RDFS.SUBCLASSOF).iterator();
            while (supers.hasNext()) {
                final IGPO tmp = supers.next();
                if (log.isInfoEnabled())
                    log.info("Superclass: " + tmp.pp());
            }
        }
        {
            final Iterator<IGPO> subs = classClass.getLinksIn(RDFS.SUBCLASSOF)
                    .iterator();
            while (subs.hasNext()) {
                final IGPO tmp = subs.next();
                if (log.isInfoEnabled())
                    log.info("Subclass: " + tmp.pp());
            }
        }

        // OWL.DATATYPEPROPERTY vs OWL.OBJECTPROPERTY

	}

    //	/**
//	 * Utility to load n3 statements from a resource
//	 */
//	private void load(final URL data, final RDFFormat format)
//			throws IOException, RDFParseException, RepositoryException {
//	    final InputStream in = data.openConnection().getInputStream();
//	    final Reader reader = new InputStreamReader(in);
//		m_repo.getConnection().setAutoCommit(false);
//
//		m_repo.getConnection().add(reader, "", format);
//	}

//	public void setUp() throws RepositoryException, IOException {
//		Properties properties = new Properties();
//
//		// create a backing file for the database
//		File journal = File.createTempFile("bigdata", ".jnl");
//		properties.setProperty(BigdataSail.Options.FILE, journal
//				.getAbsolutePath());
//		properties
//				.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
//
//		properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW
//				.toString());
//		properties.setProperty(AbstractTripleStore.Options.TEXT_INDEX, "false");
//		properties.setProperty(
//				IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY, "200");
//		properties
//				.setProperty(
//						"com.bigdata.namespace.kb.spo.SPO.com.bigdata.btree.BTree.branchingFactor",
//						"200");
//		properties
//				.setProperty(
//						"com.bigdata.namespace.kb.spo.POS.com.bigdata.btree.BTree.branchingFactor",
//						"200");
//		properties
//				.setProperty(
//						"com.bigdata.namespace.kb.spo.OSP.com.bigdata.btree.BTree.branchingFactor",
//						"200");
//		properties
//				.setProperty(
//						"com.bigdata.namespace.kb.spo.BLOBS.com.bigdata.btree.BTree.branchingFactor",
//						"200");
//		properties
//				.setProperty(
//						"com.bigdata.namespace.kb.lex.TERM2ID.com.bigdata.btree.BTree.branchingFactor",
//						"200");
//		properties
//				.setProperty(
//						"com.bigdata.namespace.kb.lex.ID2TERM.com.bigdata.btree.BTree.branchingFactor",
//						"200");
//
//		// instantiate a sail and a Sesame repository
//		BigdataSail sail = new BigdataSail(properties);
//		m_repo = new BigdataSailRepository(sail);
//		m_repo.initialize();
//
//		m_om = new ObjectManager(m_repo);
//		m_vf = m_om.getValueFactory();
//	}

}
