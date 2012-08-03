package com.bigdata.gom;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.gom.alchemy.owl.OWLClassSkin;
import com.bigdata.gom.alchemy.owl.OWLOntologySkin;
import com.bigdata.gom.alchemy.owl.OWLPropertySkin;
import com.bigdata.gom.alchemy.owl.OWLSkins;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.om.ObjectManager;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal.Options;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.store.AbstractTripleStore;

import junit.framework.TestCase;

public class TestOWLSkin extends TestCase {
	BigdataSailRepositoryConnection m_cxn = null;
	ObjectManager m_om = null;
	ValueFactory m_vf = null;

	public void testOWLwithProperties() throws RDFParseException, RepositoryException, IOException {
		showOntology("testowl2.xml");
	}
	
	public void testSimpleOWL() throws RDFParseException, RepositoryException, IOException {
		showOntology("testowl.xml");
	}
	
	public void testUnivBenchOWL() throws RDFParseException, RepositoryException, IOException {
		showOntology("univ-bench.owl");
	}
	
	private void showOntology(String owlfile) throws RDFParseException, RepositoryException, IOException {
		doLoad(owlfile);
		final OWLOntologySkin onto = OWLOntologySkin.getOntology(m_om);
		Iterator<OWLClassSkin> classes = onto.getClasses();
		while (classes.hasNext()) {
			final OWLClassSkin clss = classes.next();
			System.out.println(clss.getName());
			Iterator<OWLClassSkin> subclasses = clss.getSubclasses();
			if (subclasses.hasNext()) {
				System.out.print("Subclasses: ");
				while (subclasses.hasNext()) {
					System.out.print(subclasses.next().getName());
					if (subclasses.hasNext()) {
						System.out.print(", ");
					}
				}
				System.out.println();
			}
			final Iterator<OWLPropertySkin> props = clss.getProperties();
			while (props.hasNext()) {
				final OWLPropertySkin prop = props.next();
				final IGPO typ = prop.getType();
				System.out.println("- " + prop.getName() + " : " + (typ == null ? null : typ.getId().stringValue()));
			}
		}
	}
	
	private void doLoad(final String owlFile) throws RDFParseException,
			RepositoryException, IOException {
		final URL xml = TestGOM.class.getResource(owlFile);

		load(xml, RDFFormat.RDFXML);
	}

	public void setUp() throws RepositoryException, IOException {
		Properties properties = new Properties();

		// create a backing file for the database
		File journal = File.createTempFile("bigdata", ".jnl");
		properties.setProperty(BigdataSail.Options.FILE, journal
				.getAbsolutePath());
		properties
				.setProperty("com.bigdata.rdf.sail.truthMaintenance", "false");

		properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW
				.toString());
		properties.setProperty(AbstractTripleStore.Options.TEXT_INDEX, "false");
		properties.setProperty(
				IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY, "200");
		properties
				.setProperty(
						"com.bigdata.namespace.kb.spo.SPO.com.bigdata.btree.BTree.branchingFactor",
						"200");
		properties
				.setProperty(
						"com.bigdata.namespace.kb.spo.POS.com.bigdata.btree.BTree.branchingFactor",
						"200");
		properties
				.setProperty(
						"com.bigdata.namespace.kb.spo.OSP.com.bigdata.btree.BTree.branchingFactor",
						"200");
		properties
				.setProperty(
						"com.bigdata.namespace.kb.spo.BLOBS.com.bigdata.btree.BTree.branchingFactor",
						"200");
		properties
				.setProperty(
						"com.bigdata.namespace.kb.lex.TERM2ID.com.bigdata.btree.BTree.branchingFactor",
						"200");
		properties
				.setProperty(
						"com.bigdata.namespace.kb.lex.ID2TERM.com.bigdata.btree.BTree.branchingFactor",
						"200");

		// instantiate a sail and a Sesame repository
		BigdataSail sail = new BigdataSail(properties);
		BigdataSailRepository repo = new BigdataSailRepository(sail);
		repo.initialize();

		m_cxn = repo.getConnection();
		m_cxn.setAutoCommit(false);

		m_om = new ObjectManager(UUID.randomUUID(), m_cxn);
		m_vf = m_om.getValueFactory();
		
		OWLSkins.register();
	}

	/**
	 * Utility to load n3 statements from a resource
	 */
	private void load(final URL data, final RDFFormat format)
			throws IOException, RDFParseException, RepositoryException {
		InputStream in = data.openConnection().getInputStream();
		Reader reader = new InputStreamReader(in);

		m_cxn.add(reader, "", format);
	}
}
