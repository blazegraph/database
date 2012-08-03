package com.bigdata.gom;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.gom.gpo.GPO;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.gpo.ILinkSet;
import com.bigdata.gom.om.ObjectManager;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal.Options;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.store.AbstractTripleStore;

import junit.framework.TestCase;

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
public class TestOwlGOM extends TestCase {
	BigdataSailRepositoryConnection m_cxn = null;
	ObjectManager m_om = null;
	ValueFactory m_vf = null;

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
		final URL xml = TestGOM.class.getResource(owlFile);

		load(xml, RDFFormat.RDFXML);

		IGPO owl = m_om.getGPO(OWL.ONTOLOGY);

		System.out.println(owl.pp());

		// Iterator<IGPO> ontos = owl.getLinksIn(rdfURI("type")).iterator();
		Iterator<IGPO> ontos = owl.getLinksIn().iterator();
		while (ontos.hasNext()) {
			IGPO onto = ontos.next();

			showOntology(onto);
		}

		ArrayList<IGPO> rootClasses = new ArrayList<IGPO>();
		IGPO classClass = m_om.getGPO(OWL.CLASS);
		System.out.println("ClassClass: " + classClass.pp());
		System.out.println("RDFS.SUBCLASSOF: " + RDFS.SUBCLASSOF);

		Iterator<IGPO> owlClasses = classClass.getLinksIn(RDF.TYPE).iterator();
		while (owlClasses.hasNext()) {
			final IGPO owlClass = owlClasses.next();
			System.out.println("OWL Class: " + owlClass.getId().stringValue());
			if (owlClass.getValue(RDFS.SUBCLASSOF) == null) {
				rootClasses.add(owlClass);
			}
		}

		showClassHierarchy(rootClasses.iterator(), 0);

		Iterator<IGPO> supers = classClass.getLinksOut(RDFS.SUBCLASSOF)
				.iterator();
		while (supers.hasNext()) {
			System.out.println("Superclass: " + supers.next().pp());
		}
		Iterator<IGPO> subs = classClass.getLinksIn(RDFS.SUBCLASSOF).iterator();
		while (subs.hasNext()) {
			System.out.println("Subclass: " + subs.next().pp());
		}

		// OWL.DATATYPEPROPERTY vs OWL.OBJECTPROPERTY
	}

	private void showClassHierarchy(Iterator<IGPO> classes, int indent) {
		StringBuilder out = new StringBuilder();
		showClassHierarchy(out, classes, indent);
		System.out.println("Hierarchy: " + out.toString());
	}

	private void showClassHierarchy(StringBuilder out, Iterator<IGPO> classes,
			int indent) {
		while (classes.hasNext()) {
			final IGPO clss = classes.next();
			out.append(indentOut(clss, indent + 1));
			showClassHierarchy(out,
					clss.getLinksIn(RDFS.SUBCLASSOF).iterator(), indent + 1);
		}
	}

	String indents = "\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t";

	private Object indentOut(IGPO clss, int indent) {
		Value lbl = clss.getValue(RDFS.LABEL);
		final String display = lbl == null ? clss.getId().stringValue() : lbl
				.stringValue();
		return indents.substring(0, indent) + display;
	}

	private void showOntology(IGPO onto) {
		System.out.println("Ontology: " + onto.pp());
		Iterator<IGPO> parts = onto.getLinksIn().iterator();
		while (parts.hasNext()) {
			IGPO part = parts.next();
			System.out.println("Onto Part: " + part.pp());
		}
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
	}

}
