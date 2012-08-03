package com.bigdata.gom;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.SailException;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.gom.gpo.BasicSkin;
import com.bigdata.gom.gpo.GPO;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.gpo.ILinkSet;
import com.bigdata.gom.om.IObjectManager;
import com.bigdata.gom.om.ObjectManager;
import com.bigdata.gom.skin.GenericSkinRegistry;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal.Options;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.search.FullTextIndex;

import junit.framework.TestCase;

/**
 * The DESCRIBE query retrieves relevant data to support
 * GPO read.
 * 
 * But the simplest model is one that relies directly on
 * a GPO interface to the triple store.
 * 
 * @author Martyn Cutcher
 *
 */
public class TestGOM extends TestCase {
	protected static final Logger log = Logger.getLogger(IObjectManager.class);
	BigdataSailRepositoryConnection m_cxn = null;
	private BigdataSail m_sail;

	/**
	 * Simple test loads data from a file and navigates around
	 */
	public void testSimpleDirectData() throws IOException, RDFParseException, RepositoryException {
		
		final URL n3 = TestGOM.class.getResource("testgom.n3");
		
		print(n3);
		load(n3);
		
		final ObjectManager om = new ObjectManager(UUID.randomUUID(), m_cxn);
		final ValueFactory vf = om.getValueFactory();
		
		final URI s = vf.createURI("gpo:#root");
		
		final URI rootAttr = vf.createURI("attr:/root");
		om.getGPO(s).getValue(rootAttr);
		final URI rootId = (URI) om.getGPO(s).getValue(rootAttr);
		
		final IGPO rootGPO = om.getGPO(rootId);
		
		if (log.isInfoEnabled()) {
			System.out.println("--------");
			System.out.println(rootGPO.pp());
		
			System.out.println(rootGPO.getType().pp());
		
			System.out.println(rootGPO.getType().getStatements());
		}
		
		final URI typeName = vf.createURI("attr:/type#name");
		assertTrue("Company".equals(rootGPO.getType().getValue(typeName).stringValue()));
		
		// find set of workers for the Company
		final URI worksFor = vf.createURI("attr:/employee#worksFor");
	    ILinkSet linksIn = rootGPO.getLinksIn(worksFor);
	    Iterator<IGPO> workers = linksIn.iterator();
	    while (workers.hasNext()) {
	    	log.info("Returned: " + workers.next().pp());
	    }
	    
	}
	
	/**
	 * Creates a simple GPO within a transaction.  Commits and and checks materialization
	 */
	public void testSimpleCreate() throws RepositoryException, IOException {
		
		final ObjectManager om = new ObjectManager(UUID.randomUUID(), m_cxn);
		final ValueFactory vf = om.getValueFactory();
		
		final URI keyname = vf.createURI("attr:/test#name");
		final Resource id = vf.createURI("gpo:test#1");
		om.checkValue(id); // ensure is imported!
		
		final int transCounter = om.beginNativeTransaction();
		try {
			IGPO gpo = om.getGPO(id);
			
			gpo.setValue(keyname, vf.createLiteral("Martyn"));			
			
			om.commitNativeTransaction(transCounter);
		} catch (Throwable t) {
			om.rollbackNativeTransaction();
			
			throw new RuntimeException(t);
		}
		
		// clear cached data
		om.clearCache();
		
		IGPO gpo = om.getGPO(vf.createURI("gpo:test#1")); // reads from backing journal
		
		assertTrue("Martyn".equals(gpo.getValue(keyname).stringValue()));
	}
	
	/**
	 * Checks getPropertyURIs of committed  GPO
	 */
	public void testSimpleProperties() throws RepositoryException, IOException {
		
		final ObjectManager om = new ObjectManager(UUID.randomUUID(), m_cxn);
		final ValueFactory vf = om.getValueFactory();
		
		final URI keyname = vf.createURI("attr:/test#name");
		final URI keyage = vf.createURI("attr:/test#age");
		final URI keyfriend = vf.createURI("attr:/test#friend");
		final Resource id = vf.createURI("gpo:test#1");
		final Resource id2 = vf.createURI("gpo:test#2");
		om.checkValue(id); // ensure is imported!
		om.checkValue(id2); // ensure is imported!
		
		final int transCounter = om.beginNativeTransaction();
		try {
			final IGPO gpo = om.getGPO(id);
			
			gpo.setValue(keyname, vf.createLiteral("Martyn"));			
			gpo.setValue(keyage, vf.createLiteral(53));			
			
			Iterator<URI> uris = ((GPO) gpo).getPropertyURIs();
			// should be two
			uris.next();
			uris.next();
			// .. and no more
			assertFalse(uris.hasNext());
			
			// add simple reverse link
			om.getGPO(id2).setValue(keyfriend, id);

			om.commitNativeTransaction(transCounter);
			
			uris = ((GPO) gpo).getPropertyURIs();
			// should be two
			uris.next();
			uris.next();
			// .. and no more
			assertFalse(uris.hasNext());
		} catch (Throwable t) {
			om.rollbackNativeTransaction();
			
			throw new RuntimeException(t);
		}
		
		// clear cached data
		om.clearCache();
		
		IGPO gpo = om.getGPO(vf.createURI("gpo:test#1")); // reads from backing journal
		
		Iterator<URI> uris = ((GPO) gpo).getPropertyURIs();
		// should be two
		uris.next();
		uris.next();
		// .. and no more
		assertFalse(uris.hasNext());
	}
	
	public void testSimpleReverseLinkProperties() throws RepositoryException, IOException {
		
		final ObjectManager om = new ObjectManager(UUID.randomUUID(), m_cxn);
		final ValueFactory vf = om.getValueFactory();
		
		final URI keyname = vf.createURI("attr:/test#name");
		final URI keyage = vf.createURI("attr:/test#age");
		final URI parent = vf.createURI("attr:/test#parent");
		final URI parent2 = vf.createURI("attr:/test#parent2");
		final URI nme = vf.createURI("gpo:ROOT");
		
		final int transCounter = om.beginNativeTransaction();
		try {
			IGPO gpo = om.createGPO();
			
			gpo.setValue(keyname, vf.createLiteral("Martyn"));			
			gpo.setValue(keyage, vf.createLiteral(53));		
			
			om.save(nme, gpo.getId());
			
			om.createGPO().setValue(parent, gpo.getId());
			om.createGPO().setValue(parent, gpo.getId());
			om.createGPO().setValue(parent, gpo.getId());
			om.createGPO().setValue(parent2, gpo.getId());
			om.createGPO().setValue(parent2, gpo.getId());
			om.createGPO().setValue(parent2, gpo.getId());
			
			om.commitNativeTransaction(transCounter);
		} catch (Throwable t) {
			om.rollbackNativeTransaction();
			
			throw new RuntimeException(t);
		}
		
		// clear cached data
		om.clearCache();
		
		IGPO gpo = om.getGPO((Resource) om.recall(nme)); // reads from backing journal
		
		Map<URI, Long> map = gpo.getReverseLinkProperties();
		
		// there should be 3 - the two parent properties plus the name manager
		assertTrue(map.size() == 3);
	}
	
	/**
	 * SimpleClassObjects tests navigation around a constructed network of GPOs.
	 * First some class objects are created, with superclass and metaclass data.
	 * Then instances of the classes are written.
	 * The purpose of this test is two fold: 1) To exercise the developing system
	 * and 2) To explore and develop utilities around the GPO usage.
	 * 
	 * Class: Person
	 * Class: Employee
	 * Class: Manager
	 * Class: Company
	 */
	public void testSimpleClassObjects() throws RepositoryException, IOException {
		
		final ObjectManager om = new ObjectManager(UUID.randomUUID(), m_cxn);
		final ValueFactory vf = om.getValueFactory();

		// This is the only required root!
		final URI clssclssName = vf.createURI("attr:/classClass");
		final URI descr = vf.createURI("attr:/classClass#description");
		
		final URI superClss = vf.createURI("attr:/class#super");
		final URI className = vf.createURI("attr:/class#name");
		
		final URI gpoType = vf.createURI("attr:/gpo#type");

		final int transCounter = om.beginNativeTransaction();
		try {
			// Object ClassClass provides hook into class hierarchy and thence
			//	into associated instances
			
			final IGPO clssclss = om.createGPO();
			
			om.save(clssclssName, clssclss.getId()); // remember the ClassClass
			
			clssclss.setValue(descr, vf.createLiteral("The class of all classes"));
			
			
			final IGPO clssPerson = om.createGPO();
			clssPerson.setValue(gpoType, clssclss.getId());
			clssPerson.setValue(className, vf.createLiteral("Person"));

			final IGPO clssEmployee = om.createGPO();
			clssEmployee.setValue(gpoType, clssclss.getId());
			clssEmployee.setValue(superClss, clssPerson.getId());
			clssEmployee.setValue(className, vf.createLiteral("Employee"));

			om.commitNativeTransaction(transCounter);
		} catch (Throwable t) {
			om.rollbackNativeTransaction();
			
			throw new RuntimeException(t);
		}
		
		// check state post commit
		{
			final IGPO clssclss = om.recallAsGPO(clssclssName);
			final Iterator<IGPO> classes = clssclss.getLinksIn(gpoType).iterator();
			while (classes.hasNext()) {
				System.out.println("Class: " + classes.next().pp());
			}
		}
		
		// clear cached data and run again rebuilding
		om.clearCache();
		{
			final IGPO clssclss = om.recallAsGPO(clssclssName);
			final Iterator<IGPO> classes = clssclss.getLinksIn(gpoType).iterator();
			while (classes.hasNext()) {
				final IGPO clss = classes.next();
				System.out.println("Class: " + clss.pp());
			}
		}
	}
	
	/**
	 * This tests both the usability and functionality of the Skinning mechanism
	 */
	public void testBasicSkin() {
		GenericSkinRegistry.registerClass(BasicSkin.class);
		final IObjectManager om = new ObjectManager(UUID.randomUUID(), m_cxn);
		
		GPO gpo = (GPO) om.createGPO();
		
		final BasicSkin skin = (BasicSkin) gpo.getSkin(BasicSkin.class);
		
		assertTrue(skin == gpo.getSkin(BasicSkin.class)); // check identity
		
		skin.setValue("attr:#name", "Martyn");
		
	}

	public void testIncrementalUpdates() throws RepositoryException, IOException {
		
		final IObjectManager om = new ObjectManager(UUID.randomUUID(), m_cxn);
		final ValueFactory vf = om.getValueFactory();

		final int transCounter = om.beginNativeTransaction();
		final URI name = vf.createURI("attr:/test#name");
		final URI ni = vf.createURI("attr:/test#ni");
		final URI age = vf.createURI("attr:/test#age");
		final URI mob = vf.createURI("attr:/test#mobile");
		final URI gender = vf.createURI("attr:/test#mail");
		try {
			// warmup
			for (int i = 0; i < 10000; i++) {
				final IGPO tst = om.createGPO();
				tst.setValue(name, vf.createLiteral("Test" + i));
			}

			// go for it
			final long start = System.currentTimeMillis();

			final int creates = 100000;
			for (int i = 0; i < creates; i++) {
				final IGPO tst = om.createGPO();
				tst.setValue(name, vf.createLiteral("Name" + i));
				tst.setValue(ni, vf.createLiteral("NI" + i));
				tst.setValue(age, vf.createLiteral(i));
				tst.setValue(mob, vf.createLiteral("0123-" + i));
				tst.setValue(gender, vf.createLiteral(1 % 3 == 0));
			}

			om.commitNativeTransaction(transCounter);
			
			final long duration = (System.currentTimeMillis()-start);
			
			// Note that this is a concervative estimate for statements per second since there is
			//	only one per object, requiring the object URI and the Value to be added.
			System.out.println("Creation rate of " + (creates*1000/duration) + " objects per second");
			System.out.println("Creation rate of " + (creates*5*1000/duration) + " statements per second");
		} catch (Throwable t) {
			t.printStackTrace();
			
			om.rollbackNativeTransaction();
			
			throw new RuntimeException(t);
		}
		
	}
	
	/**
	 * Utility to load n3 statements from a resource
	 */
	private void load(final URL n3) throws IOException, RDFParseException, RepositoryException {
		InputStream in = n3.openConnection().getInputStream();
		Reader reader = new InputStreamReader(in);
		
		m_cxn.add(reader, "", RDFFormat.N3);
	}

	/**
	 * com.bigdata.btree.writeRetentionQueue.capacity=4000
	 * com.bigdata.btree.BTree.branchingFactor=128
	 * 
	 * Note that higher branching factors and retentions require more memory.
	 * For good performance with retention: 1000, BF: 1000, 800M is required
	 * for incremental test.
	 * 
	 * In local tests I have seen just as good performance with lower (200) BF and
	 * less memory (200M).
	 * 
	 * @throws RepositoryException
	 * @throws IOException
	 */
	public void setUp() throws RepositoryException, IOException {
		Properties properties = new Properties();

		// create a backing file for the database
		File journal = File.createTempFile("bigdata", ".jnl");
		properties.setProperty(
			    BigdataSail.Options.FILE,
			    journal.getAbsolutePath()
			    );
        properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());
        properties.setProperty(AbstractTripleStore.Options.TEXT_INDEX, "false");
        properties.setProperty(IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY, "200");
        properties.setProperty("com.bigdata.namespace.kb.spo.SPO.com.bigdata.btree.BTree.branchingFactor", "200");
        properties.setProperty("com.bigdata.namespace.kb.spo.POS.com.bigdata.btree.BTree.branchingFactor", "200");
        properties.setProperty("com.bigdata.namespace.kb.spo.OSP.com.bigdata.btree.BTree.branchingFactor", "200");
        properties.setProperty("com.bigdata.namespace.kb.spo.BLOBS.com.bigdata.btree.BTree.branchingFactor", "200");
        properties.setProperty("com.bigdata.namespace.kb.lex.TERM2ID.com.bigdata.btree.BTree.branchingFactor", "200");
        properties.setProperty("com.bigdata.namespace.kb.lex.ID2TERM.com.bigdata.btree.BTree.branchingFactor", "200");

		// instantiate a sail and a Sesame repository
		m_sail = new BigdataSail(properties);
		BigdataSailRepository repo = new BigdataSailRepository(m_sail);
		repo.initialize();		
		
		m_cxn = repo.getConnection();
		m_cxn.setAutoCommit(false);
		
	}
	
	public void tearDown() {
		try {
			final long start = System.currentTimeMillis();
			m_cxn.close();
			m_sail.shutDown();
			final long dur = System.currentTimeMillis()-start;
			System.out.println("Sail shutdown: " + dur + "ms");
		} catch (SailException e) {
			e.printStackTrace();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
	}
	
	void print(final URL n3) throws IOException {
		if (log.isInfoEnabled()) {
			InputStream in = n3.openConnection().getInputStream();
			Reader reader = new InputStreamReader(in);
			try {
			char[] buf = new char[256];
			int rdlen = 0;
			while ((rdlen = reader.read(buf)) > -1) {
				if (rdlen == 256)
					System.out.print(buf);
				else
					System.out.print(new String(buf, 0, rdlen));
			}
			} finally {
				reader.close();
			}
		}
	}
	
	/**
	 * A simple test to compare native String interns with own-rolled ConcurrentHashMap based
	 * dictionary.  Figures suggest that the ConcurrentHashMap is a little slower building the
	 * table but then around 10 times faster processing the lookup.
	 * 
	 * The intention is to store such an "interned" String reference in the GPOs and to perform
	 * a sequential search for property keys rather than use a map.  This will minimise memory
	 * overhead and object creation times.
	 */
	public void notestPerfStringIntern() {
		Random r = new Random();
		ArrayList<String> strs = new ArrayList<String>();
		for (int i = 0; i < 10000; i++) {
			strs.add("url://this/is/a/long/string/" + (987654 + r.nextInt(1000000)));
		}
		for (int i = 0; i < 20; i++) {
			final long start = System.nanoTime();
			Iterator<String> striter = strs.iterator();
			while (striter.hasNext()) {
				assertTrue(striter.next().intern() != null); // just to keep find bugs happy
			}
			final long end = System.nanoTime();
			System.out.println("Interns#" + i + ":" + (end-start));
		}
		final ConcurrentHashMap<String, String> dict = new ConcurrentHashMap<String, String>();
		for (int i = 0; i < 20; i++) {
			final long start = System.nanoTime();
			Iterator<String> striter = strs.iterator();
			while (striter.hasNext()) {
				final String str = striter.next();
				String ret = dict.get(str);
				if (ret == null) {
					dict.put(str, str);		
				}
			}
			final long end = System.nanoTime();
			System.out.println("HashMap#" + i + ":" + (end-start));
		}
		

	}
}
