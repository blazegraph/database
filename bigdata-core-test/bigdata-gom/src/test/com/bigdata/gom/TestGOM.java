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
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.bigdata.gom.gpo.BasicSkin;
import com.bigdata.gom.gpo.GPO;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.gpo.ILinkSet;
import com.bigdata.gom.om.ObjectMgrModel;
import com.bigdata.gom.skin.GenericSkinRegistry;

/**
 * Base test suite for the embedded (local) GOM.
 * 
 * @author Martyn Cutcher
 */
public class TestGOM extends ProxyGOMTest {

    private static final Logger log = Logger.getLogger(TestGOM.class);

    /**
	 * Simple test loads data from a file and navigates around
	 */
	public void testSimpleDirectData() throws IOException, RDFParseException, RepositoryException {

        final URL n3 = TestGOM.class.getResource("testgom.n3");

        // print(n3);
		((IGOMProxy) m_delegate).load(n3, RDFFormat.N3);

        final ValueFactory vf = om.getValueFactory();

        final URI s = vf.createURI("gpo:#root");

        final URI rootAttr = vf.createURI("attr:/root");
        om.getGPO(s).getValue(rootAttr);
        final URI rootId = (URI) om.getGPO(s).getValue(rootAttr);

        final IGPO rootGPO = om.getGPO(rootId);

        if (log.isInfoEnabled()) {
            log.info("--------\n" + rootGPO.pp() + "\n"
                    + rootGPO.getType().pp() + "\n"
                    + rootGPO.getType().getStatements());
        }

        final URI typeName = vf.createURI("attr:/type#name");

        assertTrue("Company".equals(rootGPO.getType().getValue(typeName)
                .stringValue()));

        // find set of workers for the Company
        final URI worksFor = vf.createURI("attr:/employee#worksFor");
        final ILinkSet linksIn = rootGPO.getLinksIn(worksFor);
        final Iterator<IGPO> workers = linksIn.iterator();
        while (workers.hasNext()) {
            final IGPO tmp = workers.next();
            if (log.isInfoEnabled())
                log.info("Returned: " + tmp.pp());
        }

    }

    /**
     * Creates a simple GPO within a transaction. Commits and and checks
     * materialization
     */
    public void testSimpleCreate() throws RepositoryException, IOException {

        final ValueFactory vf = om.getValueFactory();

        final URI keyname = vf.createURI("attr:/test#name");
        final Resource id = vf.createURI("gpo:test#1");
//        om.checkValue(id); // ensure is imported!

        final int transCounter = om.beginNativeTransaction();
        try {
            final IGPO gpo = om.getGPO(id);

            gpo.setValue(keyname, vf.createLiteral("Martyn"));

            om.commitNativeTransaction(transCounter);
        } catch (Throwable t) {
            om.rollbackNativeTransaction();

            throw new RuntimeException(t);
        }

        // clear cached data
        ((ObjectMgrModel) om).clearCache();

        // reads from backing journal
        final IGPO gpo = om.getGPO(vf.createURI("gpo:test#1"));

        assertTrue("Martyn".equals(gpo.getValue(keyname).stringValue()));

    }
	
	/**
	 * Checks getPropertyURIs of committed  GPO
	 */
	public void testSimpleProperties() throws RepositoryException, IOException {

        final ValueFactory vf = om.getValueFactory();

        final URI keyname = vf.createURI("attr:/test#name");
        final URI keyage = vf.createURI("attr:/test#age");
        final URI keyfriend = vf.createURI("attr:/test#friend");
        final Resource id = vf.createURI("gpo:test#1");
        final Resource id2 = vf.createURI("gpo:test#2");
//        om.checkValue(id); // ensure is imported!
//        om.checkValue(id2); // ensure is imported!

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
        ((ObjectMgrModel) om).clearCache();

        {
            // reads from backing journal
            final IGPO gpo = om.getGPO(vf.createURI("gpo:test#1"));
            final Iterator<URI> uris = ((GPO) gpo).getPropertyURIs();
            // should be two
            uris.next();
            uris.next();
            // .. and no more
            assertFalse(uris.hasNext());
        }

    }

    public void testSimpleReverseLinkProperties() throws RepositoryException,
            IOException {

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
        ((ObjectMgrModel) om).clearCache();

        IGPO gpo = om.getGPO((Resource) om.recall(nme)); // reads from
                                                         // backing journal

        Map<URI, Long> map = gpo.getReverseLinkProperties();

        // there should be 3 - the two parent properties plus the name
        // manager
        assertTrue(map.size() == 3);

    }
    
    /**
     * SimpleClassObjects tests navigation around a constructed network of GPOs.
     * First some class objects are created, with superclass and metaclass data.
     * Then instances of the classes are written. The purpose of this test is
     * two fold: 1) To exercise the developing system and 2) To explore and
     * develop utilities around the GPO usage.
     * 
     * Class: Person Class: Employee Class: Manager Class: Company
     */
    public void testSimpleClassObjects() throws RepositoryException,
            IOException {

        final ValueFactory vf = om.getValueFactory();

        // This is the only required root!
        final URI clssclssName = vf.createURI("attr:/classClass");
        final URI descr = vf.createURI("attr:/classClass#description");

        final URI superClss = vf.createURI("attr:/class#super");
        final URI className = vf.createURI("attr:/class#name");

        final URI gpoType = vf.createURI("attr:/gpo#type");

        final int transCounter = om.beginNativeTransaction();
        try {
            // Object ClassClass provides hook into class hierarchy and
            // thence
            // into associated instances

            final IGPO clssclss = om.createGPO();

            om.save(clssclssName, clssclss.getId()); // remember the
                                                     // ClassClass

            clssclss.setValue(descr,
                    vf.createLiteral("The class of all classes"));

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

        // check state post commit : TODO Tests what? Just iterating...
        {
            final IGPO clssclss = om.recallAsGPO(clssclssName);
            final Iterator<IGPO> classes = clssclss.getLinksIn(gpoType)
                    .iterator();
            while (classes.hasNext()) {
                final IGPO cls = classes.next();
                if (log.isInfoEnabled())
                    log.info("Class: " + cls.pp());
            }
        }

        // clear cached data and run again rebuilding
        ((ObjectMgrModel) om).clearCache();
        {
            final IGPO clssclss = om.recallAsGPO(clssclssName);
            final Iterator<IGPO> classes = clssclss.getLinksIn(gpoType)
                    .iterator();
            while (classes.hasNext()) {
                final IGPO clss = classes.next();
                if (log.isInfoEnabled())
                    log.info("Class: " + clss.pp());
            }
        }

    }

    /**
     * This tests both the usability and functionality of the Skinning mechanism
     */
    public void testBasicSkin() {

        GenericSkinRegistry.registerClass(BasicSkin.class);

        final GPO gpo = (GPO) om.createGPO();

        final BasicSkin skin = (BasicSkin) gpo.getSkin(BasicSkin.class);

        assertTrue(skin == gpo.getSkin(BasicSkin.class)); // check identity

        skin.setValue("attr:#name", "Martyn");

    }

    /**
     * Throughput test for updates.
     */
    public void testUpdateThroughput() throws RepositoryException, IOException {

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

            // The LocalGOMTestcase can handle much larger tests if
            //	incremental updating is enabled
            final int creates = 50000;
            for (int i = 0; i < creates; i++) {
                final IGPO tst = om.createGPO();
                tst.setValue(name, vf.createLiteral("Name" + i));
                tst.setValue(ni, vf.createLiteral("NI" + i));
                tst.setValue(age, vf.createLiteral(i));
                tst.setValue(mob, vf.createLiteral("0123-" + i));
                tst.setValue(gender, vf.createLiteral(1 % 3 == 0));
            }

            om.commitNativeTransaction(transCounter);

            final long duration = (System.currentTimeMillis() - start);
            
            final long objectsPS = creates * 1000 / duration;
            final long statementsPS = objectsPS * 5;

            /*
             * Note that this is a conservative estimate for statements per
             * second since there is only one per object, requiring the object
             * URI and the Value to be added.
             */
            if (log.isInfoEnabled()) {

                log.info("Creation rate of " + objectsPS
                        + " objects per second");

                log.info("Creation rate of " + statementsPS
                        + " statements per second");

            }
            
            // Anything less than 5000 statements per second is a failure
            assertTrue(statementsPS > 5000);
        } catch (Throwable t) {
            // t.printStackTrace();

            om.rollbackNativeTransaction();

            throw new RuntimeException(t);
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
		final Random r = new Random();
		final int limit = 10000;
		final ArrayList<String> strs = new ArrayList<String>(limit);
        for (int i = 0; i < limit; i++) {
            strs.add("url://this/is/a/long/string/"
                    + (987654 + r.nextInt(1000000)));
        }
		for (int i = 0; i < 20; i++) {
			final long start = System.nanoTime();
			final Iterator<String> striter = strs.iterator();
			while (striter.hasNext()) {
				assertTrue(striter.next().intern() != null); // just to keep find bugs happy
			}
            final long end = System.nanoTime();
            if (log.isInfoEnabled())
                log.info("Interns#" + i + ":" + (end - start));
        }
		final ConcurrentHashMap<String, String> dict = new ConcurrentHashMap<String, String>();
		for (int i = 0; i < 20; i++) {
			final long start = System.nanoTime();
			final Iterator<String> striter = strs.iterator();
			while (striter.hasNext()) {
				final String str = striter.next();
				final String ret = dict.get(str);
				if (ret == null) {
					dict.put(str, str);		
				}
			}
            final long end = System.nanoTime();
            if (log.isInfoEnabled())
                log.info("HashMap#" + i + ":" + (end - start));
        }
		
	}

}
