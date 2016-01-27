/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2011.  All rights reserved.

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

package com.bigdata.rdf.sail.contrib;

import java.io.StringReader;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.ProxyBigdataSailTestCase;

/**
 * Unit test template for use in submission of bugs.
 * <p>
 * This test case will delegate to an underlying backing store. You can specify
 * this store via a JVM property as follows:
 * <code>-DtestClass=com.bigdata.rdf.sail.TestBigdataSailWithQuads</code>
 * <p>
 * There are three possible configurations for the testClass:
 * <ul>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithQuads (quads mode)</li>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithoutSids (triples mode)</li>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithSids (SIDs mode)</li>
 * </ul>
 * <p>
 * The default for triples and SIDs mode is for inference with truth maintenance
 * to be on. If you would like to turn off inference, make sure to do so in
 * {@link #getProperties()}.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class TestOOMBug extends ProxyBigdataSailTestCase {

	private static final transient Logger log = Logger.getLogger(TestOOMBug.class);
	
	
	public TestOOMBug() {
	}

	public TestOOMBug(String arg0) {
		super(arg0);
	}

	/**
	 * Please set your database properties here, except for your journal file,
	 * please DO NOT SPECIFY A JOURNAL FILE.
	 */
	@Override
	public Properties getProperties() {

		final Properties props = super.getProperties();

		final StringBuilder sb = new StringBuilder();
		
		/*
		 * This one is the culprit.
		 */
		sb.append("com.bigdata.btree.BTree.branchingFactor=4196").append("\n");
		
//		sb.append("com.bigdata.journal.AbstractJournal.bufferMode=DiskRW").append("\n");
//		sb.append("com.bigdata.btree.writeRetentionQueue.capacity=8000").append("\n");
//		sb.append("com.bigdata.LRUNexus.enabled=false").append("\n");
//		sb.append("com.bigdata.io.DirectBufferPool.bufferCapacity=10485760").append("\n");
//		sb.append("com.bigdata.rwstore.RWStore.allocationSizes=1, 2, 3, 5, 8, 12, 16, 32, 48, 64, 128, 192, 320, 512, 832, 1344, 2176, 3520").append("\n");
//		sb.append("com.bigdata.journal.AbstractJournal.initialExtent=209715200").append("\n");
//		sb.append("com.bigdata.journal.AbstractJournal.maximumExtent=209715200").append("\n");
//		sb.append("com.bigdata.rdf.sail.queryTimeExpander=false").append("\n");
		
		try {
			props.load(new StringReader(sb.toString()));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		if (log.isInfoEnabled())
			for (Object o : props.entrySet())
				log.info(o);

		return props;

	}

	public void testOOM() throws Exception {
		final BigdataSail sail = getSail();
		
		try {
			
			sail.initialize();
			final BigdataSailRepository repo = new BigdataSailRepository(sail);
			final BigdataSailRepositoryConnection cxn = repo.getConnection();
			cxn.setAutoCommit(false);
			
			final BigdataValueFactory vf = cxn.getValueFactory();
			
			try {
			
				// first add the data
				for(int index = 0; index < triples.length; index++){
					org.openrdf.model.URI 	s = vf.createURI(triples[index][0]);
					org.openrdf.model.URI 	p = vf.createURI(triples[index][1]);
					if (triples[index][3] != null){
						org.openrdf.model.Literal 	l = vf.createLiteral(triples[index][2],vf.createURI(triples[index][3]));
						if (log.isInfoEnabled())
							log.info("Adding("+index+" of "+triples.length+") "+s+" "+p+" "+l);
						cxn.add(s,p,l);
					}
					else {
						org.openrdf.model.URI 		o = vf.createURI(triples[index][2]);
						if (log.isInfoEnabled())
							log.info("Adding("+index+" of "+triples.length+") "+s+" "+p+" "+o);
						cxn.add(s,p,o);
					}
				}
				cxn.commit();

				if (log.isInfoEnabled())
					log.info("\n"+cxn.getTripleStore().dumpStore(true, true, false));
				
				// then test the interleaving of removes() with adds()
				if (log.isInfoEnabled())
					log.info("Start data processing...");
				for(int index = 0; index < triples.length; index++){
					org.openrdf.model.URI 	s = vf.createURI(triples[index][0]);
					org.openrdf.model.URI 	p = vf.createURI(triples[index][1]);
					if (triples[index][3] != null){
						org.openrdf.model.Literal 	l = vf.createLiteral(triples[index][2],vf.createURI(triples[index][3]));
						if (log.isInfoEnabled())
							log.info("Processing("+index+" of "+triples.length+") "+s+" "+p+" "+l);
						cxn.remove(s, p, null);		cxn.add(s,p,l);
					}
					else {
						org.openrdf.model.URI 		o = vf.createURI(triples[index][2]);
						if (log.isInfoEnabled())
							log.info("Processing("+index+" of "+triples.length+") "+s+" "+p+" "+o);
						cxn.remove(s, p, null);		cxn.add(s,p,o);
					}
				}
				if (log.isInfoEnabled())
					log.info("End data processing...");
				
				cxn.commit();
				
				if (log.isInfoEnabled())
					log.info("\n"+cxn.getTripleStore().dumpStore(true, true, false));
				
			} finally {
				cxn.close();
			}
			
		} finally {
			sail.__tearDownUnitTest();
		}
		
	}
	
	
	protected static final String[][]		triples = new String[][]{
		   new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.test.com/ontology#ExcelFile", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee", "http://www.test.com/ontology#fileName", "D:\\testFiles\\changes\\worksheet.xlsx", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee", "http://www.test.com/ontology#author", "", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee", "http://www.test.com/ontology#lastAuthor", "", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee", "http://www.test.com/ontology#applicationName", "Microsoft Excel", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee", "http://www.test.com/ontology#creationDate", "2006-09-16T06:00:00Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee", "http://www.test.com/ontology#lastSaveTime", "2011-07-15T12:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee", "http://www.test.com/ontology#security", "0", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee", "http://www.test.com/ontology#company", "", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasColumn", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col0", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col0", "http://www.w3.org/2000/01/rdf-schema#subPropertyOf", "http://www.test.com/ontology#tableValue", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col0", "http://www.w3.org/2000/01/rdf-schema#label", "A", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col0", "http://www.test.com/ontology#tableOrder", "1", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasColumn", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col1", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col1", "http://www.w3.org/2000/01/rdf-schema#subPropertyOf", "http://www.test.com/ontology#tableValue", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col1", "http://www.w3.org/2000/01/rdf-schema#label", "B", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col1", "http://www.test.com/ontology#tableOrder", "2", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasColumn", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col2", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col2", "http://www.w3.org/2000/01/rdf-schema#subPropertyOf", "http://www.test.com/ontology#tableValue", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col2", "http://www.w3.org/2000/01/rdf-schema#label", "C", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col2", "http://www.test.com/ontology#tableOrder", "3", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasColumn", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col3", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col3", "http://www.w3.org/2000/01/rdf-schema#subPropertyOf", "http://www.test.com/ontology#tableValue", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col3", "http://www.w3.org/2000/01/rdf-schema#label", "D", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col3", "http://www.test.com/ontology#tableOrder", "4", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasColumn", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col4", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col4", "http://www.w3.org/2000/01/rdf-schema#subPropertyOf", "http://www.test.com/ontology#tableValue", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col4", "http://www.w3.org/2000/01/rdf-schema#label", "E", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col4", "http://www.test.com/ontology#tableOrder", "5", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasColumn", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col5", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col5", "http://www.w3.org/2000/01/rdf-schema#subPropertyOf", "http://www.test.com/ontology#tableValue", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col5", "http://www.w3.org/2000/01/rdf-schema#label", "F", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col5", "http://www.test.com/ontology#tableOrder", "6", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasColumn", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col6", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col6", "http://www.w3.org/2000/01/rdf-schema#subPropertyOf", "http://www.test.com/ontology#tableValue", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col6", "http://www.w3.org/2000/01/rdf-schema#label", "G", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col6", "http://www.test.com/ontology#tableOrder", "7", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row0", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.test.com/ontology#TableRow", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasRow", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row0", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row0", "http://www.test.com/ontology#tableOrder", "1", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row0", "http://www.test.com/ontology#docID", "1", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row0", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col0", "123", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row0", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col1", "123", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row1", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.test.com/ontology#TableRow", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasRow", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row1", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row1", "http://www.test.com/ontology#tableOrder", "2", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row1", "http://www.test.com/ontology#docID", "2", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row1", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col0", "123", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row1", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col1", "123", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row2", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.test.com/ontology#TableRow", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasRow", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row2", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row2", "http://www.test.com/ontology#tableOrder", "3", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row2", "http://www.test.com/ontology#docID", "3", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row2", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col0", "123", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row2", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col1", "123", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row3", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.test.com/ontology#TableRow", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasRow", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row3", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row3", "http://www.test.com/ontology#tableOrder", "4", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row3", "http://www.test.com/ontology#docID", "4", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row3", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col0", "123", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row3", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col1", "123", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row4", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.test.com/ontology#TableRow", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasRow", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row4", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row4", "http://www.test.com/ontology#tableOrder", "5", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row4", "http://www.test.com/ontology#docID", "5", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row4", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col1", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row4", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col2", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row4", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col3", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row4", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col4", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row4", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col5", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row4", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col6", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row5", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.test.com/ontology#TableRow", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasRow", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row5", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row5", "http://www.test.com/ontology#tableOrder", "6", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row5", "http://www.test.com/ontology#docID", "6", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row5", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col1", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row5", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col2", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row5", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col3", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row5", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col4", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row5", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col5", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row5", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col6", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row6", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.test.com/ontology#TableRow", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasRow", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row6", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row6", "http://www.test.com/ontology#tableOrder", "7", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row6", "http://www.test.com/ontology#docID", "7", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row6", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col1", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row6", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col2", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row6", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col3", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row6", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col4", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row6", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col5", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row6", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col6", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row7", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.test.com/ontology#TableRow", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasRow", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row7", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row7", "http://www.test.com/ontology#tableOrder", "8", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row7", "http://www.test.com/ontology#docID", "8", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row7", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col1", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row7", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col2", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row7", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col3", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row7", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col4", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row7", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col5", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row7", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col6", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row8", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.test.com/ontology#TableRow", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasRow", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row8", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row8", "http://www.test.com/ontology#tableOrder", "9", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row8", "http://www.test.com/ontology#docID", "9", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row8", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col1", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row8", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col2", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row8", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col3", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row8", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col4", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row8", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col5", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row8", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col6", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row9", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.test.com/ontology#TableRow", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#tableHasRow", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row9", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row9", "http://www.test.com/ontology#tableOrder", "10", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row9", "http://www.test.com/ontology#docID", "10", "http://www.w3.org/2001/XMLSchema#integer"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row9", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col1", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row9", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col2", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row9", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col3", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row9", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col4", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row9", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col5", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row9", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col6", "3333", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.test.com/ontology#ExcelWorksheet", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#name", "my", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#docID", "worksheet: my", "http://www.w3.org/2001/XMLSchema#string"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#locatedIn", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee", "http://www.test.com/ontology#contains", "http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", null}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col0", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col1", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col2", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col3", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col4", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col5", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_col6", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row0", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row1", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row2", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row3", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row4", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row5", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row6", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row7", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row8", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee_wmy_row9", "http://www.test.com/ontology#lastUpdateTime", "2011-07-15T06:06:06Z", "http://www.w3.org/2001/XMLSchema#dateTime"}
		  ,new String[]{"http://www.test.com/tester/k98833f89e570c4b7c923e61c211d00eee", "http://www.test.com/ontology#fileNameWithoutExtension", "worksheet", "http://www.w3.org/2001/XMLSchema#string"}	
		   };
	
}
