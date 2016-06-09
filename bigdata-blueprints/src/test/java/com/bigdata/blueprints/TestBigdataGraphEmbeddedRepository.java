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
package com.bigdata.blueprints;

import java.io.File;
import java.util.Properties;

import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.remote.BigdataSailFactory;
import com.bigdata.rdf.sail.remote.BigdataSailFactory.Option;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Class to test BigdataGraphEmbedded creation using a SailRepository for client
 * test suite coverage.
 * 
 * @author beebs
 *
 */
public class TestBigdataGraphEmbeddedRepository extends
		AbstractTestBigdataGraphFactory {

	public void setProperties() {

		Properties props = System.getProperties();

		// no inference
		props.setProperty(BigdataSail.Options.AXIOMS_CLASS,
				NoAxioms.class.getName());
		props.setProperty(BigdataSail.Options.VOCABULARY_CLASS,
				NoVocabulary.class.getName());
		props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
		props.setProperty(BigdataSail.Options.JUSTIFY, "false");

		// no text index
		props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");

		// triples mode
		props.setProperty(BigdataSail.Options.QUADS, "false");
		props.setProperty(BigdataSail.Options.STATEMENT_IDENTIFIERS, "false");

	}

	public BigdataSail getOrCreateRepository(String journalFile) {

		final java.util.Properties props = new java.util.Properties();
		SailRepository repo = null;

		/*
		 * Lax edges allows us to use non-unique edge identifiers
		 */
		props.setProperty(BigdataGraph.Options.LAX_EDGES, "true");

		/*
		 * SPARQL bottom up evaluation semantics can have performance impact.
		 */
		props.setProperty(AbstractTripleStore.Options.BOTTOM_UP_EVALUATION,
				"false");

		if (journalFile == null || !new File(journalFile).exists()) {

			/*
			 * No journal specified or journal does not exist yet at specified
			 * location. Create a new store. (If journal== null an in-memory
			 * store will be created.
			 */
			repo = BigdataSailFactory.createRepository(props, journalFile,
					Option.TextIndex);// , Option.RDR);

		} else {

			/*
			 * Journal already exists at specified location. Open existing
			 * store.
			 */
			repo =  BigdataSailFactory.openRepository(journalFile);

		}

		try {
			repo.initialize();
		} catch (RepositoryException e) {
			e.printStackTrace();
			testPrint(e.toString());
		}

		return (BigdataSail) repo.getSail();
	}

	@Override
	protected BigdataGraph getNewGraph(String file) throws Exception {
		
		return loadGraph(file);
		
	}

	@Override
	protected BigdataGraph loadGraph(String file) throws Exception {

		return new BigdataGraphEmbedded(getOrCreateRepository(file));

	}
}
