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
package com.bigdata.rdf.graph.impl.bd;

import java.util.Properties;

import org.openrdf.model.Value;

import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.analytics.FuzzySSSP;
import com.bigdata.rdf.graph.analytics.FuzzySSSP.FuzzySSSPResult;


/**
 *  TODO.  This is a placeholder to remove the bigdata dependency from
 *  the bigdata-gas project.  See BLZG-1272.  Needs unit tests:  BLZG-1369. 
 * 
 * @author beebs
 *
 */

public class TestFuzzySSSP extends AbstractBigdataGraphTestCase {

	public TestFuzzySSSP() {

	}

	public TestFuzzySSSP(String name) {
		super(name);
	}

	public static void main(final String[] args) throws Exception {

		final int nthreads = 4;

		final Properties properties = new Properties();

		final BigdataGraphFixture graphFixture = new BigdataGraphFixture(
				properties);

		final IGASEngine gasEngine = graphFixture.newGASEngine(nthreads);

		try {

			final Value[] src = null;
			final Value[] tgt = null;
			final int N = 0;

			final IGraphAccessor graphAccessor = graphFixture
					.newGraphAccessor(null/* ignored */);

			final FuzzySSSPResult result = new FuzzySSSP(src, tgt, N,
					gasEngine, graphAccessor).call();

			System.out.println(result);

		} finally {

			gasEngine.shutdownNow();

		}
	}
}
