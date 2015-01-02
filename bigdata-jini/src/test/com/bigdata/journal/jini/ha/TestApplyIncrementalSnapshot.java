/**

Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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

package com.bigdata.journal.jini.ha;

import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.journal.RWStrategy;
import com.bigdata.rwstore.RWStore;

/**
 * This test was written to help debug a peroblem with the snapshot mechanism.
 * 
 * The initial phase is simply to determine a problem snapshot and then to analyse
 * at which transaction did the problem occur.
 * 
 * It turns out, that the problem was not with a transaction but with the snapshot mechanism
 * itself.
 * 
 * @author Martyn Cutcher
 *
 */
public class TestApplyIncrementalSnapshot extends TestCase2 {

	public static void main(final String[] args) {
		final TestApplyIncrementalSnapshot test = new TestApplyIncrementalSnapshot();
		
		test.process();
	}
	
	public void process() {
		final String path = "/Volumes/NonSSD/bigdata/autodesk/snapshots/snapshot/000/000/000/000/000/";
		final String j1 = path + "032/000000000000000032080.jnl"; // cVersion
		final String j2 = path + "045/000000000000000045523.jnl"; // good
		final String j3 = path + "070/000000000000000070159.jnl"; // cVersion
		final String j4 = path + "075/000000000000000075705.jnl"; // 2
																	// allocators

		// First see if we can open each journal
		try {
			final Journal jnl = openJournal(j1);
			try {
				final RWStrategy bs = (RWStrategy) jnl.getBufferStrategy();
				final RWStore rws = bs.getStore();
				System.out.println("GOOD: " + j4 + " using demispace: " + rws.isUsingDemiSpace());
				final StringBuilder sb = new StringBuilder();
				rws.getStorageStats().showStats(sb);
				System.out.println(sb.toString());
			} finally {
				if (jnl != null)
					jnl.close();
			}
		} catch (Throwable t) {
			System.out.println("ERROR: " + t.getMessage());
		}
		try {
			final Journal jnl = openJournal(j2);
			try {
				final RWStrategy bs = (RWStrategy) jnl.getBufferStrategy();
				final RWStore rws = bs.getStore();
				System.out.println("GOOD: " + j4 + " using demispace: " + rws.isUsingDemiSpace());
				final StringBuilder sb = new StringBuilder();
				rws.getStorageStats().showStats(sb);
				System.out.println(sb.toString());
			} finally {
				if (jnl != null)
					jnl.close();
			}
		} catch (Throwable t) {
			System.out.println("ERROR: " + t.getMessage());
		}

		try {
			final Journal jnl = openJournal(j3);
			try {
				final RWStrategy bs = (RWStrategy) jnl.getBufferStrategy();
				final RWStore rws = bs.getStore();
				System.out.println("GOOD: " + j4 + " using demispace: " + rws.isUsingDemiSpace());
				final StringBuilder sb = new StringBuilder();
				rws.getStorageStats().showStats(sb);
				System.out.println(sb.toString());
			} finally {
				if (jnl != null)
					jnl.close();
			}
		} catch (Throwable t) {
			System.out.println("ERROR: " + t.getMessage());
		}
		try {
			final Journal jnl = openJournal(j4);
			try {
				final RWStrategy bs = (RWStrategy) jnl.getBufferStrategy();
				final RWStore rws = bs.getStore();
				System.out.println("GOOD: " + j4 + " using demispace: " + rws.isUsingDemiSpace());
				final StringBuilder sb = new StringBuilder();
				rws.getStorageStats().showStats(sb);
				System.out.println(sb.toString());
			} finally {
				if (jnl != null)
					jnl.close();
			}
		} catch (Throwable t) {
			System.out.println("ERROR: " + t.getMessage());
		}
	}
	
	Journal openJournal(final String file) {
		System.out.println("Opening: " + file);

		final Properties props = new Properties();
		props.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());
		props.setProperty(Options.FILE, file);
		
		return new Journal(props);
	}

}
