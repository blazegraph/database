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
package com.bigdata.rwstore;

import java.io.File;
import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Journal.Options;
import com.bigdata.journal.RWStrategy;

/**
 * A utility class to explicitly change the metabits storage to allow for
 * compatibility with previous versions.
 * <p>
 * There is an option to use a demispace rather than standard allocations to
 * support stores with large numbers of allocations. If such a store needs to be
 * opened by an earlier code-base, then the store must be amended to store the
 * metabits in a standard allocation.
 * <p>
 * It is only possible to set the metabits demi-space mode to <code>false</code>
 * if the size of the metabits region is less than or equal to the maximum slot
 * size for the declared alloctors.
 * 
 * @author Martyn Cutcher
 * @see <a href="http://trac.blazegraph.com/ticket/936"> Support larger metabit
 *      allocations</a>
 * @see <a href="http://wiki.blazegraph.com/wiki/index.php/DataMigration" > Data
 *      migration </a>
 */
public class MetabitsUtil {

	static String getArg(final String[] args, final String arg, final String def) {
		for (int p = 0; p < args.length; p += 2) {
			if (arg.equals(args[p]))
				return args[p + 1];
		}

		return def;
	}

	static Journal getStore(final String storeFile) {

		final Properties properties = new Properties();

		properties.setProperty(Options.FILE, storeFile);

		properties.setProperty(Options.BUFFER_MODE,
				BufferMode.DiskRW.toString());

		return new Journal(properties);// .getBufferStrategy();

	}

    /**
     * Example usage:
     * 
     * <pre>
     * MatabitsUtil -store "/path/store.jnl" -usedemispace true
     * </pre>
     */
    static public void main(final String[] args) {
		final String store = getArg(args, "-store", null);
		if (store == null) {
			System.err.println("file must be specificed with -store");
			return;
		}
		final File file = new File(store);
		if (!file.exists()) {
			System.err.println("Specified file '" + store + "' not found");
			return;
		}

		final boolean usedemi = "true".equals(getArg(args, "-usedemispace",
				"true"));

		final Journal jnl = getStore(store);

		try {
			final RWStore rws = ((RWStrategy) jnl.getBufferStrategy())
					.getStore();

			if (rws.ensureMetabitsDemispace(usedemi)) { // changed
				jnl.commit();
			}
		} finally {
			jnl.close();
		}
	}

}
