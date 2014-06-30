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
package com.bigdata.rwstore;

import java.io.File;
import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.RWStrategy;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Journal.Options;
import com.bigdata.rawstore.IRawStore;

/**
 * A utility class to explicitly change the metabits storage to allow for
 * compatibility with previous versions.
 * <p>
 * There is an option to use a demispace rather than standard allocations to
 * support stores with large numbers of allocations. If such a store needs to be
 * opened by an earlier code-base, then the store must be amended to store the
 * metabits in a standard allocation.
 * 
 * @author Martyn Cutcher
 * 
 */
public class MetabitsUtil {

	static String getArg(final String[] args, final String arg, final String def) {
		for (int p = 0; p < args.length; p += 2) {
			if (arg.equals(args[p]))
				return args[p + 1];
		}

		return def;
	}

	static Journal getStore(String storeFile) {

		final Properties properties = new Properties();

		properties.setProperty(Options.FILE, storeFile);

		properties.setProperty(Options.BUFFER_MODE,
				BufferMode.DiskRW.toString());

		return new Journal(properties);// .getBufferStrategy();

	}

	/**
	 * Example usage:
	 * <p>
	 * MatabitsUtil -store "/path/store.jnl" -usedemispace true
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
