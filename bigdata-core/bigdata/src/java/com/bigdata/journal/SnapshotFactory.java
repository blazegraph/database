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
package com.bigdata.journal;

import java.io.File;
import java.io.IOException;

/**
 * Provides a basic implementation of a {@link ISnapshotFactory} to be used 
 * with non-HA operations to provide online backup capabilities.
 * 
 * This is exposed via the REST API via the {@link StatusServlet}.
 * 
 * @author beebs
 *
 */
public class SnapshotFactory implements ISnapshotFactory {

	//Default to "backup.jnl" in the directory where the service is running.
	private String file = "backup.jnl";
	
	//Default to uncompressed
	private boolean compress = false;
	
	public SnapshotFactory(final String file, final boolean compress) {
		
		this.file = file;
		this.compress = compress;
		
	}
	
	public SnapshotFactory() {}  //Use defaults

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	public void setCompress(boolean compress) {
		this.compress = compress;
	}

	/**
	 * For the default implementation don't append the commit point to the file.
	 * 
	 *  In non-HA mode, the system does not have the responsibility to manage and
	 *  restore from snapshots.  That is accomplished by operations and procedures
	 *  around the deployment of the non-HA service.
	 * 
	 */
	@Override
	public File getSnapshotFile(IRootBlockView rbv) throws IOException {

		File f = new File(file);
		
		if(!f.canWrite()) {
			throw new RuntimeException("Backup file " + file + " is not writable.");
		}
		
		return f;
	}

	@Override
	public boolean getCompress() {
		return compress;
	}

}
