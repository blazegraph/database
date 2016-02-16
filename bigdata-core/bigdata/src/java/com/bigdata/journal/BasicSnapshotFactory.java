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

import org.apache.log4j.Logger;

/**
 * Provides a basic implementation of a {@link ISnapshotFactory} to be used 
 * with non-HA operations to provide online backup capabilities.
 * 
 * This is exposed via the REST API via the {@link StatusServlet}.
 * 
 * @author beebs
 *
 */
public class BasicSnapshotFactory implements ISnapshotFactory {

	 /**
     * Logger.
     */
    private static final Logger log = Logger.getLogger(BasicSnapshotFactory.class);
    
    private static final boolean debug = log.isDebugEnabled();
    private static final boolean info = log.isDebugEnabled();
	
	//Default to "backup.jnl" in the directory where the service is running.
	private String file = "backup.jnl";
	
	//Default to uncompressed
	private boolean compress = false;
	
	public BasicSnapshotFactory(final String file, final boolean compress) {
		
		this.file = file;
		this.compress = compress;
		
	}
	
	public BasicSnapshotFactory() {
		
		//Get the absolute path for the running instance.
		file = getAbsolutePath(file);
		
	}  

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = getAbsolutePath(file);
	}

	public void setCompress(boolean compress) {
		this.compress = compress;
	}
	
	/**
	 * Utility to get the absolute path of a file if passed a relative one.
	 * 
	 * @param file
	 * @return
	 */
	protected String getAbsolutePath(String file) {

		String ret = file;

		File f = new File(file);
		//Check if we have a relative path file and use the absolute path.
		if(f.getParent() == null) {
			ret = f.getAbsolutePath();
		}
		f = null;
		
		return ret;
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
		
		if(!f.createNewFile() ) {
			if(debug) {
				log.debug("Backup file " + file + " already exists.");
			}
			throw new RuntimeException("Backup file " + file + " already exists.");
		}
		
		if(!f.canWrite()) {
			if(debug) {
				log.debug("Backup file " + file + " is not writable.");
			}
			throw new RuntimeException("Backup file " + file + " is not writable.");
		}
		
		return f;
	}

	@Override
	public boolean getCompress() {
		return compress;
	}

}
