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
package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.journal.BasicSnapshotFactory;
import com.bigdata.journal.ISnapshotResult;
import com.bigdata.journal.Journal;

/**
 * Request an online backup of the journal (non-HA Mode). The backup will be
 * written to the backup file specified as the value of the Request Parameter,
 * i.e.
 * 
 * <code> curl --data-urlencode "file=/path/to/backup.jnl" http://localhost:9999/blazegraph/backup <code>
 * 
 * Will place the backup file in /path/to/backup.jnl. If not backupFile is specified, it will be written to backup.jnl 
 * in the directory where the java process is currently executing.
 * 
 * 
 * The REST API supports the following parameters:
 * file :  The name of the file.  Defaults to backup.jnl in the current working directory.
 * compress :  Boolean to compress the backup.   It defaults to false.  It is true if the parameter is present without a value.  Compress does not append a .gz to the backup file name.
 * block :   Boolean to block the REST call on creating the snapshot.  Defaults to true.  
 * 
 * <code> curl \
 * 			--data-urlencode "file=/path/to/backup.jnl" \
 * 			--data-urlencode "compress=true" \
 * 			--data-urlencode "block=true" \
 * 			http://localhost:9999/blazegraph/backup 
 * <code>
 * 
 */
public class BackupServlet extends BigdataRDFServlet {
	 /**
     * Logger.
     */
    private static final Logger log = Logger.getLogger(BackupServlet.class);
    
    private static final boolean debug = log.isDebugEnabled();
    
	/**
	 * 
	 */
	private static final long serialVersionUID = 5983619974621184746L;
	
	/**
	 * URL parameter to compress the snapshot.  Default value
	 * is false.  If it is present without a value, it is assumed
	 * to be true.  It does not append a .gz to the filename.
	 */
	public static final String COMPRESS = "compress";
	
	/**
	 * URL parameter to specify the backup file.    It is optional.
	 * If it is not specified, it defaults to &quot;backup.jnl&quot;.
	 * 
	 * The file must be writable and must not exist.  If the file is
	 * passed with a relatively filename, it will be converted to an 
	 * absolute path.
	 */
	public static final String FILE = "file";

	public static final String DEFAULT_FILE = "backup.jnl";
	
	/**
	 * URL parameter to specify that the REST call should block until
	 * the backup is completed.   The default value is true.
	 * 
	 * If it is set to false, the backup will not be available until the
	 * {@link SnapshoTask} is completed.
	 */
	public static final String BLOCK = "block";
	
	protected void doPost(final HttpServletRequest req,
			final HttpServletResponse res) throws IOException {
		doGet(req, res);
	}

	protected void doGet(final HttpServletRequest req,
			final HttpServletResponse res) throws IOException {
		processRequest(req, res);
	}
	
	protected void processRequest(HttpServletRequest req, HttpServletResponse res) throws IOException {

		boolean compress = false; // Default value is no compression
		boolean block = true; // Default value is to block on the response
		String file = DEFAULT_FILE;
		
		boolean hasError = false;
		final StringBuffer errorMessage = new StringBuffer();
		
		{
			String param = req.getParameter(COMPRESS);

			if (param != null) {
				if (!"".equals(param)) { // try to parse a boolean value
					compress = Boolean.parseBoolean(param);
				} else {
					compress = true; // Set to true if passed without a value
				}
			}

			param = req.getParameter(BLOCK);

			if (param != null) {
				if (!"".equals(param)) {
					block = Boolean.parseBoolean(param);
				} // Default is true
			}

			param = req.getParameter(FILE);

			if (param != null) {
				if (!"".equals(param)) {
					file = param;
				} // Default is set at initialization
			}

		}

		{
			final BasicSnapshotFactory snapfact = new BasicSnapshotFactory(
					file, compress);

			if (debug) {
				log.debug("Snapshot requested.  Writing backup to "
						+ snapfact.getFile());
			}

			log.warn("Snapshot requested.  Writing backup to "
					+ snapfact.getFile());

			final Future<ISnapshotResult> f = ((Journal) getIndexManager())
					.snapshot(snapfact);

			if (block) {

				try {
					ISnapshotResult r = f.get();
					if (debug) {
						log.debug("Snapshot completed at "
								+ r.getFile().getAbsolutePath());
					}
				} catch (InterruptedException e) {
					hasError = true;
					errorMessage.append(e.toString());
					log.warn(e);
				} catch (ExecutionException e) {
					hasError = true;
					errorMessage.append(e.toString());
					log.warn(e);
				}

			}

		}
		
		if(hasError) {
			buildAndCommitResponse(res, HTTP_INTERNALERROR, MIME_TEXT_PLAIN, errorMessage.toString() + "\n");
		} else {
			buildAndCommitResponse(res, HTTP_OK, MIME_TEXT_PLAIN, "Backup created at " + file + ".\n");
		}

	}

}
