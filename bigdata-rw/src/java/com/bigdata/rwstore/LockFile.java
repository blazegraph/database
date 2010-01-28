/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

import org.apache.log4j.Logger;

import com.bigdata.journal.IJournal;

public class LockFile {
    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(LockFile.class);
    
	protected File m_lockfd = null;
	private Thread m_lockThread = null;
	
	public static LockFile create(String lckname) {
		LockFile lf = new LockFile(lckname);
		
		return (lf.m_lockfd != null) ? lf : null;
	}
	
	public LockFile(String lckname) {
		try {
			System.out.println("** LockFile request **");

			m_lockfd = new File(lckname);
			if (m_lockfd.exists()) {
				System.out.println("** LockFile exists **");
				if (m_lockfd.lastModified() > (System.currentTimeMillis() - (40 * 1000))) {
					System.out.println("** CONFLICT - STILL IN USE **");
					
					m_lockfd = null;
					
					return;
				} else {
					System.out.println("** Deleting current Lock File **");
					m_lockfd.delete();
				}
			}
			
    	File pfile = m_lockfd.getParentFile();
    	if (pfile != null) {
    		pfile.mkdirs();
    	}

			m_lockfd.createNewFile();
			
			m_lockfd.deleteOnExit();
			
			m_lockThread = new Thread() {
				public void run() {
					while (m_lockfd != null) {
						m_lockfd.setLastModified(System.currentTimeMillis());
						
						try {
							sleep(10 * 1000);
						} catch (Throwable e) {
							return;
						}
					}
				}
			};
			m_lockThread.setDaemon(true);
			
			m_lockThread.start();
		} catch (Throwable e) {
			log.error("LockFile Error", e);
			
			m_lockfd = null;
		}
	}
	
	public void clear() {
		if (m_lockfd != null) {
			File fd = m_lockfd;
			
			m_lockfd = null;
			
			fd.delete();
		}
	}
}
