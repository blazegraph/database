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

import org.apache.log4j.Logger;

public class StorageTerminalError extends Error {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private final Throwable m_cause;

	private static final transient Logger cat = Logger.getLogger(StorageTerminalError.class);
	
	public StorageTerminalError(final String message, final Throwable cause) {
		super(message);
		
		m_cause = cause;
		
		cat.fatal(message);
	}

	public String getMessage() {
		String msg = super.getMessage();
		
		if (m_cause != null) {
			msg = msg + "[" + m_cause.getMessage() + "]";
		}
		
		return msg;
	}
	public void printStackTrace() {
		if (m_cause != null) {
			m_cause.printStackTrace();
		} else {
			super.printStackTrace();
		}
	}
	
	public void printStackTrace(java.io.PrintStream s) {
		if (m_cause != null) {
			m_cause.printStackTrace(s);
		} else {
			super.printStackTrace(s);
		}
	}
	
	public void printStackTrace(java.io.PrintWriter s) {
		if (m_cause != null) {
			m_cause.printStackTrace(s);
		} else {
			super.printStackTrace(s);
		}
	}
}
