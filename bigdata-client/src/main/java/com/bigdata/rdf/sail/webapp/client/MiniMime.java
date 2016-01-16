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
/*
 * Created on Mar 7, 2012
 */

package com.bigdata.rdf.sail.webapp.client;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.bigdata.util.NV;

/**
 * Extract and return the quality score for the mime type (defaults to
 * <code>1.0</code>).
 * 
 * Note: <a
 * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.1">the
 * grammar</a> permits whitespace fairly generally, but the parser in this class
 * does not cope with this correctly, but largely assumes that such whitespace
 * is omitted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Lift out patterns.
 */
public class MiniMime {

	static private final Logger log = Logger.getLogger(MiniMime.class);

	/**
	 * The extracted quality score for the MIME Type (<code>q=...</code>).
	 */
	public final float q;

	private final String mimeType;

	/**
	 * The parsed name=value MIME Type parameters. The tail of the array MAY
	 * have one or more null elements. You can just break out of the loop when
	 * you hit the first <code>null</code> MIME type parameter name.
	 */
	private final List<NV> params = new LinkedList<NV>();

   /**
    * 
    * @param s
    *           The value to be parsed.
    * 
    * @throws NullPointerException
    *            if the argument is <code>null</code>
    */
	public MiniMime(final String s) {
		final String[] b = s.split(";");
		mimeType = b[0].trim();
		float q = 1f;
		// params = new String[b.length][];
		for (int i = 1; i < b.length; i++) {
			final String c = b[i];
			final String[] d = c.split("=");
			if (d.length < 2)
				continue;
			params.add(new NV(d[0].trim(), d[1].trim()));
			// params[i] = d;
			// params[i][0] = d[0];
			// params[i][1] = d[1];
			if (!d[0].equals("q"))
				continue;
			q = Float.valueOf(d[1]);
		}
		if (log.isDebugEnabled())
			log.debug("Considering: " + s + " :: q=" + q);
		this.q = q;
	}

	/**
	 * The MIME type without any MIME parameters.
	 */
	public String getMimeType() {
		return mimeType;
	}

	/**
	 * Return the first value for the named MIME type parameter.
	 * 
	 * @param name
	 *            The parameter name (case sensitive).
	 * @param def
	 *            The default value (optional).
	 * 
	 * @return The value for that parameter and the caller's default value
	 *         otherwise.
	 */
	public String getParam(final String name, final String def) {
		if (name == null)
			throw new IllegalArgumentException();
		for (NV nv : params) {
			if (name.equals(nv.getName())) {
				return nv.getValue();
			}
		}
		return def;
	}

	/**
	 * Return the value of the <code>charset</code>.
	 * <p>
	 * Note: Per Section 3.7.1 Canonicalization and Text Defaults of the HTTP
	 * 1.1 specification:
	 * <ul>
	 * <li>
	 * The "charset" parameter is used with some media types to define the
	 * character set (section 3.4) of the data. When no explicit charset
	 * parameter is provided by the sender, media subtypes of the "text" type
	 * are defined to have a default charset value of "ISO-8859-1" when received
	 * via HTTP. Data in character sets other than "ISO-8859-1" or its subsets
	 * MUST be labeled with an appropriate charset value. See section 3.4.1 for
	 * compatibility problems.</li>
	 * </ul>
	 * 
	 * @return The value of the <code>charset</code> parameter and
	 *         <code>null</code> if that parameter was not given.
	 */
	public String getContentEncoding() {

		// return getParam("charset","ISO-8859-1");
		return getParam("charset", null);

	}

}
