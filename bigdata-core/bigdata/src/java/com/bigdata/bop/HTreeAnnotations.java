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
 * Created on Sep 28, 2010
 */

package com.bigdata.bop;

import com.bigdata.htree.HTree;

/**
 * Annotations for an operator using an {@link HTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ConcurrentHashMapAnnotations.java 3665 2010-09-28 16:53:22Z thompsonbry $
 */
public interface HTreeAnnotations extends IndexAnnotations {

	/**
	 * The number of address bits to use (default {@link #DEFAULT_ADDRESS_BITS}
	 * ). The fan-out of the {@link HTree} will be <code>2^addressBits</code>. A
	 * value of <code>10</code> will have a fan-out of <code>1024</code> and the
	 * resulting page size will be in 4 ~ 8k.
	 * 
	 * @see #DEFAULT_ADDRESS_BITS
	 */
	String ADDRESS_BITS = HTreeAnnotations.class.getName() + ".addressBits";

	int DEFAULT_ADDRESS_BITS = 10;

}
