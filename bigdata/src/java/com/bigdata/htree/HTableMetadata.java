/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
/*
 * Created on Dec 1, 2010
 */
package com.bigdata.htree;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

/**
 * Configuration options.
 * 
 * @todo Reconcile with IndexMetadata.
 */
public class HTableMetadata implements Externalizable {

	/**
	 * The unique identifier for the index.
	 */
	private UUID uuid;

	/**
	 * Function used to generate hash values from keys.
	 */
	private HashFunction hashFunction;

	private Object directoryCoder;
	
	private Object bucketCoder;
	
	/**
	 * Function decides whether to split a page, link an overflow page, or
	 * expand the size of a page.
	 */
	// private SplitFunction splitFunction;

	/**
	 * De-serialization constructor.
	 */
	public HTableMetadata() {

	}

	/**
	 * Anonymous hash index.
	 * 
	 * @param uuid
	 *            The unique index identifier.
	 */
	public HTableMetadata(final UUID uuid) {

		this(null/* name */, uuid);

	}

	/**
	 * Named hash index
	 * 
	 * @param name
	 *            The index name.
	 * @param uuid
	 *            The unique index identifier.
	 */
	public HTableMetadata(final String name, final UUID uuid) {

	}

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

}
