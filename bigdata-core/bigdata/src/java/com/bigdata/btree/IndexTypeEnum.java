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
 * Created on Jun 14, 2012
 */
package com.bigdata.btree;

/**
 * Type safe enumeration of index types.
 */
public enum IndexTypeEnum {

    /** BTree. */
    BTree((short) 0),

    /** Extendible hash tree. */
    HTree((short) 1),

    /** Stream. */
    Stream((short) 2);

	private IndexTypeEnum(final short code) {

		this.code = code;

	}

	private final short code;

	public short getCode() {

		return code;

	}

	static public IndexTypeEnum valueOf(final short code) {
		switch (code) {
		case 0:
			return BTree;
        case 1:
            return HTree;
        case 2:
            return Stream;
		default:
			throw new IllegalArgumentException("code=" + code);
		}
	}

}
