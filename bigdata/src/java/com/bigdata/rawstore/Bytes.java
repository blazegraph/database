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
 * Created on Oct 8, 2006
 */

package com.bigdata.rawstore;

/**
 * Constants.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Bytes {

    public static final int SIZEOF_BYTE   = 1;
    public static final int SIZEOF_SHORT  = 2;
    public static final int SIZEOF_INT    = 4;
    public static final int SIZEOF_LONG   = 8;
    public static final int SIZEOF_FLOAT  = 4;
    public static final int SIZEOF_DOUBLE = 8;
    public static final int SIZEOF_UUID   = 16;
    
    /*
     * int32 constants.
     */
    public static final int kilobyte32 = 1024;
    public static final int megabyte32 = 1048576;
    public static final int gigabyte32 = 1073741824; 

    /*
     * int64 constants.
     */
    public static final long kilobyte = 1024;
    public static final long megabyte = 1048576;
    public static final long gigabyte = 1073741824; 
    public static final long terabyte = 1099511627776L;

}
