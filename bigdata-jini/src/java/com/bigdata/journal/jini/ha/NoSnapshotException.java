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
package com.bigdata.journal.jini.ha;

import java.io.IOException;

/**
 * An instance of this exception is thrown if the {@link HARestore} class is
 * unable to locate a snapshot file. This can happend you specify the wrong
 * directory. It can also happen if you are using the {@link NoSnapshotPolicy}
 * and never took a snapshot!
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class NoSnapshotException extends IOException {

    private static final long serialVersionUID = 1L;

    public NoSnapshotException() {
        super();
    }

    public NoSnapshotException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoSnapshotException(String message) {
        super(message);
    }

    public NoSnapshotException(Throwable cause) {
        super(cause);
    }
}
