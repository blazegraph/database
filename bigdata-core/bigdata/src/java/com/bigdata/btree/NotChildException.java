/*

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Dec 8, 2015
 */
package com.bigdata.btree;

/**
 * An instance of this exception is thrown when a node or leaf is not a child
 * (or self) for some other node or leaf.
 * 
 * @author bryan
 */
public class NotChildException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public NotChildException() {
    }

    public NotChildException(String message) {
        super(message);
    }

    public NotChildException(Throwable cause) {
        super(cause);
    }

    public NotChildException(String message, Throwable cause) {
        super(message, cause);
    }

//    public NotChildException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
//        super(message, cause, enableSuppression, writableStackTrace);
//    }

}
