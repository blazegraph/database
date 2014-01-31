/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Jan 9, 2014
 */
package com.bigdata.rdf.sparql.ast.eval;

/**
 * An instance of this exception is thrown if out of order evaluation of
 * solutions is observed in an evaluation context which does not permit this
 * (the RTO).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class OutOfOrderEvaluationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public OutOfOrderEvaluationException() {
    }

    public OutOfOrderEvaluationException(String message) {
        super(message);
    }

    public OutOfOrderEvaluationException(Throwable cause) {
        super(cause);
    }

    public OutOfOrderEvaluationException(String message, Throwable cause) {
        super(message, cause);
    }

//    public OutOfOrderEvaluationException(String message, Throwable cause,
//            boolean enableSuppression, boolean writableStackTrace) {
//        super(message, cause, enableSuppression, writableStackTrace);
//    }

}
