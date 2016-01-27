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
package com.bigdata.rdf.sail.webapp;

import java.io.IOException;

/**
 * Well-known exception is used to convey a non-success response from inside of
 * an {@link AbstractRestApiTask} to
 * {@link BigdataRDFServlet#launderThrowable(Throwable, javax.servlet.http.HttpServletResponse, String)}
 * .
 * 
 * @author bryan
 */
public class HttpOperationException extends IOException {

   private static final long serialVersionUID = 1L;
   
   public final int status;
   public final String mimeType;
   public final String content;

   /**
    * 
    * @param status
    *           The HTTP status code for the response.
    * @param mimeType
    *           The MIME type of the response (required).
    * @param content
    *           The content of the response (optional).
    */
   HttpOperationException(final int status, final String mimeType,
         final String content) {
      this.status = status;
      this.mimeType = mimeType;
      this.content = content;
   }

   @Override
   public String toString() {
      return getClass().getName() + "{status=" + status + ", mimeType="
            + mimeType + ", content=" + content + "}";
   }

}
