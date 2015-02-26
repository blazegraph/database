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
package com.bigdata.rdf.sail.webapp;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.DestroyKBTask;

class RestApiDestroyKBTask extends AbstractDelegateRestApiTask<Void> {

   public RestApiDestroyKBTask(final HttpServletRequest req,
         final HttpServletResponse resp, final String namespace) {

      super(req, resp, namespace, ITx.UNISOLATED, new DestroyKBTask(namespace,
            ITx.UNISOLATED));

   }

   @Override
   public boolean isReadOnly() {
      return false;
   }

   @Override
   public Void call() throws Exception {

      // Destroy namespace.
      super.call();

      buildResponse(HttpServletResponse.SC_OK,
            BigdataServlet.MIME_TEXT_PLAIN, "DELETED: " + namespace);

      return null;

   }

}