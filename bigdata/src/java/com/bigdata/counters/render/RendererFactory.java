/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on May 27, 2009
 */

package com.bigdata.counters.render;

import java.io.IOException;
import java.io.Writer;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.query.ICounterSelector;
import com.bigdata.counters.query.ReportEnum;
import com.bigdata.counters.query.URLQueryModel;
import com.bigdata.util.httpd.NanoHTTPD;

/**
 * Factory for {@link IRenderer} objects based on a MIME type. For known MIME
 * types, the factory returns an instance of an {@link IRenderer} capable of
 * rendering for that MIME type. The instance will then decide whether or not it
 * can render the requested {@link ReportEnum} as not all report types can be
 * rendered for all MIME types. For example, a request for a graph of the event
 * data will fail if the MIME type is <code>text/plain</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RendererFactory {

    /**
     * @param model
     *            Describes the state of the controller (e.g., as parsed from
     *            the URL query parameters).
     * @param counterSelector
     *            Selects the counters to be rendered.
     * @param defaultMimeType
     *            The negotiated or default MIME type.  This can be overridden
     *            using {@value URLQueryModel#MIMETYPE}.
     * 
     * @return An {@link IRenderer} for that MIME type.
     */
    static public IRenderer get(final URLQueryModel model,
            final ICounterSelector counterSelector, final String defaultMimeType) {

        if (model == null)
            throw new IllegalArgumentException();

        if (counterSelector == null)
            throw new IllegalArgumentException();

        if (defaultMimeType == null)
            throw new IllegalArgumentException();

        final String mimeType = (model.mimeType == null ? defaultMimeType
                : model.mimeType);

        if (mimeType.startsWith(NanoHTTPD.MIME_TEXT_PLAIN)) {

            return new TextRenderer(model, counterSelector);

        } else if (mimeType.startsWith(NanoHTTPD.MIME_TEXT_HTML)) {

            return new XHTMLRenderer(model, counterSelector);

        } else if (mimeType.startsWith(NanoHTTPD.MIME_APPLICATION_XML)) {

            /*
             * @todo should be the charset specified with the MIME type and
             * default per the HTTP RFC when none was specified.
             */
            final String charset = "UTF-8";

            return new XMLRenderer(model, counterSelector, charset);

        } else {

            throw new UnsupportedOperationException("mimeType=" + mimeType);

        }
        
    }
    
}
