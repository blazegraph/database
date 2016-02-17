/*

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
 * Created on May 27, 2009
 */

package com.bigdata.counters.render;

import java.io.IOException;
import java.io.Writer;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.query.ICounterSelector;
import com.bigdata.counters.query.URLQueryModel;

/**
 * Renders the selected counters using the counter set XML representation - the
 * client can apply XSLT as desired to style the XML.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class XMLRenderer implements IRenderer {

    /**
     * Describes the state of the controller.
     */
    private final URLQueryModel model;

    /**
     * Selects the counters to be rendered.
     */
    private final ICounterSelector counterSelector;

    /**
     * The selected character set encoding.
     */
    private final String charset;

    /**
     * @param model
     *            Describes the state of the controller (e.g., as parsed from
     *            the URL query parameters).
     * @param counterSelector
     *            Selects the counters to be rendered.
     * @param charset
     *            The selected character set encoding.
     */
    public XMLRenderer(final URLQueryModel model,
            final ICounterSelector counterSelector, final String charset) {

        if (model == null)
            throw new IllegalArgumentException();

        if (counterSelector == null)
            throw new IllegalArgumentException();

        if (charset == null)
            throw new IllegalArgumentException();

        this.model = model;

        this.counterSelector = counterSelector;

        this.charset = charset;
        
    }

    @Override
    public void render(final Writer w) {

        final CounterSet counterSet = new CounterSet();

        @SuppressWarnings("rawtypes")
        final ICounter[] counters = counterSelector.selectCounters(model.depth,
                model.pattern, model.fromTime, model.toTime, model.period,
                false/* historyRequired */);

        /*
         * Apply an optional constraint on the path prefix.
         * 
         * TODO push down into the ICounterSelector. This will be more
         * efficient.
         */
        final String pathPrefix = model.path != null && model.path.length() > 0 ? model.path
                : null;

        for (ICounter<?> counter : counters) {

            if (pathPrefix != null) {

                if (!counter.getPath().startsWith(pathPrefix)) {

                    continue;

                }

            }
            
            counterSet.makePath(counter.getParent().getPath()).attach(counter);

        }

        try {

            counterSet.asXML(w, charset, null/* filter */);

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

}
