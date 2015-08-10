package com.bigdata.counters.render;

import java.io.IOException;
import java.io.Writer;
import java.text.Format;

import com.bigdata.counters.ICounter;
import com.bigdata.counters.query.URLQueryModel;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TextValueFormatter extends ValueFormatter {

    public TextValueFormatter(final URLQueryModel model) {
        
        super(model);

    }

    /**
     * @todo the logic for choosing the {@link Format} should be shared by
     *       impls but there is also an interaction with rendering links.
     */
    public String value(final ICounter counter, final Object val) {

        if (counter == null)
            throw new IllegalArgumentException();

        if (val == null)
            return "N/A";

        if (val instanceof Double || val instanceof Float) {

            Format fmt = decimalFormat;

            if (counter.getName().contains("%")
                    || percent_pattern.matcher(counter.getName()).matches()) {

                fmt = percentFormat;

            }

            return fmt.format(((Number) val).doubleValue());

        } else if (val instanceof Long || val instanceof Integer) {

            final Format fmt = integerFormat;

            return fmt.format(((Number) val).longValue());

        }

        return val.toString();

    }

    public void writeFullPath(Writer w, String path) throws IOException {

        writePath(w, path, 0/* root */);

    }

    @Override
    public void writePath(Writer w, String path, int rootDepth) throws IOException {

        final String[] a = path.split(XHTMLRenderer.ps);

        // builds up the path query parameter for each split.
        final StringBuilder sb = new StringBuilder(XHTMLRenderer.ps);

        for (int n = 1; n < a.length; n++) {

            final String name = a[n];
            
            if (n > 1) {

                if ((n+1) > rootDepth) {

                    w.write(XHTMLRenderer.ps);
                    
                }

                sb.append(XHTMLRenderer.ps);

            }

            sb.append(name);

            if ((n + 1) > rootDepth) {

                if (rootDepth != 0 && n == rootDepth) {

                    w.write("...");

                    w.write(XHTMLRenderer.ps);

                }

                // current path component.
                w.write(name);

            }

        }
        
    }

}