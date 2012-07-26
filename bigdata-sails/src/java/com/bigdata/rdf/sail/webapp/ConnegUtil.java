/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 6, 2012
 */

package com.bigdata.rdf.sail.webapp;


import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.properties.PropertiesFormat;


/**
 * Helper class for content negotiation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ConnegUtil {

    private static Logger log = Logger.getLogger(ConnegUtil.class);

    private static final Pattern pattern;
    static {
        pattern = Pattern.compile("\\s*,\\s*");
    }

    private final ConnegScore<?>[] scores;

    /**
     * 
     * @param acceptStr
     *            The <code>Accept</code> header.
     */
    public ConnegUtil(final String acceptStr) {

        if (acceptStr == null)
            throw new IllegalArgumentException();

        final String[] a = pattern.split(acceptStr);

        final List<ConnegScore<?>> scores = new LinkedList<ConnegScore<?>>();

        {

            for (String s : a) {

                final MiniMime t = new MiniMime(s);

                // RDFFormat
                {

                    final RDFFormat rdfFormat = RDFFormat
                            .forMIMEType(t.getMimeType());

                    if (rdfFormat != null) {

                        scores.add(new ConnegScore<RDFFormat>(t.q, rdfFormat));

                    }

                }
                
                // TupleQueryResultFormat
                {
                
                    final TupleQueryResultFormat tupleFormat = TupleQueryResultFormat
                            .forMIMEType(t.getMimeType());

                    if (tupleFormat != null) {

                        scores.add(new ConnegScore<TupleQueryResultFormat>(t.q,
                                tupleFormat));
                    }
                    
                }

                // BooleanQueryResultFormat
                {
                
                    final BooleanQueryResultFormat booleanFormat = BooleanQueryResultFormat
                            .forMIMEType(t.getMimeType());

                    if (booleanFormat != null) {

                        scores.add(new ConnegScore<BooleanQueryResultFormat>(
                                t.q, booleanFormat));

                    }

                }

                // PropertiesFormat
                {

                    final PropertiesFormat format = PropertiesFormat
                            .forMIMEType(t.getMimeType());

                    if (format != null) {

                        scores.add(new ConnegScore<PropertiesFormat>(t.q,
                                format));

                    }

                }
                
            }

        }

        this.scores = scores.toArray(new ConnegScore[scores.size()]);

        // Order by quality.
        Arrays.sort(this.scores);

    }

    /**
     * Return the best {@link RDFFormat} from the <code>Accept</code> header,
     * where "best" is measured by the <code>q</code> parameter.
     * 
     * @return The best {@link RDFFormat} -or- <code>null</code> if no
     *         {@link RDFFormat} was requested.
     */
    public RDFFormat getRDFFormat() {
        
        return getRDFFormat(null/*fallback*/);
        
    }
    
    /**
     * Return the best {@link RDFFormat} from the <code>Accept</code> header,
     * where "best" is measured by the <code>q</code> parameter.
     * 
     * @param fallback
     *            The caller's default, which is returned if no match was
     *            specified.
     * 
     * @return The best {@link RDFFormat} -or- <i>fallback</i> if no
     *         {@link RDFFormat} was requested.
     */
    public RDFFormat getRDFFormat(final RDFFormat fallback) {

        for (ConnegScore<?> s : scores) {

            if (s.format instanceof RDFFormat) {

                return (RDFFormat) s.format;

            }

        }

        return fallback;

    }
    
    /**
     * Return the best {@link TupleQueryResultFormat} from the
     * <code>Accept</code> header, where "best" is measured by the
     * <code>q</code> parameter.
     * 
     * @return The best {@link TupleQueryResultFormat} -or- <code>null</code> if
     *         no {@link TupleQueryResultFormat} was requested.
     */
    public TupleQueryResultFormat getTupleQueryResultFormat() {
        
        return getTupleQueryResultFormat(null/*fallback*/);
        
    }

    /**
     * Return the best {@link TupleQueryResultFormat} from the
     * <code>Accept</code> header, where "best" is measured by the
     * <code>q</code> parameter.
     * 
     * @param fallback
     *            The caller's default, which is returned if no match was
     *            specified.
     * 
     * @return The best {@link TupleQueryResultFormat} -or- <i>fallback</i> if
     *         no {@link TupleQueryResultFormat} was requested.
     */
    public TupleQueryResultFormat getTupleQueryResultFormat(
            final TupleQueryResultFormat fallback) {

        for (ConnegScore<?> s : scores) {

            if (s.format instanceof TupleQueryResultFormat) {

                return (TupleQueryResultFormat) s.format;

            }

        }

        return fallback;

    }

    /**
     * Return the best {@link BooleanQueryResultFormat} from the
     * <code>Accept</code> header, where "best" is measured by the
     * <code>q</code> parameter.
     * 
     * @return The best {@link BooleanQueryResultFormat} -or- <code>null</code>
     *         if no {@link BooleanQueryResultFormat} was requested.
     */
    public BooleanQueryResultFormat getBooleanQueryResultFormat() {
        
        return getBooleanQueryResultFormat(null/* fallback */);
        
    }

    /**
     * Return the best {@link BooleanQueryResultFormat} from the
     * <code>Accept</code> header, where "best" is measured by the
     * <code>q</code> parameter.
     * 
     * @param fallback
     *            The caller's default, which is returned if no match was
     *            specified.
     *            
     * @return The best {@link BooleanQueryResultFormat} -or- <i>fallback</i> if
     *         no {@link BooleanQueryResultFormat} was requested.
     */
    public BooleanQueryResultFormat getBooleanQueryResultFormat(
            final BooleanQueryResultFormat fallback) {

        for (ConnegScore<?> s : scores) {

            if (s.format instanceof BooleanQueryResultFormat) {

                return (BooleanQueryResultFormat) s.format;

            }

        }

        return fallback;

    }

    /**
     * Return the best {@link PropertiesFormat} from the <code>Accept</code>
     * header, where "best" is measured by the <code>q</code> parameter.
     * 
     * @return The best {@link PropertiesFormat} -or- <code>null</code> if no
     *         {@link PropertiesFormat} was requested.
     */
    public PropertiesFormat getPropertiesFormat() {

        return getPropertiesFormat(null/* fallback */);

    }

    /**
     * Return the best {@link PropertiesFormat} from the <code>Accept</code>
     * header, where "best" is measured by the <code>q</code> parameter.
     * 
     * @param fallback
     *            The caller's default, which is returned if no match was
     *            specified.
     * 
     * @return The best {@link PropertiesFormat} -or- <i>fallback</i> if no
     *         {@link PropertiesFormat} was requested.
     */
    public PropertiesFormat getPropertiesFormat(final PropertiesFormat fallback) {

        for (ConnegScore<?> s : scores) {

            if (s.format instanceof PropertiesFormat) {

                return (PropertiesFormat) s.format;

            }

        }

        return fallback;

    }

    /**
     * Return an ordered list of the {@link ConnegScore}s for MIME Types which
     * are consistent with the desired format type.
     * 
     * @param cls
     *            The format type.
     * 
     * @return The ordered list.
     */
    @SuppressWarnings("unchecked")
    public <E> ConnegScore<E>[] getScores(final Class<E> cls) {

        if (cls == null)
            throw new IllegalArgumentException();

        final List<ConnegScore<E>> t = new LinkedList<ConnegScore<E>>();

        for (ConnegScore<?> s : scores) {

            if (cls == s.format.getClass()) {

                t.add((ConnegScore<E>) s);

            }

        }

        return t.toArray(new ConnegScore[t.size()]);

    }

    public String toString() {

        return getClass().getSimpleName() + "{" + Arrays.toString(scores) + "}";

    }

}
