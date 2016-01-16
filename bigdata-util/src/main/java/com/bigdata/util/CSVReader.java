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
/*
 * Created on Aug 13, 2007
 */

package com.bigdata.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.log4j.Logger;

/**
 * A helper class to read CSV (comma separated value) and similar kinds of
 * delimited data. Files may use commas or tabs to delimit columns. If you have
 * to parse other kinds of delimited data then you should override
 * {@link #split(String)}.
 * <p>
 * Note: The default parsing of column values will provide {@link Long} integers
 * and {@link Double} precision floating point values rather than
 * {@link Integer} or {@link Float}. If you want to change this you need to
 * customize the {@link Header} class since that is responsible for interpreting
 * column values.
 * <p>
 * Note: If no headers are defined (by the caller) or read from the file (by the
 * caller), then default headers named by the origin ONE column indices will be
 * used.
 * 
 * @todo replace with <a href="http://flatpack.sourceforge.net/">flatpack</a>?
 *       It uses an Apache 2 license.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CSVReader implements Iterator<Map<String, Object>> {

    private static final Logger log = Logger.getLogger(CSVReader.class);

//    protected static final boolean INFO = log.isInfoEnabled();
    
    /**
     * The #of characters to buffer in the reader.
     */
    protected static final int BUF_SIZE = Bytes.kilobyte32 * 20;
    
    /**
     * A header for a column that examines its values and interprets them as
     * floating point numbers, integers, dates, or times when possible and
     * as uninterpreted character data otherwise.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static class Header {

        private final String name;
        
        public String getName() {
            
            return name;
            
        }

        /**
         * An array of formats to be tested against the column values. The order
         * in the array is the order in which the formats are tested.
         * <P>
         * Note that formats DO NOT require a complete match on the source test.
         * For this reason, they are arranged based on the expected ability for
         * the format to be diagnositic. For example, a date such as 4/6/2002
         * would be interpreted as an integer if you tested the integer format
         * first.
         * 
         * @todo consider using regular expressions to select which formats to
         *       apply (or which formats to ignore).
         * 
         * @todo verify that the entire text was matched by the format in order
         *       to allow the format to be the approved interpretation of the
         *       text.
         */
        protected static final Format[] formats;

        static {

            formats = new Format[] {

                    // handles 2 digits years by adjusting for the current century 
                    new SimpleDateFormat("MM/dd/yy"),
                    
                    // handles explicit years (treats 2 digit years as yyAD).
                    new SimpleDateFormat("MM/dd/yyyy"),
                    
                    DateFormat.getDateInstance(DateFormat.SHORT),
                    DateFormat.getDateInstance(DateFormat.MEDIUM),
                    DateFormat.getDateInstance(DateFormat.LONG),
                    DateFormat.getDateInstance(DateFormat.FULL),

                    DateFormat.getTimeInstance(DateFormat.SHORT),
                    DateFormat.getTimeInstance(DateFormat.MEDIUM),
                    DateFormat.getTimeInstance(DateFormat.LONG),
                    DateFormat.getTimeInstance(DateFormat.FULL),

                    DateFormat.getDateTimeInstance(DateFormat.SHORT,DateFormat.SHORT),
                    DateFormat.getDateTimeInstance(DateFormat.MEDIUM,DateFormat.MEDIUM),
                    DateFormat.getDateTimeInstance(DateFormat.LONG,DateFormat.LONG),
                    DateFormat.getDateTimeInstance(DateFormat.FULL,DateFormat.FULL),

                    NumberFormat.getCurrencyInstance(),

                    NumberFormat.getPercentInstance(),

                    NumberFormat.getNumberInstance(),

                    /*
                     * Note: There are no factory methods for formats that
                     * handle exponents. I've put a few in here, but there may
                     * very well be other examples that will still not be parsed
                     * correctly.
                     */
                    
                    // scientific 
                    new DecimalFormat("0.###E0"),

                    // engineering
                    new DecimalFormat("##0.#####E0"),

                    NumberFormat.getIntegerInstance(),

            };

        }

        public Header(final String name) {

            if (name == null)
                throw new IllegalArgumentException();

            if (name.trim().length() == 0)
                throw new IllegalArgumentException();

            this.name = name;

        }

        /**
         * Attempts to interpret the character data as a date/time,
         * currency, percentage, floating point value, or integer. If the
         * text can not be interpreted as any of those formats then it
         * returns the uninterpreted character data.
         * 
         * @param text
         *            The character data.
         *            
         * @return The parsed value.
         */
        public Object parseValue(final String text) {

                for (int i = 0; i < formats.length; i++) {

                try {

                    final Format f = formats[i];

                    if (f instanceof DateFormat) {

                        return ((DateFormat) f).parse(text);

                    } else if (f instanceof NumberFormat) {

                        return ((NumberFormat) f).parse(text);

                    } else
                        throw new AssertionError();

                } catch (NumberFormatException ex) {
                    
                    // ignore.
                    
                } catch (ParseException ex) {

                    // ignore.

                }

            }

            return text;

        }

        /**
         * Equal if the headers have the same data.
         */
        @Override
        public boolean equals(final Object o) {

            if (this == o)
                return true;

            if (!(o instanceof Header)) {

                return false;

            }

            return name.equals(((Header) o).name);

        }
        
//        public boolean equals(final Header o) {
//            
//            if(this==o) return true;
//            
//            return name.equals(o.name);
//            
//        }
        
        /**
         * Based on the header name.
         */
        @Override
        public int hashCode() {
            
            return name.hashCode();
            
        }

        @Override
        public String toString() {
            
            return name;
            
        }
        
    }

    /**
     * The source.
     */
    protected final BufferedReader r;

    /**
     * The current line. Set to null initially and by {@link #next()}. Set
     * to non-null by {@link #hasNext()} when testing for more lines or by
     * {@link #next()} when attempting to read the next line.
     */
    private String line = null;

    /**
     * The current line # (origin one).
     */
    private int lineNo = 0;

    /**
     * Set to true when {@link BufferedReader#readLine()} returns false
     * indicating that no more data may be read from the source.
     */
    private boolean exhausted = false;

    private boolean skipCommentLines = true;
    
    private boolean skipBlankLines = true;

    private boolean trimWhitespace = true;

    private long tailDelayMillis = 0L;
    
    /**
     * The header definitions (initially null).
     * 
     * @see #readHeaders()
     * @see #setHeaders(String[])
     */
    protected Header[] headers;

    public CSVReader(final InputStream is, final String charSet)
            throws IOException {

        if (is == null)
            throw new IllegalArgumentException();

        if (charSet == null)
            throw new IllegalArgumentException();

        r = new BufferedReader(new InputStreamReader(is, charSet),
                BUF_SIZE);

    }

    public CSVReader(final Reader r) throws IOException {

        if (r == null)
            throw new IllegalArgumentException();

        this.r = new BufferedReader(r, BUF_SIZE);

    }

    /**
     * The current line number (origin one).
     */
    public int lineNo() {

        return lineNo;

    }
    
    public boolean setSkipCommentLines(boolean skipCommentLines) {

        boolean tmp = this.skipCommentLines;

        this.skipCommentLines = skipCommentLines;

        return tmp;

    }

    public boolean getSkipCommentLines() {

        return skipCommentLines;

    }

    public boolean setSkipBlankLines(final boolean skipBlankLines) {

        final boolean tmp = this.skipBlankLines;

        this.skipBlankLines = skipBlankLines;

        return tmp;

    }

    public boolean getSkipBlankLines() {

        return skipBlankLines;

    }

    public boolean setTrimWhitespace(final boolean trimWhitespace) {

        final boolean tmp = this.trimWhitespace;

        this.trimWhitespace = trimWhitespace;

        return tmp;

    }

    public boolean getTrimWhitespace() {

        return trimWhitespace;

    }

    /**
     * The #of milliseconds that the {@link CSVReader} should wait before
     * attempting to read another line from the source (when reading from
     * a pipe) -or- 0L if the {@link CSVReader} should NOT continue reading
     * once it has reached the end of the input (default 0L). 
     */
    public long getTailDelayMillis() {
        
        return tailDelayMillis;
        
    }
    
    public long setTailDelayMillis(final long tailDelayMillis) {

        if (tailDelayMillis < 0)
            throw new IllegalArgumentException();

        long tmp = this.tailDelayMillis;
        
        this.tailDelayMillis = tailDelayMillis;
        
        return tmp;
        
    }
    
    @Override
    public boolean hasNext() {

        if (exhausted)
            return false;
        
        if (line != null) {

            return true;
            
        }

//        final Thread currentThread = Thread.currentThread();
        
        try {

            while (true) {

                if (Thread.interrupted()) {

                    if (log.isInfoEnabled())
                        log.info("Interrupted");
                    
                    exhausted = true;

                    return false;
                    
                }
                
                while (tailDelayMillis != 0L && !r.ready()) {
                    /*
                     * Wait until more data is available.
                     * 
                     * @todo may have to wait until a newline is available, or
                     * just incrementally buffer until we have a full line of
                     * text.
                     */
                    try {
                        Thread.sleep(tailDelayMillis);
                    } catch (InterruptedException e) {
                        // Interrupted - stop processing.
                        log.warn(e.getMessage());
                        return false;
                    }
                    
                }
                
                line = r.readLine();

                if (line == null) {

                    exhausted = true;

                    return false;

                }

                lineNo++;

                if (skipBlankLines && line.trim().length() == 0)
                    continue;
                
                if (skipCommentLines && line.length()>0 && line.charAt(0) == '#')
                    continue;

                return true;

            }

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    @Override
    public Map<String, Object> next() {

        if (!hasNext())
            throw new NoSuchElementException();

        // parse the line.
        Map<String, Object> map = parse(trim(split(line)));

        // the line has been consumed.
        this.line = null;

        // return the parsed data.
        return map;

    }

    /**
     * Split the line into columns based on tabs or commas.
     * 
     * @param line
     *            The line.
     * 
     * @return The columns. There will be one value for each column
     *         identified in the line.
     * 
     * @todo allow quoted values that contain commas.
     */
    protected String[] split(final String line) {

        final String[] cols = line.split("[,\t]");

        return cols;

    }

    /**
     * Trim whitespace and optional quotes from each value iff
     * {@link #getTrimWhitespace()} is true.
     * 
     * @param cols
     *            The column values.
     * 
     * @return The column values.
     */
    protected String[] trim(final String[] cols) {

        if (!trimWhitespace)
            return cols;

        for (int i = 0; i < cols.length; i++) {

            String col = cols[i];

            if (col != null) {

                col = col.trim();

                if (col.startsWith("\"") && col.endsWith("\"")) {

                    col = col.substring(1, col.length() - 1);

                }

                cols[i] = col;

            }

        }

        return cols;

    }

    /**
     * Parse the line into column values. If no headers have been defined then
     * default headers are automatically using {@link #setDefaultHeaders(int)}.
     * 
     * @param line
     *            The line.
     * 
     * @return A map containing the parsed data.
     */
    protected Map<String, Object> parse(final String[] values) {

        final Map<String, Object> map = new TreeMap<String, Object>();

        if (headers == null) {
       
            log.warn("No headers - using defaults.");
            
            setDefaultHeaders(values.length);
            
        }

        if (headers.length < values.length) {

            throw new RuntimeException("Too many values: line=" + lineNo);

        }

        for (int i = 0; i < values.length; i++) {

            final Header h = headers[i];

            final String text = values[i];

            map.put(h.name, h.parseValue(text));

        }

        return map;

    }

    /**
     * Creates default headers named by the origin ONE column indices
     * {1,2,3,4,...}.
     * 
     * @param ncols
     *            The #of columns.
     */
    protected void setDefaultHeaders(final int ncols) {

        final Header[] headers = new Header[ncols];

        for (int i = 0; i < ncols; i++) {

            headers[i] = new Header("" + (i + 1));
    
        }
        
        this.headers = headers;
        
    }
    
    /**
     * Parse a line containing headers.
     * 
     * @param line
     *            The line.
     *            
     * @return The header definitions.
     */
    protected Header[] parseHeaders(final String line) {

        final String[] cols = trim(split(line));

        final Header[] headers = new Header[cols.length];

        for (int i = 0; i < cols.length; i++) {

            headers[i] = new Header(cols[i]);

        }

        return headers;

    }

    /**
     * Interpret the next row as containing headers.
     * 
     * @throws IOException
     */
    public void readHeaders() throws IOException {

        if (!hasNext())
            throw new IOException("No more rows");

        // parse the line to extract the headers.
        headers = parseHeaders(line);

        // the line has been consumed.
        line = null;

    }

    /**
     * Return the current headers (by reference).
     */
    public Header[] getHeaders() {

        return headers.clone();
        
    }
    
    /**
     * Explictly set the headers.
     * 
     * @param headers
     *            The headers.
     */
    public void setHeaders(final Header[] headers) {

        if (headers == null)
            throw new IllegalArgumentException();
        
        this.headers = headers;

    }
    
    /**
     * Re-define the {@link Header} at the specified index.
     * 
     * @param index
     *            The index in [0:#headers-1].
     * @param header
     *            The new {@link Header} definition.
     */
    public void setHeader(final int index, final Header header) {
        
        if (index < 0 || index > headers.length)
            throw new IndexOutOfBoundsException();

        if (header == null)
            throw new IllegalArgumentException();
        
        headers[index] = header;
        
    }
    
    /**
     * Unsupported operation.
     */
    @Override
    public void remove() {

        throw new UnsupportedOperationException();

    }

}
