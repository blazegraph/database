package com.bigdata.counters.render;

import java.util.regex.Pattern;

import com.bigdata.counters.query.HistoryTable;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class HistoryTableRenderer implements IRenderer {

    protected final HistoryTable t;

    protected final Pattern pattern;
    
    protected final ValueFormatter formatter;

    /**
     * 
     * @param tbl
     * @param pattern
     *            Used to identify capturing groups and extract column
     *            labels.
     * @param formatter
     */
    protected HistoryTableRenderer(HistoryTable tbl,
            Pattern pattern, ValueFormatter formatter) {

        this.t = tbl;
        
        this.pattern = pattern;

        this.formatter = formatter;
        
    }
    
}