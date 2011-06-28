package com.bigdata.counters.render;

import java.io.IOException;
import java.io.Writer;

/**
 * Interface for rendering some data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRenderer {

    /**
     * Render the data.
     * 
     * @param w
     *            Where to write the data.
     *            
     * @throws IOException
     */
    public void render(Writer w) throws IOException;
    
}
