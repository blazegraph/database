package com.bigdata.rdf.graph;


import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;

/**
 * Utility class for operations on the public interfaces.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class GASUtil {

//    private static final Logger log = Logger.getLogger(GASUtil.class);

    /**
     * Return the other end of a link.
     * 
     * @param u
     *            One end of the link.
     * @param e
     *            The link.
     * 
     * @return The other end of the link.
     * 
     *         FIXME We can optimize this to use reference testing if we are
     *         careful in the GATHER and SCATTER implementations to always use
     *         the {@link IV} values on the {@link ISPO} object that is exposed
     *         to the {@link IGASProgram}.
     */
    @SuppressWarnings("rawtypes")
    public static IV getOtherVertex(final IV u, final ISPO e) {

        if (e.s().equals(u))
            return e.o();
        
        return e.s();

    }
    
}
