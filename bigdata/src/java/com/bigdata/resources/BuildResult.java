package com.bigdata.resources;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.mdi.SegmentMetadata;

/**
 * The result of an {@link BuildIndexSegmentTask}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BuildResult extends AbstractResult {

    /**
     * The metadata describing the generated {@link IndexSegment}.
     */
    public final SegmentMetadata segmentMetadata;

    /**
     * 
     * @param name
     *            The name under which the processed index partition was
     *            registered (this is typically different from the name of
     *            the scale-out index).
     * @param indexMetadata
     *            The index metadata object for the processed index as of
     *            the timestamp of the view from which the
     *            {@link IndexSegment} was generated.
     * @param segmentMetadata
     *            The metadata describing the generated {@link IndexSegment}.
     */
    public BuildResult(String name, IndexMetadata indexMetadata,
            SegmentMetadata segmentMetadata) {

        super(name, indexMetadata);
        
        if (segmentMetadata == null) {

            throw new IllegalArgumentException();
            
        }

        this.segmentMetadata = segmentMetadata;

    }

}