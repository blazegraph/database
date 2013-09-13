/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph.impl.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.openrdf.model.Resource;

/**
 * Utility class for sampling vertices from a graph.
 * <p>
 * If we build a table <code>[f,v]</code>, where <code>f</code> is a frequency
 * value and <code>v</code> is a vertex identifier, then we can select according
 * to different biases depending on how we normalize the scores for
 * <code>f</code>. For example, if <code>f</code> is ONE (1), and we normalize
 * by <code>sum(f)</code>, then we have a uniform selection over the vertices.
 * On the other hand, if <code>f</code> is the #of out-edges for <code>v</code>
 * and we normalize by <code>sum(f)</code>, then we have a selection that is
 * uniform based on the #of out-edges. Since we need to take multiple samples,
 * the general approach is to build the table <code>[f,v]</code> using some
 * policy, compute <code>sum(f)</code>, and the select from the table by
 * computing the desired random value in <code>[0:1]</code> and scanning the
 * table until we find the corresponding row and report that vertex identifier.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class VertexDistribution {

    /**
     * A sample.
     */
    private static class VertexSample {
        /** The frequence of the {@link Resource}. */
        public double f;
        /** The {@link Resource}. */
        public final Resource v;

        // /** The #of times the {@link Resource} has been selected. */
        // public int n;
        public VertexSample(final double f, final Resource v) {
            this.f = f;
            this.v = v;
            // this.n = 0;
        }
    }

    /**
     * The random number generator.
     */
    private final Random r;
    
    /**
     * Map from {@link Resource} to {@link VertexSample}.
     */
    private final Map<Resource, VertexSample> samples = new HashMap<Resource, VertexSample>();

    /**
     * Map from an arbitrary (one-up) index (origin ZERO) to the sequence in
     * which the {@link VertexSample} was first observed.
     */
    private final Map<Integer, VertexSample> indexOf = new HashMap<Integer, VertexSample>();

    public VertexDistribution(final Random r) {
        
        if(r == null)
            throw new IllegalArgumentException();
        
        this.r = r;
        
    }

    /**
     * Add a sample.
     * 
     * @param v
     *            The vertex.
     */
    public void addSample(final Resource v) {

        VertexSample s = samples.get(v);

        if (s == null) {

            // new sample.
            samples.put(v, s = new VertexSample(1d/* f */, v));

            // indexOf that sample.
            indexOf.put(samples.size() - 1, s);

        }

    }

    /**
     * Return the #of samples in the distribution from which a called specified
     * number of samples may then drawn using a random sampling without
     * replacement technique.
     * 
     * @see #getUnweightedSample(int)
     * @see #getWeightedSample(int)
     */
    public int size() {

        return samples.size();
        
    }
    
    /**
     * Build a normalized vector over the sample frequences. The indices of the
     * sample vector are correlated with the {@link #indexOf} map. The values in
     * the normalized vector are in <code>[0:1]</code> and sum to
     * <code>1.0</code>.
     */
    double[] getNormVector() {

        final double[] norm = new double[samples.size()];

        if (norm.length == 0) {

            // No samples. Avoid division by zero.
            return norm;

        }

        int i = 0;

        double sum = 0d;

        for (VertexSample s : samples.values()) {

            norm[i++] = sum += s.f;

        }

        for (i = 0; i < norm.length; i++) {

            norm[i] /= sum;

        }

        return norm;

    }

    /**
     * Return a sample (without duplicates) of vertices from the graph choosen
     * randomly according to their frequency within the underlying distribution.
     * 
     * @param desiredSampleSize
     *            The desired sample size.
     * 
     * @return The distinct samples that were found.
     */
    public Resource[] getWeightedSample(final int desiredSampleSize) {

        if (samples.isEmpty()) {

            // There are no samples to choose from.
            return new Resource[] {};

        }

        // Build a normalized vector over the sample.
        final double[] norm = getNormVector();

        // Maximum number of samples to attempt.
        final int limit = (int) Math.min(desiredSampleSize * 3L,
                Integer.MAX_VALUE);

        int round = 0;

        // The selected samples.
        final Set<Resource> selected = new HashSet<Resource>();

        while (selected.size() < desiredSampleSize && round++ < limit) {

            final double d = r.nextDouble();

            double sum = 0d;
            int i = 0;
            for (; i < norm.length; i++) {
                final double f = norm[i];
                sum += f;
                if (sum > d) {
                    break;
                }
            }

            final Resource v = indexOf.get(Integer.valueOf(i)).v;

            selected.add(v);

        }

        return selected.toArray(new Resource[selected.size()]);

    }

    /**
     * Return a sample (without duplicates) of vertices from the graph choosen
     * at random without regard to their frequency distribution.
     * 
     * @param desiredSampleSize
     * 
     * @return
     */
    public Resource[] getUnweightedSample(final int desiredSampleSize) {

        if (samples.isEmpty()) {

            // There are no samples to choose from.
            return new Resource[] {};

        }

        // Maximum number of samples to attempt.
        final int limit = (int) Math.min(desiredSampleSize * 3L,
                Integer.MAX_VALUE);

        int round = 0;

        // The selected samples.
        final Set<Resource> selected = new HashSet<Resource>();

        final int nsamples = this.samples.size();

        while (selected.size() < desiredSampleSize && round++ < limit) {

            final int i = r.nextInt(nsamples);

            final Resource v = indexOf.get(Integer.valueOf(i)).v;

            selected.add(v);

        }

        return selected.toArray(new Resource[selected.size()]);

    }

}
