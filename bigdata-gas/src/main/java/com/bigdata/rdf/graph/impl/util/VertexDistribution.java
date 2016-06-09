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

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;
import org.openrdf.model.Resource;

import com.bigdata.rdf.graph.EdgesEnum;

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

    private static final Logger log = Logger.getLogger(VertexDistribution.class);

    /**
     * A sample.
     */
    private static class VertexSample {
        /** The {@link Resource}. */
        public final Resource v;
        /**
         * The #of times this {@link Resource} occurs as the target of an
         * in-edge.
         */
        public int in;
        /**
         * The #of times this {@link Resource} occurs as the source of an
         * out-edge.
         */
        public int out;

        /**
         * 
         * @param v
         *            The resource.
         * @param in
         *            The #of times this {@link Resource} has been observed as
         *            the target of an in-edge.
         * @param out
         *            The #of times this {@link Resource} has been observed as
         *            the source of an out-edge.
         */
        public VertexSample(final Resource v, final int in, final int out) {
            this.v = v;
            this.in = in;
            this.out = out;
            // this.n = 0;
        }
        
        @Override
        public String toString() {
            return getClass().getName() + "{v=" + v + ",#in=" + in + ",#out="
                    + out + "}";
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

//    /**
//     * Add a sample of a vertex having some attribute value.
//     * 
//     * @param v
//     *            The vertex.
//     */
//    public void addAttributeSample(final Resource v) {
//
//    }
    
    /**
     * Add a sample of a vertex having some out-edge.
     * 
     * @param v
     *            The vertex.
     */
    public void addOutEdgeSample(final Resource v) {

        VertexSample s = samples.get(v);

        if (s == null) {

            // new sample.
            samples.put(v, s = new VertexSample(v, 0/* in */, 1/* out */));

            // indexOf that sample.
            indexOf.put(samples.size() - 1, s);

        }

    }

    /**
     * Add a sample of a vertex having some in-edge.
     * 
     * @param v
     *            The vertex.
     */
    public void addInEdgeSample(final Resource v) {

        VertexSample s = samples.get(v);

        if (s == null) {

            // new sample.
            samples.put(v, s = new VertexSample(v, 1/* in */, 0/* out */));

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
     * Build a vector over the samples. The indices of the sample vector are
     * correlated with the {@link #indexOf} map.
     * 
     * @param edges
     *            Only vertice having the specified type(s) of edges will be
     *            included in the distribution. If {@link EdgesEnum#NoEdges} is
     *            specified, then vertices will not be filtered based on the #of
     *            and type of edges.
     * @param normalize
     *            When <code>true</code> the vector will be normalized such that
     *            the elements in the vector are in <code>[0:1]</code> and sum
     *            to <code>1.0</code>. When <code>false</code> the elements of
     *            the vector are the unnormalized sum of the #of edges of the
     *            specified type(s).
     * 
     * @return The distribution vector over the samples.
     * 
     *         TODO There should also be an edge type filter (filter by link
     *         type).
     */
    double[] getVector(final EdgesEnum edges, final boolean normalize) {

        if (edges == null)
            throw new IllegalArgumentException();

//        if (edges == EdgesEnum.NoEdges)
//            throw new IllegalArgumentException();

        final double[] a = new double[samples.size()];

        if (a.length == 0) {

            // No samples. Avoid division by zero.
            return a;

        }

        int i = 0;

        double sum = 0d;

        for (VertexSample s : samples.values()) {

            final double d;
            switch (edges) {
            case NoEdges:
                d = 1;
                break;
            case InEdges:
                d = s.in;
                break;
            case OutEdges:
                d = s.out;
                break;
            case AllEdges:
                d = (s.in + s.out);
                break;
            default:
                throw new AssertionError();
            }
            
            if (d == 0)
                continue;

            if (normalize) {
                a[i] = sum += d;
            } else {
                a[i] = d;
            }

            i++;

        }
        final int nfound = i;

        if (nfound == 0) {
            // Empty sample.
            return new double[0];
        }
        
        if (normalize) {

            for (i = 0; i < a.length; i++) {

                a[i] /= sum;

            }

        }

        if (nfound < a.length) {

            // Make the array dense.
            
            final double[] b = new double[nfound];

            System.arraycopy(a/* src */, 0/* srcPos */, b/* dest */,
                    0/* destPos */, nfound/* length */);

            return b;
        }
        
        return a;

    }

    /**
     * Return a sample (without duplicates) of vertices from the graph choosen
     * randomly according to their frequency within the underlying distribution.
     * 
     * @param desiredSampleSize
     *            The desired sample size.
     * @param edges
     *            The sample is taken from vertices having the specified type(s)
     *            of edges. Vertices with zero degree for the specified type(s)
     *            of edges will not be present in the returned sampled.
     * 
     * @return The distinct samples that were found.
     */
    public Resource[] getWeightedSample(final int desiredSampleSize,
            final EdgesEnum edges) {

        if (samples.isEmpty()) {

            // There are no samples to choose from.
            return new Resource[] {};

        }

        // Build a normalized vector over the sample.
        final double[] norm = getVector(edges, true/* normalized */);

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
     *            The desired sample size.
     * @param edges
     *            The sample is taken from vertices having the specified type(s)
     *            of edges. Vertices with zero degree for the specified type(s)
     *            of edges will not be present in the returned sampled.
     * 
     * @return The distinct samples that were found.
     */
    public Resource[] getUnweightedSample(final int desiredSampleSize,
            final EdgesEnum edges) {

        if (samples.isEmpty()) {

            // There are no samples to choose from.
            return new Resource[] {};

        }

        // Build a vector over the sample.
        final double[] vec = getVector(edges, true/* normalized */);

        // Maximum number of samples to attempt.
        final int limit = (int) Math.min(desiredSampleSize * 3L,
                Integer.MAX_VALUE);

        int round = 0;

        // The selected samples.
        final Set<Resource> selected = new HashSet<Resource>();

        while (selected.size() < desiredSampleSize && round++ < limit) {

            final int i = r.nextInt(vec.length);

            final Resource v = indexOf.get(Integer.valueOf(i)).v;

            selected.add(v);

        }

        return selected.toArray(new Resource[selected.size()]);
        
//        // The selected samples.
//        final Set<Resource> selected = new HashSet<Resource>();
//
//        final int nsamples = this.samples.size();
//
//        while (selected.size() < desiredSampleSize && round++ < limit) {
//
//            final int i = r.nextInt(nsamples);
//
//            final Resource v = indexOf.get(Integer.valueOf(i)).v;
//
//            selected.add(v);
//
//        }
//
//        return selected.toArray(new Resource[selected.size()]);

    }

    /**
     * Return all (without duplicates) vertices from the graph
     *
     *
     * @return The distinct samples that were found.
     */
    public Resource[] getAll() {

        if (samples.isEmpty()) {

            // There are no samples to choose from.
            return new Resource[] {};

        }

        // The selected samples.
        final Set<Resource> selected = new HashSet<Resource>();

        for(int i=0; i<indexOf.size(); i++) {

            final Resource v = indexOf.get(Integer.valueOf(i)).v;

            selected.add(v);
        }

        return selected.toArray(new Resource[selected.size()]);

    }

    @Override
    public String toString() {
        return super.toString() + "{size=" + size() + "}";
    }
    
}
