package com.bigdata.service.mapReduce;

import java.io.File;
import java.io.FileFilter;

import com.bigdata.service.mapReduce.tasks.CountKeywords;
import com.bigdata.service.mapReduce.tasks.ExtractKeywords;

/**
 * Job that extracts and counts keywords from Java files in the current
 * working directory (and subdirectories).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CountKeywordJob extends MapReduceJob {

    public CountKeywordJob(int m, int n) {

        super(m, n, new FileSystemMapSource(new File("."), new FileFilter() {

            public boolean accept(File pathname) {

                if (pathname.isDirectory()) {

                    // i.e., recursive processing of directories.

                    return true;

                }

                String name = pathname.getName();

                return name.endsWith(".java");

            }

        }),

        // map task
                //                    new NopMapTask(),
                //                    new ReadOnlyMapTask(),
                ExtractKeywords.class,

                // reduce task.
                CountKeywords.class,

                DefaultHashFunction.INSTANCE);

    }

}
