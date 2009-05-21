This module contains an integration with LUBM (the Lehigh University
Benchmark).

This integration is completely optional.  You may use it to run the
LUBM benchmark on an bigdata installation.  The integration contains
special code which permits the LUBM generator to run incrementally,
feeding the generated files to bigdata via a queue and the local file
system.  This makes it possible to run very large scale test without
having to pre-generate a massive synthetic data set.
