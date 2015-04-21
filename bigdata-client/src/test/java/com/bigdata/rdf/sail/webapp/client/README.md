For the most part, the bigdata client is tested implicitly when we test the
NanoSparqlServer web application.  This is unfortunate, but we can't really
change this until we break out the web application into its own maven project
and even then there is a chicken and egg problem with where the test suites
live.