# Generating SPARQL grammar

By default, `mvn compile` just compiles pre-generated java sources.

To update java sources for SPARQL grammar, please follow next steps:

1. Make changes to `src/main/java/com/bigdata/rdf/sail/sparql/ast/sparql.jjt`
2. Run `mvn generate-sources -P syntax`. This step requires javacc 5.0 to be installed at `/opt/javacc-5.0`.
3. Commit changed files to git
