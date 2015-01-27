GRAPH_DIR = ../../bigdata-rdf/src/resources/data

GRAPHS = foaf lehigh barData bsbm
#ak2010 belgium_osm delaunay_n13 coAuthorsDBLP delaunay_n21 kron_g500-logn21 soc-LiveJournal1 webbase-1M
#GRAPHS = ak2010 belgium_osm delaunay_n13 coAuthorsDBLP delaunay_n21 soc-LiveJournal1 webbase-1M

ALGORITHMS = BFS SSSP

TEST_FILES = $(foreach P,$(ALGORITHMS),$(foreach G,$(GRAPHS),$G.$P.test))

NSAMPLES=1000
SEED=217
PROFILER=

all: test

test: $(TEST_FILES)

%.BFS.test:  
	ant -Danalytic=BFS -Dnsamples=$(NSAMPLES) -Dseed=$(SEED) -DprofilerAgent=$(PROFILER) '-Dload=-load $(GRAPH_DIR)/$*' '-DjournalFile=bigdata-gas-$*.jnl' run-gas-engine  > $*.BFS.out
	grep "TOTAL: " $*.BFS.out > $@

%.SSSP.test:  
	ant -Danalytic=SSSP -Dnsamples=$(NSAMPLES) -Dseed=$(SEED) -DprofilerAgent=$(PROFILER) '-Dload=-load $(GRAPH_DIR)/$*' '-DjournalFile=bigdata-gas-$*.jnl' run-gas-engine  > $*.SSSP.out
	grep "TOTAL: " $*.SSSP.out > $@

clean:
	rm -f *.test *.out

clean-all: clean
