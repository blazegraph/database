GRAPH_DIR = ../../bigdata-rdf/src/resources/data

#Can restrict this to the graphs you have downloaded. Largest is kron_g500-logn21.
GRAPHS = foaf lehigh barData bsbm
#ak2010 belgium_osm delaunay_n13 coAuthorsDBLP delaunay_n21 kron_g500-logn21 soc-LiveJournal1 webbase-1M
#GRAPHS = ak2010 belgium_osm delaunay_n13 coAuthorsDBLP delaunay_n21 soc-LiveJournal1 webbase-1M

#which algorithms to check
#ALGORITHMS = pagerank sssp bfs cc
ALGORITHMS = BFS SSSP

#Match this to number of cores on your system
#POWERGRAPH_OPTS = --ncpus 4

#GOLD_BINARIES = $(foreach x,$(ALGORITHMS),../PowerGraphReferenceImplementations/$x.x)
#GOLD_FILES = $(foreach P,$(ALGORITHMS),$(foreach G,$(GRAPHS),$G.$P.gold))
TEST_FILES = $(foreach P,$(ALGORITHMS),$(foreach G,$(GRAPHS),$G.$P.test))

#REGRESSIONS = $(foreach P,$(ALGORITHMS),$(foreach G,$(GRAPHS),$G.$P.pass))

NSAMPLES=1000
SEED=217
PROFILER=

#all: regress
all: test

#gold: $(GOLD_BINARIES) $(GOLD_FILES)

test: $(TEST_FILES)

#regress: $(REGRESSIONS)

#define MAKEGOLD
#  rm -f __$(1).$(2).out* ;
#  ../PowerGraphReferenceImplementations/$(2).x --graph $(GRAPH_DIR)/$(1)/$(1).mtx --graph_opts ingress=batch --save __$(1).$(2).out | awk '/Finished Running/{print $$5}' > $(1).$(2).timing ; 
#  cat __$(1).$(2).out* | sort -n > $(1).$(2).gold ;
#  rm -f __$(1).$(2).out* ;
#endef

# TODO Add reference implementation and comparison for CC. See the pagerank example below.
%.BFS.test:  
	ant -Danalytic=BFS -Dnsamples=$(NSAMPLES) -Dseed=$(SEED) -DprofilerAgent=$(PROFILER) '-Dload=-load $(GRAPH_DIR)/$*' '-DjournalFile=bigdata-gas-$*.jnl' run-gas-engine  > $*.BFS.out
	grep "TOTAL: " $*.BFS.out > $@

%.SSSP.test:  
	ant -Danalytic=SSSP -Dnsamples=$(NSAMPLES) -Dseed=$(SEED) -DprofilerAgent=$(PROFILER) '-Dload=-load $(GRAPH_DIR)/$*' '-DjournalFile=bigdata-gas-$*.jnl' run-gas-engine  > $*.SSSP.out
	grep "TOTAL: " $*.SSSP.out > $@

#	sort -n __tmp$* > $@
#	rm -f __tmp$*

#%.bfs.gold: ../PowerGraphReferenceImplementations/bfs.x
#	$(call MAKEGOLD,$*,bfs)

#%.bfs.test: ../simpleBFS
#	../simpleBFS $(GRAPH_DIR)/$*/$*.mtx __tmp$* | awk '/Took/{print $$2}' > $*.bfs.timing_gpu
#	sort -n __tmp$* > $@
#	rm -f __tmp$*

#%.bfs.pass: %.bfs.test %.bfs.gold
#	diff -q $*.bfs.test $*.bfs.gold
#	touch $*.bfs.pass

clean:
	rm -f *.test *.out

clean-all: clean
