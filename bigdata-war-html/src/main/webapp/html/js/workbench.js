$(function() {

'use strict';
/* jshint validthis: true */

/* Global variables */

// LBS/non-LBS URL prefixes
var RW_URL_PREFIX, RO_URL_PREFIX;

// query/update editors
var EDITORS = {}, ERROR_LINE_MARKERS = {}, ERROR_CHARACTER_MARKERS = {};
var CODEMIRROR_DEFAULTS = {
   lineNumbers: true,
   matchBrackets: true,
   mode: 'sparql',
};
// key is value of RDF type selector, value is name of CodeMirror mode
var RDF_MODES = {
   'n-triples': 'ntriples',
   'n-triples-RDR': 'ntriples-RDR',
   'rdf/xml': 'xml',
   'json': 'json',
   'turtle': 'turtle',
   'turtle-RDR': 'turtle'
};
var FILE_CONTENTS = null;
// file/update editor type handling
// .xml is used for both RDF and TriX, assume it's RDF
// We could check the parent element to see which it is
var RDF_TYPES = {
   'nq': 'n-quads',
   'nt': 'n-triples',
   'ntx': 'n-triples-RDR',
   'n3': 'n3',
   'rdf': 'rdf/xml',
   'rdfs': 'rdf/xml',
   'owl': 'rdf/xml',
   'xml': 'rdf/xml',
   'json': 'json',
   'trig': 'trig',
   'trix': 'trix',
   //'xml': 'trix',
   'ttl': 'turtle',
   'ttlx': 'turtle-RDR'
};
var RDF_CONTENT_TYPES = {
   'n-quads': 'text/x-nquads',
   'n-triples': 'text/plain',
   'n-triples-RDR': 'application/x-n-triples-RDR',
   'n3': 'text/rdf+n3',
   'rdf/xml': 'application/rdf+xml',
   'json': 'application/sparql-results+json',
   'trig': 'application/x-trig',
   'trix': 'application/trix',
   'turtle': 'application/x-turtle',
   'turtle-RDR': 'application/x-turtle-RDR'
};
var SPARQL_UPDATE_COMMANDS = [
   'INSERT',
   'DELETE',
   'LOAD',
   'CLEAR'
];

// pagination
var QUERY_RESULTS, PAGE_SIZE = 50, TOTAL_PAGES, CURRENT_PAGE;

// namespaces
var DEFAULT_NAMESPACE, NAMESPACE;

// namespace creation
var NAMESPACE_PARAMS = {
   'name': 'com.bigdata.rdf.sail.namespace',
   'textIndex': 'com.bigdata.rdf.store.AbstractTripleStore.textIndex',
   'truthMaintenance': 'com.bigdata.rdf.sail.truthMaintenance',
   'quads': 'com.bigdata.rdf.store.AbstractTripleStore.quads',
   'rdr': 'com.bigdata.rdf.store.AbstractTripleStore.statementIdentifiers',
   'axioms': 'com.bigdata.rdf.store.AbstractTripleStore.axiomsClass',
   'justify': 'com.bigdata.rdf.store.AbstractTripleStore.justify',
   'isolatableIndices': 'com.bigdata.rdf.sail.isolatableIndices',
   'geoSpatial': 'com.bigdata.rdf.store.AbstractTripleStore.geoSpatial'
};

var NAMESPACE_SHORTCUTS = {
   'Bigdata': {
      'bd': 'http://www.bigdata.com/rdf#',
      'bds': 'http://www.bigdata.com/rdf/search#',
      'gas': 'http://www.bigdata.com/rdf/gas#',
      'hint': 'http://www.bigdata.com/queryHints#'
   },
   'W3C': {
      'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
      'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
      'owl': 'http://www.w3.org/2002/07/owl#',
      'skos': 'http://www.w3.org/2004/02/skos/core#',
      'xsd': 'http://www.w3.org/2001/XMLSchema#'
   },
   'Dublin Core': {
      'dc': 'http://purl.org/dc/elements/1.1/',
      'dcterm': 'http://purl.org/dc/terms/',
      'void': 'http://rdfs.org/ns/void#'
   },
   'Social/Other': {
      'foaf': 'http://xmlns.com/foaf/0.1/',
      'schema': 'http://schema.org/',
      'sioc': 'http://rdfs.org/sioc/ns#'
   },
   'Custom': {}
};

// data export
var EXPORT_EXTENSIONS = {
   'application/rdf+xml': ['RDF/XML', 'rdf', true],
   'text/plain': ['N-Triples', 'nt', true],
   'application/x-n-triples-RDR': ['N-Triples-RDR', 'ntx', true],
   'application/x-turtle': ['Turtle', 'ttl', true],
   'application/x-turtle-RDR': ['Turtle-RDR', 'ttlx', true],
   'text/rdf+n3': ['N3', 'n3', true],
   'application/trix': ['TriX', 'trix', true],
   'application/x-trig': ['TRIG', 'trig', true],
   'text/x-nquads': ['NQUADS', 'nq', true],

   'text/csv': ['CSV', 'csv', false, exportCSV],
   'application/sparql-results+json': ['JSON', 'json', false, exportJSON],
   // 'text/tab-separated-values': ['TSV', 'tsv', false, exportTSV],
   'application/sparql-results+xml': ['XML', 'xml', false, exportXML]
};


/* Load balancing */

function useLBS(state) {
   // allows passing in of boolean, or firing on checkbox change
   if(typeof state != 'boolean') {
      state = this.checked;
   }
   if(state) {
      RW_URL_PREFIX = 'LBS/leader/';
      RO_URL_PREFIX = 'LBS/read/';
   } else {
      RW_URL_PREFIX = '';
      RO_URL_PREFIX = '';
   }
   $('.use-lbs').prop('checked', state);
}


/* Modal functions */

function showModal(id) {
   $('#' + id).show();
   $('body').addClass('modal-open');
}

function closeModal() {
   $('body').removeClass('modal-open');
   $(this).parents('.modal').hide();
}


/* Search */

function submitSearch(e) {
   e.preventDefault();
   var term = $(this).find('input').val();
   if(!term) {
      return;
   }
   var query = 'select ?s ?p ?o { ?o bds:search "' + term + '" . ?s ?p ?o . }';
   EDITORS.query.setValue(query);
   $('#query-errors').hide();
   $('#query-form').submit();
   showTab('query');
}


/* Tab selection */

function clickTab() {
   showTab($(this).data('target'));
}

function showTab(tab, nohash) {
   $('.tab').hide();
   $('#' + tab + '-tab').show();
   $('#tab-selector a').removeClass();
   $('a[data-target=' + tab + ']').addClass('active');
   if(!nohash && window.location.hash.substring(1).indexOf(tab) !== 0) {
      window.location.hash = tab;
   }
   if(EDITORS[tab]) {
      EDITORS[tab].refresh();
   }
}

function moveTabLeft() {
   moveTab(false);
}

function moveTabRight() {
   moveTab(true);
}

function moveTab(next) {
   // get current position
   var current = $('#tab-selector .active');
   if(next) {
      if(current.next('a').length) {
         current.next().click();
      } else {
         $('#tab-selector a:first').click();
      }
   } else {
      if(current.prev().length) {
         current.prev().click();
      } else {
         $('#tab-selector a:last').click();
      }
   }
}


/* Namespaces */

function getNamespaces(synchronous) {
   // synchronous mode is for startup only, and is false by default
   var settings = {
      async: !synchronous,
      url: RO_URL_PREFIX + 'namespace?describe-each-named-graph=false',
      success: function(data) {
         $('#namespaces-list').empty();
         var rdf = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
         var namespaces = data.getElementsByTagNameNS(rdf, 'Description');
         var i, title, titleText, use;
     if( namespaces.length > 0)
     {
            for(i=0; i<namespaces.length; i++) {
               title = namespaces[i].getElementsByTagName('title')[0].textContent;
               titleText = title == DEFAULT_NAMESPACE ? title + ' (default)' : title;
               if(title == NAMESPACE) {
                  use = 'In use';
               } else {
                  use = '<a href="#" class="use-namespace">Use</a>';
               }
               
               $('#namespaces-list').append('<tr data-name="' + title + '">><td>' + titleText + '</td><td>' + use + '</td><td><a href="#" class="delete-namespace">Delete</a></td><td><a href="#" class="namespace-properties">Properties</a></td><td><a href="#" class="namespace-rebuild-text-index">Rebuild Full Text Index</a></td><td><a href="#" class="clone-namespace">Clone</a></td><td><a href="' + RO_URL_PREFIX + 'namespace/' + title + '/sparql" class="namespace-service-description">Service Description</a></td></tr>');
               
               initMapgraphNamespaceMgmtExtensions(title);

            }
            
           $('.use-namespace').click(function(e) {
              e.preventDefault();
              useNamespace($(this).parents('tr').data('name'));
           });
           $('.delete-namespace').click(function(e) {
              e.preventDefault();
              deleteNamespace($(this).parents('tr').data('name'));
           });
           $('.namespace-properties').click(function(e) {
              e.preventDefault();
              getNamespaceProperties($(this).parents('tr').data('name'));
           });
           $('.namespace-properties-java').click(function(e) {
              e.preventDefault();
              getNamespaceProperties($(this).parents('tr').data('name'), 'java');
           });
           $('.namespace-rebuild-text-index').click(function(e) {
               e.preventDefault();
               rebuildTextIndex($(this).parents('tr').data('name'), true);
            });
           $('.clone-namespace').click(function(e) {
              e.preventDefault();
              cloneNamespace($(this).parents('tr').data('name'));
              $('#namespace-create-errors').html('');
           });
           $('.namespace-service-description').click(function() {
              return confirm('This can be an expensive operation. Proceed anyway?');
           });
     }
      }
   };
   $.ajax(settings);
}

function useNamespace(name, leaveLast) {
   if(!namespaceExists(name) || name === NAMESPACE) {
      return;
   }
   $('#current-namespace').html(name);
   NAMESPACE = name;
   getNamespaces();
   // this is for loading the last explored URI, which might otherwise
   // overwrite the last used namespace
   if(!leaveLast) {
      localStorage.lastNamespace = name;
   }
}

function deleteNamespace(namespace) {
   // prevent default namespace from being deleted
   if(namespace == DEFAULT_NAMESPACE) {
      alert('You may not delete the default namespace.');
      return;
   }

   if(confirm('Are you sure you want to delete the namespace ' + namespace + '?')) {
      if(namespace == NAMESPACE) {
         useNamespace(DEFAULT_NAMESPACE);
      }
      var url = RW_URL_PREFIX + 'namespace/' + namespace;
      var settings = {
         type: 'DELETE',
         success: getNamespaces,
         error: function() { alert('Could not delete namespace ' + namespace); }
      };
      $.ajax(url, settings);
   }
}

function getNamespaceProperties(namespace, download) {
   var url = RO_URL_PREFIX + 'namespace/' + namespace + '/properties';
      if(!download) {
         $('#namespace-properties h1').text(namespace);
         $('#namespace-properties table').empty();
         $('#namespace-properties').show();
         $('#namespace-rebuild-text-index').hide();
      }
   $.get(url, function(data) {
      var java = '';
      $.each(data.getElementsByTagName('entry'), function(i, entry) {
         if(download) {
            java += entry.getAttribute('key') + '=' + entry.textContent + '\n';
         } else {
            $('#namespace-properties table').append('<tr><td>' + entry.getAttribute('key') + '</td><td>' + entry.textContent + '</td></tr>');
         }
      });
      if(download) {
         downloadFile(java, 'text/x-java-properties', this.url.split('/')[3] + '.properties');
      }
   });
}

function rebuildTextIndex(namespace) {
    var url = RO_URL_PREFIX + 'namespace/' + namespace + '/textIndex';

    $('#namespace-rebuild-text-index h1').text(namespace);
    $('#namespace-rebuild-text-index p').empty();
    $('#namespace-properties table').empty();
    $('#namespace-properties').hide();
    $('#namespace-rebuild-text-index').show();

    var settings = {
              type: 'POST',
              success: function(data) { 
                            $('#namespace-rebuild-text-index h1').text(namespace); 
                            $('#namespace-rebuild-text-index p').html(data); 
                            },
              error: function(jqXHR, textStatus, errorThrown) {

                            if (errorThrown.indexOf('not enabled')!=-1 && confirm('Full text index does not exist. Do you want to create it?')) {

                                url += '?force-index-create=true';
                                var settings = {
                                          type: 'POST',
                                          success: function(data) { 
                                                        $('#namespace-rebuild-text-index h1').text(namespace); 
                                                        $('#namespace-rebuild-text-index p').html(data); 
                                                        },
                                          error: function(jqXHR, textStatus, errorThrown) { alert(jqXHR.responseText); }
                                          };
                                $.ajax(url, settings);
                            }

                        }
           };
    $.ajax(url, settings);
}

function dropEntailments(namespace) {

	if(confirm('Are you sure you want to drop all the entailments from the namespace ' + namespace + '?')) {
	
		$('#namespace-drop-entailments h1').text(namespace);
	    $('#namespace-drop-entailments p').empty();
	    $('#namespace-drop-entailments p').html("Operation 'Drop entailments' is running, you could check its status on <a href=\"#status\">status page</a>"); 
	    $('#namespace-properties table').empty();
	    $('#namespace-properties').hide();
	    $('#namespace-rebuild-text-index').hide();
	    $('#namespace-create-entailments').hide();
	    $('#namespace-drop-entailments').show();
	
	    var url = RO_URL_PREFIX + 'namespace/' + namespace + '/sparql';
	
	    var settings = {
	              type: 'POST',
	              data: {'update' : 'DROP ENTAILMENTS'},
	              error: function(jqXHR, textStatus, errorThrown) { 
	              				$('#namespace-drop-entailments p').empty();
	           					$('#namespace-drop-entailments').hide(); 
	              				alert(jqXHR.responseText);              				
	              				},
	              success: function(data) { 
	                            $('#namespace-drop-entailments h1').text(namespace); 
	                            $('#namespace-drop-entailments p').html(data); 
	                            }
	              
	           };
    } else {
           $('#namespace-drop-entailments p').empty();
           $('#namespace-drop-entailments').hide();
    }
    $.ajax(url, settings);
}

function createEntailments(namespace) {

    var url = RO_URL_PREFIX + 'namespace/' + namespace + '/sparql';
    
    $('#namespace-create-entailments h1').text(namespace);
    $('#namespace-create-entailments p').empty();
    $('#namespace-create-entailments p').html("Operation 'Create entailments' is running, you could check its status on <a href=\"#status\">status page</a>"); 
    $('#namespace-properties table').empty();
    $('#namespace-properties').hide();
    $('#namespace-rebuild-text-index').hide();
    $('#namespace-drop-entailments').hide();
    $('#namespace-create-entailments').show();

    var settings = {
              type: 'POST',
              data: {'update' : 'CREATE ENTAILMENTS'},
              error: function(jqXHR, textStatus, errorThrown) { 
	              				$('#namespace-create-entailments p').empty();
	           					$('#namespace-create-entailments').hide(); 
	              				alert(jqXHR.responseText);              				
	              				},
              success: function(data) { 
                            $('#namespace-create-entailments h1').text(namespace); 
                            $('#namespace-create-entailments p').html(data); 
                            }
            };
    $.ajax(url, settings);
}

function getPreparedProperties(elem) {

          var url = RO_URL_PREFIX + 'namespace/prepareProperties';
          var params = {};
          
          params.name = $('#new-namespace-name').val().trim();
          params.textIndex = $('#new-namespace-textIndex').is(':checked');
          params.isolatableIndices = $('#new-namespace-isolatableIndices').is(':checked');
          params.geoSpatial = $('#new-namespace-geoSpatial').is(':checked');
          
          var mode = $('#new-namespace-mode').val();
          if(mode == 'triples') {
             params.quads = false;
             params.rdr = false;
          } else if(mode == 'rdr') {
             params.quads = false;
             params.rdr = true;
          } else { // quads
             params.quads = true;
             params.rdr = false;
          }
          
          if($('#new-namespace-inference').is(':checked')) {
             // Enable inference related options.
             params.axioms = 'com.bigdata.rdf.axioms.OwlAxioms';
             params.truthMaintenance = true;
             params.justify = true;
          } else {
             // Disable inference related options.
             params.axioms = 'com.bigdata.rdf.axioms.NoAxioms';
             params.truthMaintenance = false;
             params.justify = false;
          }

          var data = elem.html();//'<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">\n<properties>\n';
          for(var key in NAMESPACE_PARAMS) {
//             data += '<entry key="' + NAMESPACE_PARAMS[key] + '">' + params[key] + '</entry>\n';
              data += NAMESPACE_PARAMS[key] + '=' + params[key] + '\n';
          }
//          data += '</properties>';
          
       var settings = {
                 type: 'POST',
                 data: data,
                 contentType: 'text/plain',
//                 contentType: 'application/xml',
                 async: false,
                 success: function(data) { 
                         elem.html('');
                      $.each(data.getElementsByTagName('entry'), function(i, entry) {
                              elem.append(entry.getAttribute('key') + '=' + entry.textContent + '\n');
                          });
                    },
                 error: function(jqXHR, textStatus, errorThrown) {
                          elem.html('');
                          alert(jqXHR.responseText);
                      }
                   };
      $.ajax(url, settings);
}

function cloneNamespace(namespace) {
   var url = RO_URL_PREFIX + 'namespace/' + namespace + '/properties';
   $.get(url, function(data) {
      // collect params from namespace to be cloned
      var params = {};
      var elem = $('#properties-text-area');
      elem.html('');
      $.each(data.getElementsByTagName('entry'), function(i, entry) {
         params[entry.getAttribute('key')] = entry.textContent.trim();
         elem.append(entry.getAttribute('key') + '=' + entry.textContent + '\n');
      });

      // set up new namespace form with collected params
      var mode, quads, rdr;
      quads = params[NAMESPACE_PARAMS.quads] == 'true';
      rdr = params[NAMESPACE_PARAMS.rdr] == 'true';
      if(!quads && !rdr) {
         mode = 'triples';
      } else if(!quads && rdr) {
         mode = 'rdr';
      } else if(quads && !rdr) {
         mode = 'quads';
      } else {
         alert('Incompatible set of namespace parameters');
         return;
      }
      $('#new-namespace-mode').val(mode);
      $('#new-namespace-inference').prop('checked', params[NAMESPACE_PARAMS.axioms] == 'com.bigdata.rdf.axioms.OwlAxioms');
      $('#new-namespace-textIndex').prop('checked', params[NAMESPACE_PARAMS.textIndex] == 'true');
      $('#new-namespace-isolatableIndices').prop('checked', params[NAMESPACE_PARAMS.isolatableIndices] == 'true');
      $('#new-namespace-geoSpatial').prop('checked', params[NAMESPACE_PARAMS.geoSpatial] == 'true');      
      changeNamespaceMode();

      $('#new-namespace-name').focus();
   });
}

function namespaceExists(name) {
   return $('#namespaces-list tr[data-name=' + name + ']').length !== 0;
}

function publishNamespace(namespace) {
    
    var url = RO_URL_PREFIX + 'namespace/' + namespace + '/sparql';

    var settings = {
       method: 'POST',
       data: { 'mapgraph' : 'publish' },
       success: function() { location.reload(); },
       error: function(jqXHR, textStatus, errorThrown) { alert(jqXHR.responseText); }
    };
    
    $.ajax(url, settings);
}

function dropNamespace(namespace) {

    var url = RO_URL_PREFIX + 'namespace/' + namespace + '/sparql';

    var settings = {
       method: 'POST',
       data: { 'mapgraph' : 'drop' },
       success: function() { location.reload(); },
       error: function(jqXHR, textStatus, errorThrown) { alert(jqXHR.responseText); }
    };
    
    $.ajax(url, settings);

}

/**
 * Optionally initialized the magraph extension, if mapgraph enabled.
 * Has no effect if mapgraph not active.
 */
function initMapgraphNamespaceMgmtExtensions(namespace) {
    
    var url = RO_URL_PREFIX + 'namespace/' + namespace + '/sparql';
    
    var settings = {
       method: 'POST',
       data: { 'mapgraph' : 'checkPublished' },
       success: function(xml){
           var ret = $(xml).find("data").attr('result') == "true";
           
           if (ret) {
               $('#namespaces-list > tbody > tr[data-name=' + namespace + ']').append('<td><a href="#" class="drop-namespace">Drop from GPU</a></td>'); 
           } else {
               $('#namespaces-list > tbody > tr[data-name=' + namespace + ']').append('<td><a href="#" class="publish-namespace">Publish to GPU</a></td>'); 
           }
           
           // register event listeners
           $('.publish-namespace').click(function(e) {
               e.preventDefault();
               publishNamespace($(this).parents('tr').data('name'));
            }); 
           $('.drop-namespace').click(function(e) {
               e.preventDefault();
               dropNamespace($(this).parents('tr').data('name'));
            });
           
       },
       error: function(jqXHR, textStatus, errorThrown) {  
           // nothing to do: mapgraph not initialized
       }
    };
    
    $.ajax(url, settings);

}




function validateNamespaceOptions() {
   var errors = [], i;
   var name = $('#new-namespace-name').val().trim();
   if(!name) {
      errors.push('Enter a name');
   }
   if(namespaceExists(name)) {
      errors.push('Name already in use');
   }
   if($('#new-namespace-mode').val() == 'quads' && $('#new-namespace-inference').is(':checked')) {
      errors.push('Inference is incompatible with quads mode');
   }
   if($('#new-namespace-isolatableIndices').is(':checked') && $('#new-namespace-inference').is(':checked')) {
      errors.push('Inference is incompatible with isolatable indices');
   }
   $('#namespace-create-errors').html('');
   for(i=0; i<errors.length; i++) {
      $('#namespace-create-errors').append('<li>' + errors[i] + '</li>');
   }
   return errors.length === 0;
}

// Invoked when changing the new namespace mode (triples, rdr, quads).
// Invoked when (un)checking the isolatableIndices option.
// Responsible for (en|dis)abling the Inference option and hiding or showing the message explaining why inference is not permitted.
function changeNamespaceMode() {
   var quads = $('#new-namespace-mode').val() == 'quads'; // Figure out the current mode (quads, triples, rdr).
   var isolatableIndices = $('#new-namespace-isolatableIndices').is(':checked'); // Are isolatable indices requested?
   var geoSpatial = $('new-namespace-geoSpatial').is(':checked'); // Is geoSpatial turned on?
   var inferenceDisabled = (quads || isolatableIndices) ? true : false; // true iff inference is not supported.
//alert("quads="+quads+", isolatableIndices="+isolatableIndices+", inferenceDisallowed="+inferenceDisallowed);
   $('#new-namespace-inference').prop('disabled', inferenceDisabled); // conditionally disable inference checkbox.
   $('#inference-incompatible').toggle(inferenceDisabled); // hide/show inference incompatible message.
   if(inferenceDisabled) {
      $('#new-namespace-inference').prop('checked', false); // uncheck inference checkbox if not allowed.
   }
}

// Create a new namespace.
/*function createNamespace(e) {
   e.preventDefault();
   if(!validateNamespaceOptions()) {
      return;
   }
   // get new namespace name and config options
   var params = {};
   
   params.name = $('#new-namespace-name').val().trim();
   params.textIndex = $('#new-namespace-textIndex').is(':checked');
   params.isolatableIndices = $('#new-namespace-isolatableIndices').is(':checked');
   
   var mode = $('#new-namespace-mode').val();
   if(mode == 'triples') {
      params.quads = false;
      params.rdr = false;
   } else if(mode == 'rdr') {
      params.quads = false;
      params.rdr = true;
   } else { // quads
      params.quads = true;
      params.rdr = false;
   }
   
   if($('#new-namespace-inference').is(':checked')) {
      // Enable inference related options.
      params.axioms = 'com.bigdata.rdf.axioms.OwlAxioms';
      params.truthMaintenance = true;
      params.justify = true;
   } else {
      // Disable inference related options.
      params.axioms = 'com.bigdata.rdf.axioms.NoAxioms';
      params.truthMaintenance = false;
      params.justify = false;
   }

   var data = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">\n<properties>\n';
   for(var key in NAMESPACE_PARAMS) {
      data += '<entry key="' + NAMESPACE_PARAMS[key] + '">' + params[key] + '</entry>\n';
   }
   data += '</properties>';

   var settings = {
      type: 'POST',
      data: data,
      contentType: 'application/xml',
      success: function() { $('#new-namespace-name').val(''); getNamespaces(); },
      error: function(jqXHR, textStatus, errorThrown) { alert(jqXHR.responseText); }
   };
   $.ajax(RW_URL_PREFIX + 'namespace', settings);
}*/

//Create a new namespace.
function createNamespace(elem) {
var data = elem.val();

var settings = {
   type: 'POST',
   data: data,
   contentType: 'text/plain',
   success: function() { $('#new-namespace-name').val(''); getNamespaces(); },
   error: function(jqXHR, textStatus, errorThrown) { alert(jqXHR.responseText); }
};
$.ajax(RW_URL_PREFIX + 'namespace', settings);
}

function getDefaultNamespace() {
   var settings = {
      async: false,
      url: RO_URL_PREFIX + 'namespace?describe-each-named-graph=false',
      success: function(data) {
        // Chrome does not work with rdf\:Description, so look for Description too
        var defaultDataset = $(data).find('rdf\\:Description, Description');
        var ELEMENT = defaultDataset.find('title');
    
        // select the default namespace if there's only a single namespace avlb.
        if (ELEMENT.length == 1) 
        {
            DEFAULT_NAMESPACE = ELEMENT[0].textContent;
        }
      }
   };
   $.ajax(settings);
}


/* Namespace shortcuts */

function selectNamespace() {
   var uri = this.value;
   var tab = $(this).parents('.tab').attr('id').split('-')[0];
   var current = EDITORS[tab].getValue();

   if(current.indexOf(uri) == -1) {
      var ns = $(this).find(':selected').text();
      EDITORS[tab].setValue('prefix ' + ns + ': <' + uri + '>\n' + current);
   }

   // reselect group label
   this.selectedIndex = 0;
}

function showCustomNamespacesModal() {
   showModal('custom-namespace-modal');
}

function createNamespaceShortcut() {
   var ns = $('#custom-namespace-namespace').val().trim();
   var uri = $('#custom-namespace-uri').val().trim();

   // check namespace & URI are not empty, and namespace does not already exist
   if(ns === '' || uri === '') {
      return;
   }
   for(var category in NAMESPACE_SHORTCUTS) {
      for(var oldNS in NAMESPACE_SHORTCUTS[category]) {
         if(ns === oldNS) {
            $('#custom-namespace-modal p').html(ns + ' is already in use for the URI ' + NAMESPACE_SHORTCUTS[category][ns]);
            return;
         }
      }
   }

   // add namespace & URI, and clear form & error message
   NAMESPACE_SHORTCUTS.Custom[ns] = uri;
   saveCustomNamespaces();
   $('#custom-namespace-modal p').html('');
   $(this).siblings('input').val('');
   populateNamespaceShortcuts();
   $(this).siblings('.modal-cancel').click();
}

function populateNamespaceShortcuts() {
   // add namespaces to dropdowns, and add namespaces to modal to allow deletion
   $('.namespace-shortcuts').html('Namespace shortcuts: ');
   $('#custom-namespace-modal li').remove();
   for(var category in NAMESPACE_SHORTCUTS) {
      var select = $('<select><option>' + category + '</option></select>').appendTo($('.namespace-shortcuts'));
      for(var ns in NAMESPACE_SHORTCUTS[category]) {
         select.append('<option value="' + NAMESPACE_SHORTCUTS[category][ns] + '">' + ns + '</option>');
         if(category === 'Custom') {
            // add custom namespaces to list for editing
            $('#custom-namespace-modal ul').append('<li>' + ns + ' (' + NAMESPACE_SHORTCUTS[category][ns] + ') <a href="#" data-ns="' + ns + '"">Delete</a>');
         }
      }
   }
   var edit = $('<button>Edit</button>').appendTo($('.namespace-shortcuts'));
}

function deleteCustomNamespace(e) {
   e.preventDefault();
   if(confirm('Delete this namespace shortcut?')) {
      delete NAMESPACE_SHORTCUTS.Custom[$(this).data('ns')];
      populateNamespaceShortcuts();
      saveCustomNamespaces();
   }
}

function saveCustomNamespaces() {
   localStorage.customNamespaces = JSON.stringify(NAMESPACE_SHORTCUTS.Custom);
}

function loadCustomNamespaces() {
   if(typeof localStorage.customNamespaces !== 'undefined') {
      NAMESPACE_SHORTCUTS.Custom = JSON.parse(localStorage.customNamespaces);
   }
}

/* Update */

function handleDragOver(cm, e) {
   e.stopPropagation();
   e.preventDefault();
   e.dataTransfer.dropEffect = 'copy';
}

function handleDrop(cm, e) {
   e.stopPropagation();
   e.preventDefault();
   var files = e.dataTransfer.files;
   handleFile(files);
}

function handleFileInput(e) {
   e.stopPropagation();
   e.preventDefault();
   var files = e.originalEvent.target.files;
   handleFile(files);
   $('#update-file').val('');
}

function handleFile(files) {
   // only one file supported
   if(files.length > 1) {
      alert('Ignoring all but first file');
   }
   
   var f = files[0];
   
   // if file is too large, tell user to supply local path
   if(f.size > 1048576 * 100) {
      alert('File too large, enter local path to file');
      EDITORS.update.setValue('/path/to/' + f.name);
      setType('path');
      EDITORS.update.setOption('readOnly', false);
      $('#large-file-message, #clear-file').hide();
   } else {
      var fr = new FileReader();
      fr.onload = function(e) {
         if(f.size > 10240) {
            // do not use textarea
            EDITORS.update.setOption('readOnly', true);
            $('#filename').html(f.name);
            $('#large-file-message, #clear-file').show();
            EDITORS.update.setValue('');
            FILE_CONTENTS = e.target.result;
         } else {
            // display file contents in the textarea
            clearFile();
            EDITORS.update.setValue(e.target.result);
         }
         guessType(f.name.split('.').pop().toLowerCase(), e.target.result);
      };
      fr.readAsText(f);
   }
}

function clearFile(e) {
   if(e) {
      e.preventDefault();
   }
   $('#update-box').prop('disabled', false);
   $('#large-file-message, #clear-file').hide();
   FILE_CONTENTS = null;
}

function guessType(extension, content) {
   // try to guess type
   if(extension == 'rq') {
      // SPARQL
      setType('sparql');
   } else if(extension in RDF_TYPES) {
      // RDF
      setType('rdf', RDF_TYPES[extension]);
   } else {
      // extension is no help, see if we can find some SPARQL commands
      setType(identify(content));
   }
}

function identify(text, considerPath) {
   if(considerPath) {
      // match Unix, Windows or HTTP paths
      // file:// is optional for local paths
      // when file:// is not present, Windows paths may use \ or / and must include a :
      // when file:// is present, Windows paths must use / and may include a :
      // http[s]:// is mandatory for HTTP paths
      var unix = /^(file:\/\/)?((\/[^\/]+)+)$/;
      var windows = /^(((file:\/\/)([A-Za-z]:?([\/][^\/\\]+)+))|([A-Za-z]:([\\\/][^\\\/]+)+))$/;
      var http = /^https?:\/((\/[^\/]+)+)$/;
      if(unix.test(text.trim()) || windows.test(text.trim()) || http.test(text.trim())) {
         return 'path';
      }
   }
   
   text = text.toUpperCase();
   for(var i=0; i<SPARQL_UPDATE_COMMANDS.length; i++) {
      if(text.indexOf(SPARQL_UPDATE_COMMANDS[i]) != -1) {
         return 'sparql';
      }
   }

   return 'rdf';
}

function handlePaste(e) {   
   // if the input is currently empty, try to identify the pasted content
   var that = this;
   if(this.value === '') {
      setTimeout(function() { setType(identify(that.value, true)); }, 10);
   } 
}

function setType(type, format) {
   $('#update-type').val(type);
   if(type == 'rdf') {
      $('#rdf-type').val(format);
   }
   setUpdateSettings(type);
}

function setUpdateSettings(type) {
   $('#rdf-type, label[for="rdf-type"]').attr('disabled', type != 'rdf');
   $('#update-tab .advanced-features input').attr('disabled', type != 'sparql');
   setUpdateMode(type);
}

function setUpdateMode(type) {
   var mode = '';
   if(type == 'sparql') {
      mode = 'sparql';
   } else if(type == 'rdf') {
      type = $('#rdf-type').val();
      if(type in RDF_MODES) {
         mode = RDF_MODES[type];
      }
   }
   EDITORS.update.setOption('mode', mode);
}

function createUpdateEditor() {
   EDITORS.update = CodeMirror.fromTextArea($('#update-box')[0], copyObject(CODEMIRROR_DEFAULTS));
   EDITORS.update.on('change', function() { 
      if(ERROR_LINE_MARKERS.update) {
         ERROR_LINE_MARKERS.update.clear();
         ERROR_CHARACTER_MARKERS.update.clear();
      }
   });
   EDITORS.update.on('dragover', handleDragOver);
   EDITORS.update.on('drop', handleDrop);
   EDITORS.update.on('paste', handlePaste);
   EDITORS.update.addKeyMap({'Ctrl-Enter': submitUpdate});
}

function submitUpdate(e) {
   // Updates are submitted as a regular form for SPARQL updates in monitor mode, and via AJAX for non-monitor SPARQL, RDF & file path updates.
   // When submitted as a regular form, the output is sent to an iframe. This is to allow monitor mode to work.
   // jQuery only gives us data when the request is complete, so we wouldn't see monitor results as they come in.

   try {
      e.preventDefault();
   } catch(ex) {}

   $('#update-response').show();

   var url = RW_URL_PREFIX + 'namespace/' + NAMESPACE + '/sparql';
   var settings = {
      type: 'POST',
      data: FILE_CONTENTS === null ? EDITORS.update.getValue() : FILE_CONTENTS,
      success: updateResponseXML,
      error: updateResponseError
   };

   // determine action based on type
   switch($('#update-type').val()) {
      case 'sparql':
         // see if monitor mode is on
         if($('#update-monitor').is(':checked')) {
            // create form and submit it, sending output to the iframe
            var form = $('<form id="update-monitor-form" method="POST" target="update-response-container">')
               .attr('action', url)
               .append($('<input name="update">').val(settings.data))
               .append('<input name="monitor" value="true">');
            if($('#update-analytic').is(':checked')) {
               form.append('<input name="analytic" value="true">');
            }
            form.appendTo($('body'));
            form.submit();
            $('#update-monitor-form').remove();
            $('#update-response iframe, #update-clear-container').show();
            $('#update-response span').hide();
            return;
         }
         settings.data = 'update=' + encodeURIComponent(settings.data);
         if($('#update-analytic').is(':checked')) {
            settings.data += '&analytic=true';
         }
         settings.success = updateResponseHTML;
         break;
      case 'rdf':
         var type = $('#rdf-type').val();
         if(!type) {
            alert('Please select an RDF content type.');
            return;
         }
         settings.contentType = RDF_CONTENT_TYPES[type];
         break;
      case 'path':
         // if no scheme is specified, assume a local path
         if(!/^(file|(https?)):\/\//.test(settings.data)) {
            settings.data = 'file://' + settings.data;
         }
         settings.data = 'uri=' + encodeURIComponent(settings.data);
         break;
   }

   $('#update-response span').show().html('Running update...');   

   $.ajax(url, settings);
}

function clearUpdateOutput() {
   $('#update-response, #update-clear-container').hide();
   $('#update-response span').text('');
   $('#update-response iframe').attr('src', 'about:blank');
}

// also for query panel
function toggleAdvancedFeatures(e) {
   e.preventDefault();
   $(this).next('.advanced-features').toggle();
}

function updateResponseHTML(data) {
   $('#update-response, #update-clear-container').show();
   $('#update-response iframe').attr('src', 'about:blank').hide();
   $('#update-response span').html(data);
}

function updateResponseXML(data) {
   var modified = data.childNodes[0].attributes.modified.value;
   var milliseconds = data.childNodes[0].attributes.milliseconds.value;
   $('#update-response, #update-clear-container').show();
   $('#update-response iframe').attr('src', 'about:blank').hide();
   $('#update-response span').text('Modified: ' + modified + '\nMilliseconds: ' + milliseconds);
}

function updateResponseError(jqXHR, textStatus, errorThrown) {
   $('#update-response, #update-clear-container').show();
   $('#update-response iframe').attr('src', 'about:blank').hide();

   var message = 'ERROR: ';
   if(jqXHR.status === 0) {
      message += 'Could not contact server';
   } else {
      var response = $('<div>').append(jqXHR.responseText);
      if(response.find('pre').length === 0) {
         message += response.text();
      } else {
         message += response.find('pre').text();
      }
      highlightError(jqXHR.responseText, 'update');
   }

   $('#update-response span').text(message);
}


/* Query */

// details needs explain, so turn details off if explain is unchecked,
// and turn explain on if details is checked
function handleExplain() {
   if(!this.checked) {
      $('#query-details').prop('checked', false);
   }
}

function handleDetails() {
   if(this.checked) {
      $('#query-explain').prop('checked', true);
   }
}

function createQueryEditor() {
   EDITORS.query = CodeMirror.fromTextArea($('#query-box')[0], copyObject(CODEMIRROR_DEFAULTS));
   EDITORS.query.on('change', function() { 
      if(ERROR_LINE_MARKERS.query) {
         ERROR_LINE_MARKERS.query.clear();
         ERROR_CHARACTER_MARKERS.query.clear();
      }
   });
   EDITORS.query.addKeyMap({'Ctrl-Enter': submitQuery});
}

function loadHistory(e) {
   e.preventDefault();
   EDITORS.query.setValue(this.innerText);
   EDITORS.query.focus();
}

function addQueryHistoryRow(time, query, results, executionTime) {
   var row = $('<tr>').prependTo($('#query-history tbody'));
   row.append('<td class="query-time">' + time + '</td>');
   var cell = $('<td class="query">').appendTo(row);
   var a = $('<a href="#">').appendTo(cell);
   a.text(query);
   a.html(a.html().replace(/\n/g, '<br>'));
   row.append('<td class="query-results">' + results + '</td>');
   row.append('<td class="query-execution-time">' + executionTime + '</td>');
   row.append('<td class="query-delete"><a href="#">X</a></td>');
}

function storeQueryHistory() {
   // clear existing store
   for(var i=0; i<localStorage.historyCount; i++) {
      localStorage.removeItem('history.time.' + i);
      localStorage.removeItem('history.query.' + i);
      localStorage.removeItem('history.results.' + i);
      localStorage.removeItem('history.executionTime.' + i);
   }

   // output each current row
   $('#query-history tbody tr').each(function(i, el) {
      localStorage['history.time.' + i] = $(el).find('.query-time').html();
      localStorage['history.query.' + i] = $(el).find('.query a')[0].innerText;
      localStorage['history.results.' + i] = $(el).find('.query-results').html();
      localStorage['history.executionTime.' + i] = $(el).find('.query-execution-time').html();
   });

   localStorage.historyCount = $('#query-history tbody tr').length;
}

function deleteHistoryRow(e) {
   e.preventDefault();
   $(this).parents('tr').remove();
   if($('#query-history tbody tr').length === 0) {
      $('#query-history').hide();
   }
   storeQueryHistory();
}

function clearHistory(e) {
   $('#query-history tbody tr').remove();
   $('#query-history').hide();
   storeQueryHistory();
}

function submitQuery(e) {
   try {
      e.preventDefault();
   } catch(ex) {}

   // transfer CodeMirror content to textarea
   EDITORS.query.save();

   // do nothing if query is empty
   var query = $('#query-box').val();
   if(query.trim() === '') {
      return;
   }

   var queryExists = false;

   // see if this query is already in the history
   $('#query-history tbody tr').each(function(i, row) {
      if($(row).find('.query')[0].innerText == query) {
         // clear the old results and set the time to now
         $(row).find('.query-time').text(new Date().toISOString());
         $(row).find('.query-results').text('');
         $(row).find('.query-execution-time').html('<a href="#">Running...</a>');
         // move it to the top
         $(row).prependTo('#query-history tbody');
         queryExists = true;
         return false;
      }
   });

   if(!queryExists) {
      addQueryHistoryRow(new Date().toISOString(), query, '', '<a href="#">Running...</a>', true);
   }

   storeQueryHistory();

   $('#query-history').show();

   var url = RO_URL_PREFIX + 'namespace/' + NAMESPACE + '/sparql';
   var settings = {
      type: 'POST',
      data: $('#query-form').serialize(),
      headers: { 'Accept': 'application/sparql-results+json' },
      success: showQueryResults,
      error: queryResultsError
   };

   if(!$('#query-explain').is(':checked')) {
     $('#query-response').show().html('Query running...');
     $('#query-pagination').hide();

     $.ajax(url, settings);
   } else {
     $('#query-response').show().html('Query results skipped with Explain mode.');
     $('#query-pagination').hide();
   }

   $('#query-explanation').empty();
   $('#query-explanation-download').empty();
   if($('#query-explain').is(':checked')) {
      settings = {
         type: 'POST',
         data: $('#query-form').serialize() + '&explain=' + ($('#query-details').is(':checked') ? 'details' : 'true'),
         dataType: 'html',
         success: showQueryExplanation,
         error: queryResultsError
      };
      $('#query-explanation').show().html('Query running in Explain mode.');;
      $.ajax(url, settings);
   } else {
      $('#query-explanation').hide();
      $('#query-explanation-download').hide();
   }
}

function clearQueryResponse() {
   $('#query-response, #query-explanation').empty('');
   $('#query-response, #query-explanation-download').empty('');
   $('#query-response, #query-pagination, #query-explanation, #query-explanation-download, #query-export-container').hide();
}

function showQueryExportModal() {
   updateExportFileExtension();
   showModal('query-export-modal');
}

function createExportOptions() {
   for(var contentType in EXPORT_EXTENSIONS) {
      var optgroup = EXPORT_EXTENSIONS[contentType][2] ? '#rdf-formats' : '#non-rdf-formats';
      $(optgroup).append('<option value="' + contentType + '">' + EXPORT_EXTENSIONS[contentType][0] + '</option>');
   }

   $('#export-format option:first').prop('selected', true);

   $('#export-format').change(updateExportFileExtension);
}

function updateExportFileExtension() {
   $('#export-filename-extension').html(EXPORT_EXTENSIONS[$('#export-format').val()][1]);
}

function queryExport() {
   var dataType = $('#export-format').val();
   var filename = $('#export-filename').val();
   if(filename === '') {
      filename = 'export';
   }
   filename += '.' + EXPORT_EXTENSIONS[dataType][1];
   if(EXPORT_EXTENSIONS[dataType][2]) {
      // RDF
      var settings = {
         type: 'POST',
         data: JSON.stringify(QUERY_RESULTS),
         contentType: 'application/sparql-results+json',
         headers: { 'Accept': dataType },
         success: function(data) { downloadFile(data, dataType, filename); },
         error: downloadRDFError
      };
      $.ajax(RO_URL_PREFIX + 'sparql?workbench&convert', settings);
   } else {
      // not RDF
      EXPORT_EXTENSIONS[dataType][3](filename);
   }
   $(this).siblings('.modal-cancel').click();
}

function downloadRDFError(jqXHR, textStatus, errorThrown) {
   alert(jqXHR.statusText);
}   

function exportXML(filename) {
   var xml = '<?xml version="1.0"?>\n<sparql xmlns="http://www.w3.org/2005/sparql-results#">\n\t<head>\n';
   var bindings = [];
   $('#query-response thead tr th').each(function(i, td) {
      xml += '\t\t<variable name="' + td.textContent + '"/>\n';
      bindings.push(td.textContent);
   });
   xml += '\t</head>\n\t<results>\n';
   $('#query-response tbody tr').each(function(i, tr) {
      xml += '\t\t<result>\n';
      $(tr).find('td').each(function(j, td) {
         var bindingType = td.className;
         if(bindingType == 'unbound') {
            return;
         }
         var dataType = $(td).data('datatype');
         if(dataType) {
            dataType = ' datatype="' + dataType + '"';
         } else {
            dataType = '';
         }
         var lang = $(td).data('lang');
         if(lang) {
            lang = ' xml:lang="' + lang + '"';
         } else {
            lang = '';
         }
         xml += '\t\t\t<binding name="' + bindings[j] + '"><' + bindingType + dataType + lang + '>' + td.textContent + '</' + bindingType + '></binding>\n';
      });
      xml += '\t\t</result>\n';
   });
   xml += '\t</results>\n</sparql>\n';
   downloadFile(xml, 'application/sparql-results+xml', filename);
}

function exportJSON(filename) {
   var json = JSON.stringify(QUERY_RESULTS);
   downloadFile(json, 'application/sparql-results+json', filename);
}

function exportCSV(filename) {
   var csv = '';
   $('#query-response table tr').each(function(i, tr) {
      $(tr).find('td').each(function(j, td) {
         if(j > 0) {
            csv += ',';
         }
         var val = td.textContent;
         // quote value if it contains " , \n or \r
         // replace " with ""
         if(val.match(/[",\n\r]/)) {
            val = '"' + val.replace('"', '""') + '"';
         }
         csv += val;
      });
      csv += '\n';
   });
   downloadFile(csv, 'text/csv', filename);
}

function downloadFile(data, type, filename) {
   var dataStr;
   if (data instanceof XMLDocument) {
         dataStr = new XMLSerializer().serializeToString(data);
   } else {
         dataStr = data;
   }
   var uri = 'data:' + type + ';charset=utf-8,' + encodeURIComponent(dataStr);
   $('<a id="download-link" download="' + filename + '" href="' + uri + '">').appendTo('body')[0].click();
   $('#download-link').remove();
}

function updateResultCountAndExecutionTime(count) {
   $('#query-history tbody tr:first td.query-results').text(count);

   var ms = Date.now() - Date.parse($('#query-history tbody tr:first td.query-time').html());
   var sec = Math.floor(ms / 1000);
   ms = ms % 1000;
   var min = Math.floor(sec / 60);
   sec = sec % 60;
   var hr = Math.floor(min / 60);
   min = min % 60;
   var executionTime = '';
   if(hr > 0) {
      executionTime += hr + 'hr, ';
   }
   if(min > 0) {
      executionTime += min + 'min, ';
   }
   if(sec > 0) {
      executionTime += sec + 'sec, ';
   }
   executionTime += ms + 'ms';
   $('#query-history tbody tr:first td.query-execution-time').html(executionTime);

   storeQueryHistory();
}

function showQueryResults(data) {
   $('#query-response').empty();
   $('#query-export-rdf').hide();
   $('#query-response, #query-pagination, #query-export-container').show();
   var table = $('<table>').appendTo($('#query-response'));
   var i, tr;
   if(this.dataTypes[1] == 'xml') {
      // RDF
      table.append($('<thead><tr><td>s</td><td>p</td><td>o</td></tr></thead>'));
      var rows = $(data).find('Description');
      for(i=0; i<rows.length; i++) {
         // FIXME: are about and nodeID the only possible attributes here?
         var s = rows[i].attributes['rdf:about'];
         if(typeof s == 'undefined') {
            s = rows[i].attributes['rdf:nodeID'];
         }
         s = s.textContent;
         for(var j=0; j<rows[i].children.length; j++) {
            var p = rows[i].children[j].tagName;
            var o = rows[i].children[j].attributes['rdf:resource'];
            // FIXME: is this the correct behaviour?
            if(typeof o == 'undefined') {
               o = rows[i].children[j].textContent;
            } else {
               o = o.textContent;
            }
            tr = $('<tr><td>' + (j === 0 ? s : '') + '</td><td>' + p + '</td><td>' + o + '</td>');
            table.append(tr);
         }
      }
      updateResultCountAndExecutionTime(rows.length);
   } else {
      // JSON
      // save data for export and pagination
      QUERY_RESULTS = data;

      if(typeof data.boolean != 'undefined') {
         // ASK query
         table.append('<tr><td>' + data.boolean + '</td></tr>').addClass('boolean');
         updateResultCountAndExecutionTime('' + data.boolean);
         return;
      }

      // see if we have RDF data
      var isRDF = false;
      if(data.head.vars.length == 3 && data.head.vars[0] == 'subject' && data.head.vars[1] == 'predicate' && data.head.vars[2] == 'object') {
         isRDF = true;
      } else if(data.head.vars.length == 4 && data.head.vars[0] == 'subject' && data.head.vars[1] == 'predicate' && data.head.vars[2] == 'object' && data.head.vars[3] == 'context') {
          isRDF = true;
         // see if c is used or not
         var isContextUsed = false;
         for(i=0; i<data.results.bindings.length; i++) {
            if('context' in data.results.bindings[i]) {
               isContextUsed = true;
               break;
            }
         }

         if(!isContextUsed) {
            // remove (unused) c variable from JSON
            data.head.vars.pop();
         }
      }

      if(isRDF) {
         $('#rdf-formats').prop('disabled', false);
      } else {
         $('#rdf-formats').prop('disabled', true);
         if($('#rdf-formats option:selected').length == 1) {
            $('#non-rdf-formats option:first').prop('selected', true);
         }
      }

      // put query variables in table header
      var thead = $('<thead>').appendTo(table);
      tr = $('<tr>');
      for(i=0; i<data.head.vars.length; i++) {
         tr.append('<th>' + data.head.vars[i] + '</th>');
      }
      thead.append(tr);
      table.append(thead);

      $('#total-results').html(data.results.bindings.length);
      updateResultCountAndExecutionTime(data.results.bindings.length);
      setNumberOfPages();
      showPage(1);

      $('#query-response a').click(function(e) {
         e.preventDefault();
         explore(NAMESPACE, this.textContent);
      });
   }
}

function showQueryExplanation(data) {

   //See http://stackoverflow.com/questions/3665115/create-a-file-in-memory-for-user-to-download-not-through-server
   //BLZG-1466: Adds a download link for the query explain
   $('#query-explanation-download').show().html('<a href = \"data:text/html;charset=utf-8,' 
        + encodeURIComponent(data) + '\" download = \"explain.html\">Download Query Explanation</a>');
   $('#query-explanation').html(data)
   $('#query-explanation').show();
}

function queryResultsError(jqXHR, textStatus, errorThrown) {
   $('#query-response, #query-export-container').show();
   var message = 'ERROR: ';
   if(jqXHR.status === 0) {
      message += 'Could not contact server';
   } else {
      var response = $('<div>').append(jqXHR.responseText);
      if(response.find('pre').length === 0) {
         message += response.text();
      } else {
         message += response.find('pre').text();
      }
      highlightError(jqXHR.responseText, 'query');
   }
   $('#query-response').text(message);
}

function highlightError(description, pane) {
   var match = description.match(/line (\d+), column (\d+)/);
   if(match) {
      // highlight character at error position
      var line = match[1] - 1;
      var character = match[2] - 1;
      ERROR_LINE_MARKERS[pane] = EDITORS[pane].doc.markText({line: line, ch: 0}, {line: line}, {className: 'error-line'});
      ERROR_CHARACTER_MARKERS[pane] = EDITORS[pane].doc.markText({line: line, ch: character}, {line: line, ch: character + 1}, {className: 'error-character'});
   }
}

function showDatatypes() {
   if(this.checked) {
      $('#query-response td[data-datatype]').each(function(i, el) {
         $(this).html('"' + $(this).html() + '"<span class="datatype">^^' + abbreviate($(this).data('datatype')) + '</span>');
      });
   } else {
      $('#query-response table td[data-datatype]').each(function(i, el) {
         $(this).find('.datatype').remove();
         $(this).html($(this).html().slice(1, -1));
      });
   }
}

function showLanguages() {
   if(this.checked) {
      $('#query-response td[data-lang]').each(function(i, el) {
         $(this).html('"' + $(this).html() + '"<span class="language">@' + $(this).data('lang') + '</span>');
      });
   } else {
      $('#query-response table td[data-lang]').each(function(i, el) {
         $(this).find('.language').remove();
         $(this).html($(this).html().slice(1, -1));         
      });
   }
}


/* Query result pagination */

function setNumberOfPages() {
   TOTAL_PAGES = Math.ceil(QUERY_RESULTS.results.bindings.length / PAGE_SIZE);
   $('#result-pages').html(TOTAL_PAGES);
}

function setPageSize(n) {
   if(n == 'all') {
      n = QUERY_RESULTS.results.bindings.length;
   } else {
      n = parseInt(n, 10);
      if(typeof n != 'number' || n % 1 !== 0 || n < 1 || n === PAGE_SIZE) {
         return;
      }
   }

   PAGE_SIZE = n;
   setNumberOfPages();
   // TODO: show page containing current first result
   showPage(1);
}

function handlePageSelector(e) {
   if(e.which == 13) {
      var n = parseInt(this.value, 10);
      if(typeof n != 'number' || n % 1 !== 0 || n < 1 || n > TOTAL_PAGES) {
         this.value = CURRENT_PAGE;
      } else {
         showPage(n);
      }
   }
}

function showPage(n) {
   if(typeof n != 'number' || n % 1 !== 0 || n < 1 || n > TOTAL_PAGES) {
      return;
   }

   CURRENT_PAGE = n;

   // clear table results, leaving header
   $('#query-response tbody tr').remove();

   // work out indices for this page
   var start = (CURRENT_PAGE - 1) * PAGE_SIZE;
   var end = Math.min(CURRENT_PAGE * PAGE_SIZE, QUERY_RESULTS.results.bindings.length);

   // add matching bindings
   var table = $('#query-response table');
   var text, tdData, linkText;
   for(var i=start; i<end; i++) {
         var tr = $('<tr>');
         for(var j=0; j<QUERY_RESULTS.head.vars.length; j++) {
            if(QUERY_RESULTS.head.vars[j] in QUERY_RESULTS.results.bindings[i]) {
               var binding = QUERY_RESULTS.results.bindings[i][QUERY_RESULTS.head.vars[j]];
               if(binding.type == 'sid') {
                  text = getSID(binding);
               } else {
                  text = binding.value;
                  if(binding.type == 'uri') {
                     text = abbreviate(text);
                  }
               }
               linkText = escapeHTML(text).replace(/\n/g, '<br>');
               if(binding.type == 'typed-literal') {
                  tdData = ' class="literal" data-datatype="' + binding.datatype + '"';
                  text = linkText;
               } else {
                  if(binding.type == 'uri' || binding.type == 'sid') {
                     text = '<a href="' + buildExploreHash(text) + '">' + linkText + '</a>';
                  } else {
                     text = linkText;
                  }
                  tdData = ' class="' + binding.type + '"';
                  if(binding['xml:lang']) {
                     tdData += ' data-lang="' + binding['xml:lang'] + '"';
                  }
               }
               tr.append('<td' + tdData + '>' + text + '</td>');
            } else {
               // no binding
               tr.append('<td class="unbound">');
            }
         }
         table.append(tr);
   }

   // update current results numbers
   $('#current-results').html((start + 1) + '-' + end);
   $('#current-page').val(n);
}


/* Explore */

function exploreSubmit(e) {
   e.preventDefault();
   var uri = $(this).find('input[type="text"]').val().trim();
   if(uri) {
      // add < > if they're not present and this is not a URI with a recognised namespace
      var namespaces = [];
      for(var cat in NAMESPACE_SHORTCUTS) {
         for(var namespace in NAMESPACE_SHORTCUTS[cat]) {
            namespaces.push(namespace);
         }
      }
      var namespaced = '^(' + namespaces.join('|') + '):';
      if(uri[0] != '<' && !uri.match(namespaced)) {
         uri = '<' + uri;
         if(uri.slice(-1) != '>') {
            uri += '>';
         }
         $(this).find('input[type="text"]').val(uri);
      }
      loadURI(uri);

      // if this is a SID, make the components clickable
      var re = /<< *([^ ]+) +([^ ]+) +([^ ]+) *>>/;
      var match = uri.match(re);
      if(match) {
         var header = $('<h1>');
         header.append('<< <br>');
         for(var i=1; i<4; i++) {
            header.append($('<a href="' + buildExploreHash(match[i]) + '">').text(match[i])).append('<br>');
         }
         header.append(' >>');
         $('#explore-header').html(header);
      } else {
         $('#explore-header').html($('<h1>').text(uri));
      }
   }
}

function buildExploreHash(uri) {
   return '#explore:' + NAMESPACE + ':' + encodeURIComponent(uri);
}

function loadURI(target) {
   // identify if this is a vertex or a SID
   target = target.trim().replace(/\n/g, ' ');
   // var re = /<< *(?:<[^<>]*> *){3} *>>/;
   var re = /<< *([^ ]+) +([^ ]+) +([^ ]+) *>>/;
   var vertex = !target.match(re);

   // jshint multistr:true
   var vertexQuery = '\
select ?col1 ?col2 ?incoming (count(?star) as ?star) {\n\
       {\n\
         URI ?col1 ?col2\n\
         bind (false as ?incoming) .\n\
         optional {\n\
           {\n\
             bind(<<URI ?col1 ?col2>> AS ?sid) . ?sid ?sidP ?star\n\
           } union {\n\
             bind(<<URI ?col1 ?col2>> AS ?sid) . ?star ?sidP ?sid\n\
           }\n\
         }\n\
       } union {\n\
         ?col1 ?col2 URI\n\
         bind (true as ?incoming) .\n\
         optional {\n\
           {\n\
             bind(<<?col1 ?col2 URI>> AS ?sid) . ?sid ?sidP ?star\n\
           } union {\n\
             bind(<<?col1 ?col2 URI>> AS ?sid) . ?star ?sidP ?sid\n\
           }\n\
         }\n\
       }\n\
     }\n\
     group by ?col1 ?col2 ?incoming';
   
   
   var edgeQuery = '\
select ?col1 ?col2 ?incoming (count(?star) as ?star)\n\
with {\n\
  select ?explore where {\n\
    bind (SID as ?explore) .\n\
  }\n\
} as %_explore\n\
where {\n\
  include %_explore .\n\
  {\n\
    bind (false as ?incoming) . \n\
    optional {\n\
      { bind (<<?explore ?col1 ?col2>> as ?sid) . ?sid ?sidP ?star } union { bind (<<?explore ?col1 ?col2>> as ?sid) . ?star ?sidP ?sid }\n\
    }\n\
  } union {\n\
    bind (true as ?incoming) . \n\
    optional {\n\
      { bind (<<?col1 ?col2 ?explore>> as ?sid) . ?sid ?sidP ?star } union { bind (<<?col1 ?col2 ?explore>> as ?sid) . ?star ?sidP ?sid }\n\
    }\n\
  }\n\
}\n\
group by ?col1 ?col2 ?incoming';

   var query;
   if(vertex) {
      query = vertexQuery.replace(/URI/g, target);
   } else {
      query = edgeQuery.replace('SID', target);
   }
   var settings = {
      type: 'POST',
      data: 'query=' + encodeURIComponent(query),
      dataType: 'json',
      accepts: {'json': 'application/sparql-results+json'},
      success: updateExploreStart,
      error: updateExploreError
   };
   $.ajax(RO_URL_PREFIX + 'namespace/' + NAMESPACE + '/sparql', settings); 
}

function updateExploreStart(data) {
   var results = data.results.bindings.length > 0;

   // clear tables
   $('#explore-incoming, #explore-outgoing, #explore-attributes').html('<table>');
   $('#explore-results, #explore-results .box').show();

   // go through each binding, adding it to the appropriate table
   $.each(data.results.bindings, function(i, binding) {
      var cols = [binding.col1, binding.col2].map(function(col) {
         var uri;
         if(col.type == 'sid') {
            uri = getSID(col);
         } else {
            uri = col.value;
            if(col.type == 'uri') {
               uri = abbreviate(uri);
            }
         }
         var output = escapeHTML(uri).replace(/\n/g, '<br>');
         if(col.type == 'uri' || col.type == 'sid') {
            output = '<a href="' + buildExploreHash(uri) + '">' + output + '</a>';
         }
         return output;
      });
      var star = parseInt(binding.star.value);
      if(star > 0) {
         var sid;
         if(binding.incoming.value == 'true') {
            sid = '<< <' +  binding.col1.value + '> <' + binding.col2.value + '> ' + $('#explore-form input[type=text]').val() + ' >>';
         } else {
            sid = '<< ' + $('#explore-form input[type=text]').val() + ' <' +  binding.col1.value + '> <' + binding.col2.value + '> >>';
         }
         star = '<a href="' + buildExploreHash(sid) + '"><< * (' + star + ') >></a>';
      } else {
         star = '';
      }
      var row = '<tr><td>' + cols[0] + '</td><td>' + cols[1] + '</td><td>' + star + '</td></tr>';
      if(binding.incoming.value == 'true') {
         $('#explore-incoming table').append(row);
      } else {
         // either attribute or outgoing
         if(binding.col2.type == 'uri') {
            // outgoing
            $('#explore-outgoing table').append(row);
         } else {
            // attribute
            $('#explore-attributes table').append(row);
         }
      }
   });

   var sections = {incoming: 'Incoming Links', outgoing: 'Outgoing Links', attributes: 'Attributes'};
   for(var k in sections) {
      var id = '#explore-' + k;
      if($(id + ' table tr').length === 0) {
         $(id).html('No ' + sections[k]);
      } else {
         $(id).prepend('<h1>' + sections[k] + '</h1>');
      }
   }

   $('#explore-results a').click(function(e) {
      e.preventDefault();
      var components = parseHash(this.hash);
      explore(components[2], decodeURIComponent(components[3]));
   });
}

function explore(namespace, uri, noPush, loadLast) {
   useNamespace(namespace, loadLast);
   $('#explore-form input[type=text]').val(uri);
   $('#explore-form').submit();
   if(!loadLast) {
      showTab('explore', true);
   }
   if(!noPush) {
      history.pushState(null, null, '#explore:' + NAMESPACE + ':' + uri);
   }
   localStorage.lastExploreNamespace = namespace;
   localStorage.lastExploreURI = uri;
}

function parseHash(hash) {
   // match #tab:namespace:uri
   // :namespace:uri group optional
   // namespace optional
   var re = /#([^:]+)(?::([^:]*):(.+))?/;
   return hash.match(re);
}

function handlePopState() {
   var hash = parseHash(this.location.hash);
   if(!hash) {
      $('#tab-selector a:first').click();
   } else {
      if(hash[1] == 'explore' && typeof hash[2] !== 'undefined') {
         explore(hash[2], hash[3], true);
      } else {
         $('a[data-target=' + hash[1] + ']').click();
      }
   }
}

function updateExploreError(jqXHR, textStatus, errorThrown) {
   $('#explore-results .box').html('').hide();
   $('#explore-header').text('Error! ' + textStatus + ' ' + jqXHR.statusText);
   $('#explore-results, #explore-header').show();
}


/* Status */

function getStatus(e) {
   if(e) {
      e.preventDefault();
   }
   showQueries(true);
   $.get(RO_URL_PREFIX + 'status', function(data) {
      // get data inside a jQuery object
      data = $('<div>').append(data);
      getStatusNumbers(data);
   });
}

function getStatusNumbers(data) {
   $('#status-text').html(data);
   $('p:contains(Show queries, query details)').find('a').eq(0).click(function(e) { e.preventDefault(); showQueries(false); });
   $('p:contains(Show queries, query details)').find('a').eq(1).click(function(e) { e.preventDefault(); showQueries(true); });
}

function showRunningQueries(e) {
   e.preventDefault();
   showTab('status');
   showQueries();
}

function showQueries(details) {
   var url = RO_URL_PREFIX + 'status?showQueries';
   if(details) {
      url += '=details';
   }
   $.get(url, function(data) {
      // get data inside a jQuery object
      data = $('<div>').append(data);

      // update status numbers
      getStatusNumbers(data);

      // clear current list
      $('#running-queries').empty();

      data.find('h1').each(function(i, e) {
         e = $(e);
         // get numbers string, which includes cancel link
         var form = e.next();
         // HA mode has h1 before running queries
         if(form[0].tagName != 'FORM') {
            return;
         }
         var numbers = form.find('p')[0].textContent;
         // remove cancel link
         numbers = numbers.substring(0, numbers.lastIndexOf(','));
         // get query id
         var queryId = form.find('input[type=hidden]').val();
         // get SPARQL
         var sparqlContainer = form.next().next();
         var sparql = sparqlContainer.html();

         var queryDetails;
         if(details) {
            queryDetails = $('<div>').append(sparqlContainer.nextUntil('h1')).html();
         } else {
            queryDetails = '<a href="#">Details</a>';
         }

         // got all data, create a li for each query
         var li = $('<li><div class="query"><pre>' + sparql + '</pre></div><div class="query-numbers">' + numbers + ', <a href="#" class="cancel-query">Cancel</a></div><div class="query-details">' + queryDetails + '</div>');
         li.find('a').data('queryId', queryId);
         $('#running-queries').append(li);
      });

      $('.cancel-query').click(cancelQuery);
      $('.query-details a').click(getQueryDetails);
   });
}

function cancelQuery(e) {
   e.preventDefault();
   if(confirm('Cancel query?')) {
      var id = $(this).data('queryId');
      $.post(RW_URL_PREFIX + 'status?cancelQuery&queryId=' + id, function() { getStatus(); });
      $(this).parents('li').remove();
   }
}

function getQueryDetails(e) {
   e.preventDefault();
   var id = $(this).data('queryId');
   $.ajax({url: RO_URL_PREFIX + 'status?showQueries=details&queryId=' + id,
      success: function(data) {
         // get data inside a jQuery object
         data = $('<div>').append(data);

         // update status numbers
         getStatusNumbers(data);

         // details begin after second pre
         var details = $('<div>').append($(data.find('pre')[1]).nextAll()).html();

         $(this).parent().html(details);
      },
      context: this
   });
}


/* Health */

function getHealth(e) {
   e.preventDefault();
   $.get('/status?health', function(data) {

      if(data.deployment == 'standalone') {
         $('#health-tab').html('<div class="box">Server operating in standalone mode.</div>');
         $('#tab-selector a[data-target=health]').unbind('click');
         return;
      }

      $('#health-overview .health-status span').html(data.status);
      $('#health-overview').removeClass('health-good health-warning health-bad').addClass('health-' + data.status.toLowerCase());
      $('#health-overview .health-details span').html(data.details);
      $('#health-overview .health-version span').html(data.version);
      $('#health-overview .health-timestamp span').html(new Date(data.timestamp).toString());

      $('#health-services div').remove();
      for(var i=0; i<data.services.length; i++) {
         var container = $('<div>');
         var div = $('<div class="box">');
         div.appendTo(container);
         div.append('<p>ID: ' + data.services[i].id + '</p>');
         div.append('<p>Status: ' + data.services[i].status + '</p>');
         var health;
         switch(data.services[i].status) {
            case 'leader':
            case 'follower':
               health = 'good';
               break;
            case 'unready':
               health = 'bad';
               break;
            default:
               health = 'warning';
         }
         container.addClass('box health-' + health);
         container.appendTo($('#health-services'));
      }
   });
}

function showHealthTab() {
   var settings = {
      async: false,
      url: '/status?health',
      success: function(data) {
         if(data.deployment == 'HA') {
            $('#tab-selector a[data-target=health]').show();
         } else {
            $('#tab-selector a[data-target=health]').remove();
         }
      }
   };
   $.ajax(settings);
}


/* Performance */

function loadPerformance(path) {
   if(typeof path == 'undefined') {
      path = '';
   }
   $.get(RO_URL_PREFIX + 'counters?' + path, function(data) {
      $('#performance-tab .box').html(data);
      $('#performance-tab .box a').click(function(e) {
         e.preventDefault();
         loadPerformance(this.href.split('?')[1]);
      });
   });
}

/* Utility functions */

function getSID(binding) {
   return '<<\n ' + abbreviate(binding.subject.value) + '\n ' + abbreviate(binding.predicate.value) + '\n ' + abbreviate(binding.object.value) + '\n>>';
}

function abbreviate(uri) {
   for(var nsGroup in NAMESPACE_SHORTCUTS) {
      for(var ns in NAMESPACE_SHORTCUTS[nsGroup]) {
         if(uri.indexOf(NAMESPACE_SHORTCUTS[nsGroup][ns]) === 0) {
            return uri.replace(NAMESPACE_SHORTCUTS[nsGroup][ns], ns + ':');
         }
      }
   }
   return '<' + uri + '>';
}

// currently unused
function unabbreviate(uri) {
   if(uri.charAt(0) == '<') {
      // not abbreviated
      return uri;
   }
   // get namespace
   var namespace = uri.split(':', 1)[0];
   return '<' + uri.replace(namespace, NAMESPACE_SHORTCUTS[namespace]) + '>';
}

// currently unused
function parseSID(sid) {
   // var re = /<< <([^<>]*)> <([^<>]*)> <([^<>]*)> >>/;
   var re = /<< *([^ ]+) +([^ ]+) +([^ ]+) *>>/;
   var matches = sid.match(re);
   return {'s': matches[1], 'p': matches[2], 'o': matches[3]};
}

function escapeHTML(text) {
   return $('<div/>').text(text).html();
}

function copyObject(src) {
   // this returns a new object with the same keys & values as the input one.
   // It is used to get around CodeMirror updating the default config object 
   // passed to it with the values used, which are then applied to later uses
   // of the default config object.
   var dest = {};
   for(var key in src) {
      dest[key] = src[key];
   }
   return dest;
}


/* Local storage functions */

function loadLastNamespace() {
   if(localStorage.lastNamespace && namespaceExists(localStorage.lastNamespace)) {
      useNamespace(localStorage.lastNamespace);
   } else {
      // no previously selected namespace, or it doesn't exist - use the default
      useNamespace(DEFAULT_NAMESPACE);
   }
}

function loadLastExplore() {
   if(localStorage.lastExploreURI) {
      explore(localStorage.lastExploreNamespace, localStorage.lastExploreURI, true, true);
   }
}

function loadQueryHistory() {
   if(typeof localStorage.historyCount === 'undefined') {
      localStorage.historyCount = 0;
   } else {
      for(var i=localStorage.historyCount - 1; i>=0; i--) {
         addQueryHistoryRow(localStorage['history.time.' + i], localStorage['history.query.' + i],
            localStorage['history.results.' + i], localStorage['history.executionTime.' + i], false);
      }
      if(localStorage.historyCount > 0) {
         $('#query-history').show();
      }
   }
}

function deselect(e) {
      $('.pop').slideFadeToggle(function() {
        e.removeClass('selected');
      });    
    }

    $(function() {
      $('#contact').on('click', function() {
        if($(this).hasClass('selected')) {
          deselect($(this));               
        } else {
          $(this).addClass('selected');
          $('.pop').slideFadeToggle();
        }
        return false;
      });

      $('.close').on('click', function() {
        deselect($('#contact'));
        return false;
      });
    });

    $.fn.slideFadeToggle = function(easing, callback) {
      return this.animate({ opacity: 'toggle', height: 'toggle' }, 'fast', easing, callback);
    };


/* Startup functions */

function setupHandlers() {
   // debug to access closure variables
   // jshint debug:true
   $('html, textarea, select').bind('keydown', 'ctrl+d', function() { debugger; });

   $('.use-lbs').change(useLBS);

   $('.modal-cancel').click(closeModal);

   $('#search-form').submit(submitSearch);

   $('#tab-selector a').click(clickTab);
   // these should be , and . but Hotkeys views those keypresses as these characters
   $('html, textarea, select').bind('keydown', 'ctrl+¼', moveTabLeft);
   $('html, textarea, select').bind('keydown', 'ctrl+¾', moveTabRight);
   $('#tab-selector a[data-target=status]').click(getStatus);
   $('#tab-selector a[data-target=health], #health-refresh').click(getHealth);
   $('#tab-selector a[data-target=performance]').click(loadPerformance);

   $('.namespace-shortcuts').on('change', 'select', selectNamespace);
   $('#custom-namespace-modal ul').on('click', 'a', deleteCustomNamespace);
   $('#add-custom-namespace').click(createNamespaceShortcut);
   $('.namespace-shortcuts').on('click', 'button', showCustomNamespacesModal);

   $('#new-namespace-mode').change(changeNamespaceMode);
   $('#new-namespace-isolatableIndices').change(changeNamespaceMode);
   $('#new-namespace-geoSpatial').change(changeNamespaceMode);   
   $('#namespace-create').submit(function(e){
        e.preventDefault();
        var hiddenSection = $('.popup-container');
        hiddenSection.click(function(event){
            if(event.target.className.indexOf("popup-container") !== -1){
                hiddenSection.fadeOut();
            }
        });
        getPreparedProperties($('#properties-text-area'));
        if ($('#properties-text-area').html()) {
            hiddenSection.fadeIn()
                // unhide section.hidden
                .css({ 'display':'block' })
                // set to full screen
                .css({ width: '100%', height: '100%', top: '0', left: '0' })
                // greyed out background
                .css({ 'background-color': 'rgba(0,0,0,0.5)' })
                .appendTo('body');
                
            
                $('#cancel-namespace').click(function(){ 
                        hiddenSection.fadeOut(); 
                    });
                $('#create-namespace').focus().click(function(){ 
                        $(hiddenSection).fadeOut(); 
                        createNamespace($('#properties-text-area'));
                    });
        }
    });
   $('#update-type').change(function() { setUpdateSettings(this.value); });
   $('#rdf-type').change(function() { setUpdateMode('rdf'); });
   $('#update-file').change(handleFileInput);
   // $('#update-box').on('dragover', handleDragOver)
   //    .on('drop', handleFile)
   //    .on('paste', handlePaste)
   //    .on('input propertychange', function() { $('#update-errors').hide(); });
   $('#clear-file').click(clearFile);
   $('#update-update').click(submitUpdate);
   $('#update-clear').click(clearUpdateOutput);

   $('.advanced-features-toggle').click(toggleAdvancedFeatures);

   $('#query-box').on('input propertychange', function() { $('#query-errors').hide(); });
   $('#query-form').submit(submitQuery);
   $('#query-explain').change(handleExplain);
   $('#query-details').change(handleDetails);
   $('#query-history').on('click', '.query a', loadHistory);
   $('#query-history').on('click', '.query-execution-time a', showRunningQueries);
   $('#query-history').on('click', '.query-delete a', deleteHistoryRow);
   $('#query-history-clear').click(clearHistory);
   $('#query-response-clear').click(clearQueryResponse);
   $('#query-export').click(showQueryExportModal);
   $('#query-download').click(queryExport);

   $('#results-per-page').change(function() { setPageSize(this.value); });
   $('#previous-page').click(function() { showPage(CURRENT_PAGE - 1); });
   $('#next-page').click(function() { showPage(CURRENT_PAGE + 1); });
   $('#current-page').keyup(handlePageSelector);
   $('#show-datatypes').click(showDatatypes);
   $('#show-languages').click(showLanguages);
   $('#explore-form').submit(exploreSubmit);

   // handle browser history buttons and initial display of first tab
   window.addEventListener("popstate", handlePopState);
   $(handlePopState);
}

function startup() {
   // load namespaces, default namespace, HA status
   useLBS(false); // Note: default to false. Otherwise workbench breaks when not deployed into jetty container.
   getNamespaces(true/*synchronous*/); // Note: 'synchronous' is slow if there are a lot of namespaces. Only used on startup.
   getDefaultNamespace();
   changeNamespaceMode(); // Note: Appears to be required to hide error message on load.
   showHealthTab();

   // complete setup
   loadCustomNamespaces();
   populateNamespaceShortcuts();
   createUpdateEditor();
   createQueryEditor();
   createExportOptions();
   setupHandlers();

   // restore last used namespace, last explored URI and query history
   loadLastExplore();
   loadLastNamespace();
   loadQueryHistory();
}

startup();

});
