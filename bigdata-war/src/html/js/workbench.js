$(function() {

// global variables
var DEFAULT_NAMESPACE, NAMESPACE, NAMESPACE_URL, NAMESPACES_READY, NAMESPACE_SHORTCUTS, FILE_CONTENTS, QUERY_RESULTS;
var PAGE_SIZE=10, TOTAL_PAGES, CURRENT_PAGE;

/* Modal functions */

function showModal(id) {
   $('#' + id).show();
   $('body').addClass('modal-open');
}

$('.modal-cancel').click(function() {
   $('body').removeClass('modal-open');
   $(this).parents('.modal').hide();
});

/* Search */

$('#search-form').submit(function(e) {
   e.preventDefault();
   var term = $(this).find('input').val();
   if(!term) {
      return;
   }
   var query = 'select ?s ?p ?o { ?o bds:search "' + term + '" . ?s ?p ?o . }'
   $('#query-box').val(query);
   $('#query-form').submit();
   showTab('query');
});

/* Tab selection */

$('#tab-selector a').click(function(e) {
   showTab($(this).data('target'));
});

function showTab(tab, nohash) {
   $('.tab').hide();
   $('#' + tab + '-tab').show();
   $('#tab-selector a').removeClass();
   $('a[data-target=' + tab + ']').addClass('active');
   if(!nohash && window.location.hash.substring(1).indexOf(tab) != 0) {
      window.location.hash = tab;
   }
}

function moveTab(next) {
   // get current position
   var current = $('#tab-selector .active');
   if(next) {
      if(current.next().length) {
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

// these should be , and . but Hotkeys views those keypresses as these characters
$('html, textarea, select').bind('keydown', 'ctrl+¼', function() { moveTab(false); });
$('html, textarea, select').bind('keydown', 'ctrl+¾', function() { moveTab(true); });

/* Namespaces */

function getNamespaces() {
   $.get('/bigdata/namespace?describe-each-named-graph=false', function(data) {
      $('#namespaces-list').empty();
      var rdf = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
      var namespaces = namespaces = data.getElementsByTagNameNS(rdf, 'Description')
      for(var i=0; i<namespaces.length; i++) {
         var title = namespaces[i].getElementsByTagName('title')[0].textContent;
         var titleText = title == DEFAULT_NAMESPACE ? title + ' (default)' : title;
         var url = namespaces[i].getElementsByTagName('sparqlEndpoint')[0].getAttributeNS(rdf, 'resource');
         var use;
         if(title == NAMESPACE) {
            use = 'In use';
         } else {
            use = '<a href="#" class="use-namespace">Use</a>';
         }
         $('#namespaces-list').append('<li data-name="' + title + '" data-url="' + url + '">' + titleText + ' - ' + use + ' - <a href="#" class="delete-namespace">Delete</a> - <a href="#" class="namespace-properties">Properties</a> - <a href="/bigdata/namespace/' + title + '/sparql" class="namespace-service-description">Service Description</a></li>');
      }
      $('.use-namespace').click(function(e) {
         e.preventDefault();
         useNamespace($(this).parent().data('name'), $(this).parent().data('url'));
      });
      $('.delete-namespace').click(function(e) {
         e.preventDefault();
         deleteNamespace($(this).parent().data('name'));
      });
      $('.namespace-properties').click(function(e) {
         e.preventDefault();
         getNamespaceProperties($(this).parent().data('name'));
      });
      $('.namespace-service-description').click(function(e) {
         return confirm('This can be an expensive operation. Proceed anyway?');
      });
      NAMESPACES_READY = true;
   });
}

function selectNamespace(name) {
   // for programmatically selecting a namespace with just its name
   if(!NAMESPACES_READY) {
      setTimeout(function() { selectNamespace(name); }, 10);
   } else {
      $('#namespaces-list li[data-name=' + name + '] a.use-namespace').click();
   }
}

function useNamespace(name, url) {
   $('#current-namespace').html(name);
   NAMESPACE = name;
   NAMESPACE_URL = url;
   getNamespaces();
}

function deleteNamespace(namespace) {
   // prevent default namespace from being deleted
   if(namespace == DEFAULT_NAMESPACE) {
      alert('You may not delete the default namespace.');
      return;
   }

   if(confirm('Are you sure you want to delete the namespace ' + namespace + '?')) {
      if(namespace == NAMESPACE) {
         // FIXME: what is the desired behaviour when deleting the current namespace?
      }
      var url = '/bigdata/namespace/' + namespace;
      var settings = {
         type: 'DELETE',
         success: getNamespaces,
         error: function() { alert('Could not delete namespace ' + namespace); }
      };
      $.ajax(url, settings);
   }
}

function getNamespaceProperties(namespace) {
   $('#namespace-properties h1').html(namespace);
   $('#namespace-properties table').empty();
   $('#namespace-properties').show();
   var url = '/bigdata/namespace/' + namespace + '/properties';
   $.get(url, function(data) {
      $.each(data.getElementsByTagName('entry'), function(i, entry) {
         $('#namespace-properties table').append('<tr><td>' + entry.getAttribute('key') + '</td><td>' + entry.textContent + '</td></tr>');
      });
   });
}

function createNamespace(e) {
   e.preventDefault();
   var input = $(this).find('input[type=text]');
   var namespace = input.val();
   if(!namespace) {
      return;
   }
   // TODO: validate namespace
   // TODO: allow for other options to be specified
   var data = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">\n<properties>\n<entry key="com.bigdata.rdf.sail.namespace">' + namespace + '</entry>\n</properties>';
   var settings = {
      type: 'POST',
      data: data,
      contentType: 'application/xml',
      success: function() { input.val(''); getNamespaces(); },
      error: function(jqXHR, textStatus, errorThrown) { alert(jqXHR.statusText); }
   };
   $.ajax('/bigdata/namespace', settings);
}
$('#namespace-create').submit(createNamespace);

function getDefaultNamespace() {
   $.get('/bigdata/namespace?describe-each-named-graph=false&describe-default-namespace=true', function(data) {
      // Chrome does not work with rdf\:Description, so look for Description too
      var defaultDataset = $(data).find('rdf\\:Description, Description');
      DEFAULT_NAMESPACE = defaultDataset.find('title')[0].textContent;
      var url = defaultDataset.find('sparqlEndpoint')[0].attributes['rdf:resource'].textContent;
      useNamespace(DEFAULT_NAMESPACE, url);
   });
}

getDefaultNamespace();


/* Namespace shortcuts */

NAMESPACE_SHORTCUTS = {
   'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
   'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
   'owl': 'http://www.w3.org/2002/07/owl#',
   'bd': 'http://www.bigdata.com/rdf#',
   'bds': 'http://www.bigdata.com/rdf/search#',
   'foaf': 'http://xmlns.com/foaf/0.1/',
   'hint': 'http://www.bigdata.com/queryHints#',
   'dc': 'http://purl.org/dc/elements/1.1/',
   'xsd': 'http://www.w3.org/2001/XMLSchema#'
};

$('.namespace-shortcuts').html('<ul>');
for(var ns in NAMESPACE_SHORTCUTS) {
   $('.namespace-shortcuts ul').append('<li data-ns="prefix ' + ns + ': <' + NAMESPACE_SHORTCUTS[ns] + '>">' + ns.toUpperCase() + '</li>');
}

$('.namespace-shortcuts li').click(function() {
   var textarea = $(this).parents('.tab').find('textarea');
   var current = textarea.val();
   var ns = $(this).data('ns');

   if(current.indexOf(ns) == -1) {
      textarea.val(ns + '\n' + current);
   }
});


/* Load */

function handleDragOver(e) {
   e.stopPropagation();
   e.preventDefault();
   e.originalEvent.dataTransfer.dropEffect = 'copy';
}

function handleFile(e) {
   e.stopPropagation();
   e.preventDefault();

   if(e.type == 'drop') {
      var files = e.originalEvent.dataTransfer.files;
   } else {
      var files = e.originalEvent.target.files;
   }
   
   // only one file supported
   if(files.length > 1) {
      alert('Ignoring all but first file');
   }
   
   var f = files[0];
   
   // if file is too large, tell user to supply local path
   if(f.size > 1048576 * 100) {
      alert('File too large, enter local path to file');
      $('#load-box').val('/path/to/' + f.name);
      setType('path');
      $('#load-box').prop('disabled', false)
      $('#large-file-message, #clear-file').hide();
   } else {
      var fr = new FileReader();
      fr.onload = function(e2) {
         if(f.size > 10240) {
            // do not use textarea
            $('#load-box').prop('disabled', true)
            $('#filename').html(f.name);
            $('#large-file-message, #clear-file').show()
            $('#load-box').val('');
            FILE_CONTENTS = e2.target.result;
         } else {
            // display file contents in the textarea
            clearFile();
            $('#load-box').val(e2.target.result);
         }
         guessType(f.name.split('.').pop().toLowerCase(), e2.target.result);
      };
      fr.readAsText(f);
   }

   $('#load-file').val('');
}

function clearFile(e) {
   if(e) {
      e.preventDefault();
   }
   $('#load-box').prop('disabled', false)
   $('#large-file-message, #clear-file').hide()
   FILE_CONTENTS = null;
}

function guessType(extension, content) {
   // try to guess type
   if(extension == 'rq') {
      // SPARQL
      setType('sparql');
   } else if(extension in rdf_types) {
      // RDF
      setType('rdf', rdf_types[extension]);
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
      var windows = /^((file:\/\/)([A-Za-z]:?([\/][^\/\\]+)+))|([A-Za-z]:([\\\/][^\\\/]+)+)$/;
      var http = /^https?:\/((\/[^\/]+)+)$/;
      if(unix.test(text.trim()) || windows.test(text.trim()) || http.test(text.trim())) {
         return 'path';
      }
   }
   
   text = text.toUpperCase();
   for(var i=0; i<sparql_update_commands.length; i++) {
      if(text.indexOf(sparql_update_commands[i]) != -1) {
         return 'sparql';
      }
   }

   return 'rdf';
}

function handlePaste(e) {   
   // if the input is currently empty, try to identify the pasted content
   var that = this;
   if(this.value == '') {
      setTimeout(function() { setType(identify(that.value, true)); }, 10);
   } 
}

function setType(type, format) {
   $('#load-type').val(type);
   if(type == 'rdf') {
      $('#rdf-type').val(format);
   }
}

// .xml is used for both RDF and TriX, assume it's RDF
// We could check the parent element to see which it is
var rdf_types = {'nq': 'n-quads',
                 'nt': 'n-triples',
                 'n3': 'n3',
                 'rdf': 'rdf/xml',
                 'rdfs': 'rdf/xml',
                 'owl': 'rdf/xml',
                 'xml': 'rdf/xml',
                 'json': 'json',
                 'trig': 'trig',
                 'trix': 'trix',
                 //'xml': 'trix',
                 'ttl': 'turtle'};
                 
var rdf_content_types = {'n-quads': 'text/x-nquads',
                         'n-triples': 'text/plain',
                         'n3': 'text/rdf+n3',
                         'rdf/xml': 'application/rdf+xml',
                         'json': 'application/sparql-results+json',
                         'trig': 'application/x-trig',
                         'trix': 'application/trix',
                         'turtle': 'application/x-turtle'};

var sparql_update_commands = ['INSERT', 'DELETE'];

$('#load-file').change(handleFile);
$('#load-box').on('dragover', handleDragOver)
   .on('drop', handleFile)
   .on('paste', handlePaste)
   .on('input propertychange', function() { $('#load-errors').hide(); })
   .bind('keydown', 'ctrl+return', submitLoad);
$('#clear-file').click(clearFile);

$('#load-load').click(submitLoad);

function submitLoad(e) {
   e.preventDefault();

   var settings = {
      type: 'POST',
      data: FILE_CONTENTS == null ? $('#load-box').val() : FILE_CONTENTS,
      success: updateResponseXML,
      error: updateResponseError
   }

   // determine action based on type
   switch($('#load-type').val()) {
      case 'sparql':
         settings.data = 'update=' + encodeURIComponent(settings.data);
         settings.success = updateResponseHTML;
         break;
      case 'rdf':
         var type = $('#rdf-type').val();
         if(!type) {
            alert('Please select an RDF content type.');
            return;
         }
         settings.contentType = rdf_content_types[type];
         break;
      case 'path':
         // if no scheme is specified, assume a local path
         if(!/^(file|(https?)):\/\//.test(settings.data)) {
            settings.data = 'file://' + settings.data;
         }
         settings.data = 'uri=' + encodeURIComponent(settings.data);
         break;
   }

   $('#load-response').show();
   $('#load-response pre').html('Data loading...');   

   $.ajax(NAMESPACE_URL, settings);
}

$('#load-clear').click(function() {
   $('#load-response, #load-clear').hide();
   $('#load-response pre').text('');
});

$('#advanced-features-toggle').click(function() {
   $('#advanced-features').toggle();
   return false;
});

function updateResponseHTML(data) {
   $('#load-response, #load-clear').show();
   $('#load-response pre').html(data);
}

function updateResponseXML(data) {
   var modified = data.childNodes[0].attributes['modified'].value;
   var milliseconds = data.childNodes[0].attributes['milliseconds'].value;
   $('#load-response, #load-clear').show();
   $('#load-response pre').text('Modified: ' + modified + '\nMilliseconds: ' + milliseconds);
}

function updateResponseError(jqXHR, textStatus, errorThrown) {
   $('#load-response, #load-clear').show();
   $('#load-response pre').text('Error! ' + textStatus + ' ' + jqXHR.statusText);
   highlightError(jqXHR.statusText, 'load');
}


/* Query */

$('#query-box').bind('keydown', 'ctrl+return', function(e) { e.preventDefault(); $('#query-form').submit(); })
               .on('input propertychange', function() { $('#query-errors').hide(); });
$('#query-form').submit(submitQuery);

function submitQuery(e) {
   e.preventDefault();

   var settings = {
      type: 'POST',
      data: $(this).serialize(),
      headers: { 'Accept': 'application/sparql-results+json, application/rdf+xml' },
      success: showQueryResults,
      error: queryResultsError
   }

   $('#query-response').show().html('Query running...');
   $('#query-pagination').hide();

   $.ajax(NAMESPACE_URL, settings);

   $('#query-explanation').empty();
   if($('#query-explain').is(':checked')) {
      settings = {
         type: 'POST',
         data: $(this).serialize() + '&explain=details',
         dataType: 'html',
         success: showQueryExplanation,
         error: queryResultsError
      };
      $.ajax(NAMESPACE_URL, settings);
   } else {
      $('#query-explanation').hide();
   }
}

$('#query-response-clear').click(function() {
   $('#query-response, #query-explanation').empty('');
   $('#query-response, #query-pagination, #query-explanation, #query-tab .bottom *').hide();
});

$('#query-export').click(function() { updateExportFileExtension(); showModal('query-export-modal'); });

var export_extensions = {
   "application/rdf+xml": ['RDF/XML', 'rdf', true],
   "application/x-turtle": ['N-Triples', 'nt', true],
   "application/x-turtle": ['Turtle', 'ttl', true],
   "text/rdf+n3": ['N3', 'n3', true],
   "application/trix": ['TriX', 'trix', true],
   "application/x-trig": ['TRIG', 'trig', true],
   "text/x-nquads": ['NQUADS', 'nq', true],

   "text/csv": ['CSV', 'csv', false, exportCSV],
   "application/sparql-results+json": ['JSON', 'json', false, exportJSON],
   // "text/tab-separated-values": ['TSV', 'tsv', false, exportTSV],
   "application/sparql-results+xml": ['XML', 'xml', false, exportXML]
};

for(var contentType in export_extensions) {
   var optgroup = export_extensions[contentType][2] ? '#rdf-formats' : '#non-rdf-formats';
   $(optgroup).append('<option value="' + contentType + '">' + export_extensions[contentType][0] + '</option>');
}

$('#export-format option:first').prop('selected', true);

$('#export-format').change(updateExportFileExtension);

function updateExportFileExtension() {
   $('#export-filename-extension').html(export_extensions[$('#export-format').val()][1]);
}

$('#query-download').click(function() {
   var dataType = $('#export-format').val();
   var filename = $('#export-filename').val();
   if(filename == '') {
      filename = 'export';
   }
   filename += '.' + export_extensions[dataType][1];
   if(export_extensions[dataType][2]) {
      // RDF
      var settings = {
         type: 'POST',
         data: JSON.stringify(QUERY_RESULTS),
         contentType: 'application/sparql-results+json',
         headers: { 'Accept': dataType },
         success: function() { downloadFile(data, dataType, filename); },
         error: downloadRDFError
      };
      $.ajax('/bigdata/sparql?workbench&convert', settings);
   } else {
      // not RDF
      export_extensions[dataType][3](filename);
   }
   $(this).siblings('.modal-cancel').click();
});

function downloadRDFError(jqXHR, textStatus, errorThrown) {
   alert(jqXHR.statusText);
}   

function exportXML(filename) {
   var xml = '<?xml version="1.0"?>\n<sparql xmlns="http://www.w3.org/2005/sparql-results#">\n\t<head>\n';
   var bindings = [];
   $('#query-response thead tr td').each(function(i, td) {
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
   var uri = 'data:' + type + ';charset=utf-8,' + encodeURIComponent(data);
   $('<a id="download-link" download="' + filename + '" href="' + uri + '">').appendTo('body')[0].click();
   $('#download-link').remove();
}

function showQueryResults(data) {
   $('#query-response').empty();
   $('#query-export-rdf').hide();
   $('#query-response, #query-pagination, #query-tab .bottom *').show();
   var table = $('<table>').appendTo($('#query-response'));
   if(this.dataTypes[1] == 'xml') {
      // RDF
      table.append($('<thead><tr><td>s</td><td>p</td><td>o</td></tr></thead>'));
      var rows = $(data).find('Description');
      for(var i=0; i<rows.length; i++) {
         // FIXME: are about and nodeID the only possible attributes here?
         var s = rows[i].attributes['rdf:about'];
         if(typeof(s) == 'undefined') {
            s = rows[i].attributes['rdf:nodeID'];
         }
         s = s.textContent;
         for(var j=0; j<rows[i].children.length; j++) {
            var p = rows[i].children[j].tagName;
            var o = rows[i].children[j].attributes['rdf:resource'];
            // FIXME: is this the correct behaviour?
            if(typeof(o) == 'undefined') {
               o = rows[i].children[j].textContent;
            } else {
               o = o.textContent;
            }
            var tr = $('<tr><td>' + (j == 0 ? s : '') + '</td><td>' + p + '</td><td>' + o + '</td>');
            table.append(tr);
         }
      }
   } else {
      // JSON
      // save data for export and pagination
      QUERY_RESULTS = data;

      if(typeof(data.boolean) != 'undefined') {
         // ASK query
         table.append('<tr><td>' + data.boolean + '</td></tr>').addClass('boolean');
         return;
      }

      // see if we have RDF data
      var isRDF = false;
      if(data.head.vars.length == 3 && data.head.vars[0] == 's' && data.head.vars[1] == 'p' && data.head.vars[2] == 'o') {
         isRDF = true;
      } else if(data.head.vars.length == 4 && data.head.vars[0] == 's' && data.head.vars[1] == 'p' && data.head.vars[2] == 'o' && data.head.vars[3] == 'c') {
         // see if c is used or not
         for(var i=0; i<data.results.bindings.length; i++) {
            if('c' in data.results.bindings[i]) {
               isRDF = false;
               break;
            }
         }

         if(isRDF) {
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
      var tr = $('<tr>');
      for(var i=0; i<data.head.vars.length; i++) {
         tr.append('<th>' + data.head.vars[i] + '</th>');
      }
      thead.append(tr);
      table.append(thead);

      setNumberOfPages();
      showPage(1);

      $('#query-response a').click(function(e) {
         e.preventDefault();
         explore(this.textContent);
      });
   }
}

function showQueryExplanation(data) {
   $('#query-explanation').html(data).show();
}

function queryResultsError(jqXHR, textStatus, errorThrown) {
   $('#query-response, #query-tab .bottom *').show();
   $('#query-response').text('Error! ' + textStatus + ' ' + jqXHR.statusText);
   highlightError(jqXHR.statusText, 'query');
}

function highlightError(description, pane) {
   var match = description.match(/line (\d+), column (\d+)/);
   if(match) {
      // highlight character at error position
      var line = match[1] - 1;
      var column = match[2] - 1;
      var input = $('#' + pane + '-box').val();
      var lines = input.split('\n');
      var container = '#' + pane + '-errors';
      $(container).html('');
      for(var i=0; i<line; i++) {
         var p = $('<p>').text(lines[i]);
         $(container).append(p);
      }
      $(container).append('<p class="error-line">');
      $(container + ' .error-line').append(document.createTextNode(lines[line].substr(0, column)));
      $(container + ' .error-line').append($('<span class="error-character">').text(lines[line].charAt(column) || ' '));
      $(container + ' .error-line').append(document.createTextNode(lines[line].substr(column + 1)));
      $(container).show();
      $('#' + pane + '-box').scrollTop(0);
   }
}

/* Pagination */

function setNumberOfPages() {
   TOTAL_PAGES = Math.ceil(QUERY_RESULTS.results.bindings.length / PAGE_SIZE);
   $('#result-pages').html(TOTAL_PAGES);
}

function setPageSize(n) {
   n = parseInt(n, 10);
   if(typeof n != 'number' || n % 1 != 0 || n < 1 || n == PAGE_SIZE) {
      return;
   }

   PAGE_SIZE = n;
   setNumberOfPages();
   // TODO: show page containing current first result
   showPage(1);
}

$('#results-per-page').change(function() { setPageSize(this.value); });
$('#previous-page').click(function() { showPage(CURRENT_PAGE - 1); });
$('#next-page').click(function() { showPage(CURRENT_PAGE + 1); });
$('#current-page').keyup(function(e) {
   if(e.which == 13) {
      var n = parseInt(this.value, 10);
      if(typeof n != 'number' || n % 1 != 0 || n < 1 || n > TOTAL_PAGES) {
         this.value = CURRENT_PAGE;
      } else {
         showPage(n);
      }
   }
});

function showPage(n) {
   if(typeof n != 'number' || n % 1 != 0 || n < 1 || n > TOTAL_PAGES) {
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
   for(var i=start; i<end; i++) {
         var tr = $('<tr>');
         for(var j=0; j<QUERY_RESULTS.head.vars.length; j++) {
            if(QUERY_RESULTS.head.vars[j] in QUERY_RESULTS.results.bindings[i]) {
               var binding = QUERY_RESULTS.results.bindings[i][QUERY_RESULTS.head.vars[j]];
               if(binding.type == 'sid') {
                  var text = getSID(binding);
               } else {
                  var text = binding.value;
                  if(binding.type == 'uri') {
                     text = abbreviate(text);
                  }
               }
               linkText = escapeHTML(text).replace(/\n/g, '<br>');
               if(binding.type == 'typed-literal') {
                  var tdData = ' class="literal" data-datatype="' + binding.datatype + '"';
               } else {
                  if(binding.type == 'uri' || binding.type == 'sid') {
                     text = '<a href="' + buildExploreHash(text) + '">' + linkText + '</a>';
                  }
                  var tdData = ' class="' + binding.type + '"';
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
   $('#current-results').html((start + 1) + ' - ' + end);
   $('#current-page').val(n);
}

/* Explore */

$('#explore-form').submit(function(e) {
   e.preventDefault();
   var uri = $(this).find('input').val();
   if(uri) {
      loadURI(uri);

      // if this is a SID, make the components clickable
      // var re = /<< *(<[^<>]*>) *(<[^<>]*>) *(<[^<>]*>) *>>/;
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
});

function buildExploreHash(uri) {
   return '#explore:' + NAMESPACE + ':' + uri;
}

function loadURI(target) {
   // identify if this is a vertex or a SID
   target = target.trim().replace(/\n/g, ' ');
   // var re = /<< *(?:<[^<>]*> *){3} *>>/;
   var re = /<< *([^ ]+) +([^ ]+) +([^ ]+) *>>/;
   var vertex = !target.match(re);

   var vertexQuery = '\
select ?col1 ?col2 ?incoming (count(?star) as ?star) {\n\
  bind (URI as ?explore ) .\n\
  {\n\
    bind (<<?explore ?col1 ?col2>> as ?sid) . \n\
    bind (false as ?incoming) . \n\
    optional {\n\
      { ?sid ?sidP ?star } union { ?star ?sidP ?sid }\n\
    }\n\
  } union {\n\
    bind (<<?col1 ?col2 ?explore>> as ?sid) .\n\
    bind (true as ?incoming) . \n\
    optional {\n\
      { ?sid ?sidP ?star } union { ?star ?sidP ?sid }\n\
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
    bind (<<?explore ?col1 ?col2>> as ?sid) . \n\
    bind (false as ?incoming) . \n\
    optional {\n\
      { ?sid ?sidP ?star } union { ?star ?sidP ?sid }\n\
    }\n\
  } union {\n\
    bind (<<?col1 ?col2 ?explore>> as ?sid) .\n\
    bind (true as ?incoming) . \n\
    optional {\n\
      { ?sid ?sidP ?star } union { ?star ?sidP ?sid }\n\
    }\n\
  }\n\
}\n\
group by ?col1 ?col2 ?incoming';

   if(vertex) {
      var query = vertexQuery.replace('URI', target);
   } else {
      var query = edgeQuery.replace('SID', target);
   }
   var settings = {
      type: 'POST',
      data: 'query=' + encodeURIComponent(query),
      dataType: 'json',
      accepts: {'json': 'application/sparql-results+json'},
      success: updateExploreStart,
      error: updateExploreError
   };
   $.ajax(NAMESPACE_URL, settings); 
}

function updateExploreStart(data) {
   var results = data.results.bindings.length > 0;

   // clear tables
   $('#explore-incoming, #explore-outgoing, #explore-attributes').html('<table>');
   $('#explore-tab .bottom').hide();
   $('#explore-results, #explore-results .box').show();

   // go through each binding, adding it to the appropriate table
   $.each(data.results.bindings, function(i, binding) {
      var cols = [binding.col1, binding.col2].map(function(col) {
         if(col.type == 'sid') {
            var uri = getSID(col);
         } else {
            var uri = col.value;
            if(col.type == 'uri') {
               uri = '<' + uri + '>';
            }
         }
         output = escapeHTML(uri).replace(/\n/g, '<br>');
         if(col.type == 'uri' || col.type == 'sid') {
            output = '<a href="' + buildExploreHash(uri) + '">' + output + '</a>';
         }
         return output;
      });
      var star = parseInt(binding.star.value);
      if(star > 0) {
         if(binding.incoming.value == 'true') {
            var sid = '<< <' +  binding.col1.value + '> <' + binding.col2.value + '> ' + $('#explore-form input[type=text]').val() + ' >>';
         } else {
            var sid = '<< ' + $('#explore-form input[type=text]').val() + ' <' +  binding.col1.value + '> <' + binding.col2.value + '> >>';
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
      if($(id + ' table tr').length == 0) {
         $(id).html('No ' + sections[k]);
      } else {
         $(id).prepend('<h1>' + sections[k] + '</h1>');
      }
   }

   $('#explore-results a').click(function(e) {
      e.preventDefault();
      var components = parseHash(this.hash);
      exploreNamespacedURI(components[2], components[3]);
   });
}

function exploreNamespacedURI(namespace, uri, nopush) {
   if(!NAMESPACES_READY) {
      setTimeout(function() { exploreNamespacedURI(namespace, uri, nopush); }, 10);
   } else {
      selectNamespace(namespace);
      explore(uri, nopush);
   }
}

function explore(uri, nopush) {
   $('#explore-form input[type=text]').val(uri);
   $('#explore-form').submit();
   showTab('explore', true);
   if(!nopush) {
      history.pushState(null, null, '#explore:' + NAMESPACE + ':' + uri);
   }
}

function parseHash(hash) {
   // match #tab:namespace:uri
   // :namespace:uri group optional
   // namespace optional
   var re = /#([^:]+)(?::([^:]*):(.+))?/;
   return hash.match(re);
}

// handle history buttons and initial display of first tab
window.addEventListener("popstate", handlePopState);
$(handlePopState);

function handlePopState() {
   var hash = parseHash(this.location.hash);
   if(!hash) {
      $('#tab-selector a:first').click();
   } else {
      if(hash[1] == 'explore') {
         exploreNamespacedURI(hash[2], hash[3], true);
      } else {
         $('a[data-target=' + hash[1] + ']').click();
      }
   }
}

function updateExploreError(jqXHR, textStatus, errorThrown) {
   $('#explore-tab .bottom').show();
   $('#explore-results .box').html('').hide();
   $('#explore-header').text('Error! ' + textStatus + ' ' + jqXHR.statusText);
   $('#explore-results, #explore-header').show();
}

/* Status */

$('#tab-selector a[data-target=status]').click(getStatus);

function getStatus(e) {
   if(e) {
      e.preventDefault();
   }
   $.get('/bigdata/status', function(data) {
      // get data inside a jQuery object
      data = $('<div>').append(data);
      getStatusNumbers(data);
   });
}

function getStatusNumbers(data) {
      var accepted = data.text().match(/Accepted query count=(\d+)/)[1];
      var running = data.text().match(/Running query count=(\d+)/)[1];
      var numbers = $(data).find('pre')[0].textContent;
      $('#accepted-query-count').html(accepted);
      $('#running-query-count').html(running);
      $('#status-numbers').html(numbers);
}

$('#show-queries').click(function(e) {
   e.preventDefault();
   showQueries(false);
});

$('#show-query-details').click(function(e) {
   e.preventDefault();
   showQueries(true);
});

function showQueries(details) {
   var url = '/bigdata/status?showQueries';
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
         var numbers = form.find('p')[0].textContent;
         // remove cancel link
         numbers = numbers.substring(0, numbers.lastIndexOf(','));
         // get query id
         var queryId = form.find('input[type=hidden]').val();
         // get SPARQL
         var sparqlContainer = form.next().next();
         var sparql = sparqlContainer.html();

         if(details) {
            var queryDetails = $('<div>').append(sparqlContainer.nextUntil('h1')).html();
         } else {
            var queryDetails = '<a href="#">Details</a>';
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
      $.post('/bigdata/status?cancelQuery&queryId=' + id, function() { getStatus(); });
      $(this).parents('li').remove();
   }
}

function getQueryDetails(e) {
   e.preventDefault();
   var id = $(this).data('queryId');
   $.ajax({url: '/bigdata/status?showQueries=details&queryId=' + id,
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


/* Performance */

$('#tab-selector a[data-target=performance]').click(function(e) {
   $.get('/bigdata/counters', function(data) {
      $('#performance-tab .box').html(data);
   });
});

/* Utility functions */

function getSID(binding) {
   return '<<\n ' + abbreviate(binding.value['s'].value) + '\n ' + abbreviate(binding.value['p'].value) + '\n ' + abbreviate(binding.value['o'].value) + '\n>>';
}

function abbreviate(uri) {
   for(var ns in NAMESPACE_SHORTCUTS) {
      if(uri.indexOf(NAMESPACE_SHORTCUTS[ns]) == 0) {
         return uri.replace(NAMESPACE_SHORTCUTS[ns], ns + ':');
      }
   }
   return '<' + uri + '>';
}

function unabbreviate(uri) {
   if(uri.charAt(0) == '<') {
      // not abbreviated
      return uri;
   }
   // get namespace
   var namespace = uri.split(':', 1)[0];
   return '<' + uri.replace(namespace, NAMESPACE_SHORTCUTS[namespace]) + '>';
}

function parseSID(sid) {
   // var re = /<< <([^<>]*)> <([^<>]*)> <([^<>]*)> >>/;
   var re = /<< *([^ ]+) +([^ ]+) +([^ ]+) *>>/;
   var matches = sid.match(re);
   return {'s': matches[1], 'p': matches[2], 'o': matches[3]};
}

function escapeHTML(text) {
   return $('<div/>').text(text).html();
}

});
