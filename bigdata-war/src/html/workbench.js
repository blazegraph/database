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
   if(f.size > 100 * 1048576) {
      alert('File too large, enter local path to file');
      $('#mp-box').val('/path/to/' + f.name);
      setType('path');
      $('#mp-file').val('');
      return;
   }
   
   // if file is small enough, populate the textarea with it
   var holder;
   if(f.size < 10 * 1024) {
      holder = '#mp-box';
      $('#mp-hidden').val('');
   } else {
      // store file contents in hidden input and clear textarea
      holder = '#mp-hidden';
      $('#mp-box').val('');
   }
   var fr = new FileReader();
   fr.onload = function(e2) {
      $(holder).val(e2.target.result);
      guessType(f.name.split('.').pop().toLowerCase(), e2.target.result);
   };
   fr.readAsText(f);
   $('#mp-file').val('');
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
      content = content.toUpperCase();
      for(var i=0, found=false; i<sparql_update_commands.length; i++) {
         if(content.indexOf(sparql_update_commands[i]) != -1) {
            setType('sparql');
            found = true;
            break;
         }
      }
      if(!found) {
         setType('rdf', '');
      }
   }
}

function handlePaste(e) {
   alert('pasted!');
   e.stopPropagation();
   e.preventDefault();
}

function handleTypeChange(e) {
   $('#rdf-type').toggle($(this).val() == 'rdf');
}

function setType(type, format) {
   $('#mp-type').val(type);
   if(type == 'rdf') {
      $('#rdf-type').show();
      $('#rdf-type').val(format);
   } else {
      $('#rdf-type').hide();
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
                 'trig': 'trig',
                 'trix': 'trix',
                 //'xml': 'trix',
                 'ttl': 'turtle'};

var sparql_update_commands = ['INSERT', 'DELETE'];

$('#mp-file').change(handleFile);
$('#mp-box').on('drop', handleFile);
$('#mp-box').on('paste', handlePaste);
$('#mp-type').change(handleTypeChange);

$('#mp-form').submit(function() {
   // determine action based on type
   
});
