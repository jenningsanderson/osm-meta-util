var MetaUtil = require('../');

var argv = require('minimist')(process.argv.slice(2));
var db = argv['db'];
var tags_collection = argv['tags_collection'];
var tags = argv['_'][0];
var delay = argv['delay'] || 60000

var start = argv['start']
var end   = argv['end']

// '001370500'
//'001375800'

//Will fill files between start and end
var meta = MetaUtil({'start': start, 'end': end, 'tags':tags, 'db':db, 'tags_collection':tags_collection, 'delay':delay}).pipe(process.stdout);