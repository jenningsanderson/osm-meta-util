//Basic requirements
var MetaUtil = require('../');
var MongoClient = require('mongodb').MongoClient;
var turf = require('turf')
var argv = require('minimist')(process.argv.slice(2));

//CLI Configuration Variables
var db = argv['db'];
var tags_collection = argv['tags_collection'];
var tags_delay = argv['tags_delay'] || 60000 //Default, check tags collection every minute

var delay = argv['delay']
var start = argv['start']
var end   = argv['end']

var config = {
   'delay': delay
   'start': start, //file number
   'end'  : end
  }

///////////////////// OVER-RIDING FOR TESTING
var bbox = {"type": "FeatureCollection",
                 "features": [
                  { "type": "Feature", "properties": {"Country": "Nepal"}, "geometry": { "type": "Polygon", "coordinates": [ [ [ 87.334008, 26.348672 ], [ 86.727386, 26.425131 ], [ 85.84404, 26.569691 ], [ 85.204407, 26.760311000000101 ], [ 83.301567, 27.33289 ], [ 82.731164, 27.505030000000101 ], [ 81.879593, 27.862591 ], [ 81.308311, 28.140831 ], [ 80.360421, 28.639230000000101 ], [ 80.058426, 28.83526 ], [ 80.052727, 28.845881000000102 ], [ 80.051666, 28.851301 ], [ 80.051323, 28.9098210000001 ], [ 80.054572000000107, 28.9247610000001 ], [ 80.234146, 29.451761 ], [ 80.357041, 29.7487110000001 ], [ 80.360627, 29.7548290000001 ], [ 80.396607000000103, 29.7956 ], [ 80.888557, 30.22167 ], [ 80.891937, 30.22323 ], [ 81.406807, 30.41374 ], [ 81.433212, 30.41921 ], [ 81.467828, 30.42313 ], [ 81.594481000000101, 30.4310890000001 ], [ 81.634407, 30.43339 ], [ 82.130546, 30.333879 ], [ 82.148262, 30.3286400000001 ], [ 88.170326, 27.866471 ], [ 88.172088, 27.8656900000001 ], [ 88.193878, 27.850781 ], [ 88.194229, 27.84265 ], [ 88.182373, 26.7410700000001 ], [ 88.181961000000101, 26.729521000000101 ], [ 88.179817, 26.71843 ], [ 88.096771, 26.446292 ], [ 88.091737, 26.43462 ], [ 88.024559, 26.355571000000101 ], [ 88.013398, 26.35294 ], [ 87.334008, 26.348672 ] ] ] } }
                 ]}
var config = {
   'delay': 1000,
   'start': '001296000', //file number
   'end': '001296200'
  }
///////////////////////////////////////////

var tags = []
function updateTags(db, tags_collection){
  db.collection(tags_collection).find().toArray(function(err, results){
    tags = results.map(function(result){
      return result.tag;
    });
  });
  return tags
}

function insert(db, changeset){
  changeset.created_at = new Date(changeset.created_at);
  changeset.closed_at  = new Date(changeset.closed_at);

  db.collection('changesets').update(
    { "id": changeset.id },
    changeset,
    { upsert: true,  writeConcern: 0  },
    function(err,result){}
  );
}

function filterAndImport(db){
  var meta = MetaUtil( config );

  if (tags_collection != undefined) {
    tags = updateTags(db, tags_collection)
    setInterval(function() {
      tags = updateTags(db, tags_collection)
    },tags_delay);
  }

  meta.on('data', function(chunk,err){

    var changeset = JSON.parse(chunk)

    //Check if we're using bounding boxes?
    if (bbox){
      var min_lat = parseFloat(changeset['min_lat'])
      var max_lat = parseFloat(changeset['max_lat'])
      var min_lon = parseFloat(changeset['min_lon'])
      var max_lon = parseFloat(changeset['max_lon'])

      var changesetGeoJSON = {type: "FeatureCollection", features:[
        {type: "Feature",
         properties: {"title" : "Changeset BBOX"},
         geometry: {type: "Point", coordinates: [min_lon, min_lat]}},
        {type: "Feature",
         properties: {},
         geometry: {type: "Point", coordinates: [min_lon, max_lat]}},
        {type: "Feature",
         properties: {},
         geometry: {type: "Point", coordinates: [max_lon, min_lat]}},
        {type: "Feature",
         properties: {},
         geometry: {type: "Point", coordinates: [max_lon, max_lat]}}
      ]}

      var intersect = turf.within(changesetGeoJSON, bbox)

      if (intersect.features.length > 0){
        insert(db, changeset)
      }
    }

    //Check if we're using tags
    if (tags_collection != undefined){
      var intersection = []; var j = 0; var theseTags = []
      if (changeset.tags != undefined){
        if (changeset.tags.comment){
          theseTags = changeset.tags.comment.split(/[\s,]+/);
        }
      }else{
        return null
      }
      for (var i=0; i < theseTags.length; ++i){
        if (tags.indexOf(theseTags[i]) != -1){
          intersection[j++] = theseTags[i];
        }
      }
      if (j > 0) {
        insert(db, changeset)
      }
    }
  });
}


//Call it, all dependent on being able to connect to Mongo
MongoClient.connect('mongodb://localhost:27017/' + db, function(err, database) {
  if(err) throw err;
  filterAndImport(database);
});
