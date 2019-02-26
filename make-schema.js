
/**
 * Module dependencies.
 */

var moment = require('moment');
var fs = require('fs');


// get fixtures.
var fixtures = require('./tap_recurly/schemas/fixtures.json');

// console.log(fixtures);


// write everything to /tap_clubspeed/schemas/

/**
 * Static dictionary of accepted data types.
 */

var datatypes = [
  'string',
  'integer',
  'number',
  'date-time',
  'boolean'
];

// Do The Thing.

Object.keys(fixtures).forEach(function(schema) {
  console.log('schema is ', schema);
  var json = generateSchemaJson(schema, fixtures[schema]);
  fs.writeFileSync('tap_recurly/schemas/' + schema + '.json', JSON.stringify(json, null, 2), 'utf8');
});


/**
 * Generate schema json file.
 */

function generateSchemaJson(schema, object) {

  console.log()
  console.log()
  console.log('>>>>>> inside the generate function...')
  console.log('schema:', schema);
  console.log('object:', object);
  console.log()
  console.log()

  var json = {};

  if (object && object.constructor === Array) {
    json.type = [ "null", "array" ];
    json.items = generateSchemaJson(schema, object[0])
  } else if (typeof object === 'object') {
    json.type = [ "null", "object" ];

    // Iterate through keys in object, generate props.
    var props = {};
    console.log('... iterating through the keys of the object.');
    console.log(object)

    Object.keys(object).forEach(function(key) {
      var value = object[key];

      console.log(key, value)

      // If datatype is an array.
      if (value && value.constructor === Array || typeof value === 'object') {
        props[key] = generateSchemaJson(key, value)
      } else {

        // If datatype already assigned
        var type = "string";
        if (datatypes.join(',').includes(value))
          type = value;
        else 
          type = getDataType(key, value)

        if (type === "date-time")
          props[key] = {
            type: [ "null", "string" ],
            format: "date-time"
          }
        else
          props[key] = {
            type: [ "null", type ]
          }
      }
    });

    json.properties = props;

  }
  
  return json;
}


/**
 * Get datatype given key and value.
 */

function getDataType(key, value) {
  // default everything is 'string'
  // then, it is number
  // then, it is integer
  // then, it is date-time
  // then, it is boolean
  // 
  // is it a Boolean?
  // is it a number?
  //   is it not a "price"? => integer
  //   else => number
  // is it date-time?
  // else => string

  if (typeof(value) === typeof(true)) return 'boolean';
  if (!isNaN(value)) return 'number';
  var date = moment(value);
  if (date.isValid()) return 'date-time';
  return 'string';
}