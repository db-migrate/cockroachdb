var Base = require('db-migrate-pg').base,
    util = require('util'),
    pg = require('pg'),
    log,
    type,
    Promise = require('bluebird');

var CockroachDriver = Base.extend({

  init: function(connection, schema, intern) {

    this._super(connection, schema, intern);
  },

  _applyExtensions: function(options) {

    var families = {},
        firstFamily,
        sql = [];

    Object.keys(options).forEach(function(key) {

      var option = options[key];

      if(option.family && typeof(option.family) === 'string') {

        families[option.family] = families[option.family] || [];
        families[option.family].push(key);

        if(option.primaryKey === true)
          firstFamily = option.family;
      }
    });

    if(firstFamily) {

      sql.push(util.format('FAMILY %s (%s)',
        key,
        families[firstFamily].join(', ')
      ));
    }

    Object.keys(families).forEach(function(key) {

      if(key !== firstFamily) {

        sql.push(util.format('FAMILY %s (%s)',
          key,
          families[key].join(', ')
        ));
      }
    });

    if(sql.length === 0)
      return '';

    return ', ' + sql.join(', ');
  }
});


exports.connect = function(config, intern, callback) {

    if (config.native) { pg = pg.native; }
    var db = config.db || new pg.Client(config);
    callback(null, new CockroachDriver(db, config.database, intern));
};
