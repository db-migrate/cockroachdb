var Base = require('db-migrate-pg').base,
    pg = require('pg'),
    log,
    type,
    Promise = require('bluebird');

console.log(Base);

var CockroachDriver = Base.extend({

  init: function(connection, schema, intern) {

    this._super(connection, schema, intern);
  }
});


exports.connect = function(config, intern, callback) {

    log = intern.mod.log;
    type = intern.mod.type;

    if (config.native) { pg = pg.native; }
    var db = config.db || new pg.Client(config);
    callback(null, new PgDriver(db, config.database, intern));
};
