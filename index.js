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

  addForeignKey: function(tableName, referencedTableName, keyName, fieldMapping, rules, callback) {
    if(arguments.length === 5 && typeof(rules) === 'function') {
      callback = rules;
      rules = {};
    }
    var columns = Object.keys(fieldMapping);
    var referencedColumns = columns.map(function (key) { return '"' + fieldMapping[key] + '"'; });
    var sql = util.format('ALTER TABLE "%s" ADD CONSTRAINT "%s" FOREIGN KEY (%s) REFERENCES "%s" (%s)',
      tableName, keyName, this.quoteDDLArr(columns), referencedTableName, referencedColumns);
    return this.runSql(sql).nodeify(callback);
  },

  _handleMultiPrimaryKeys: function(primaryKeyColumns) {

    return util.format(', PRIMARY KEY (%s)',
      this.quoteDDLArr(primaryKeyColumns.sort(function(a, b) {

        if(a.spec.interleave && b.spec.interleave)
          return 0;

        return a.spec.interleave ? -1 : 1;
      }).map(function(value) {

        return value.name;
      })).join(', '));
  },

  _applyTableOptions: function(options) {

    var sql = '',
        interleave,
        interleaves = [],
        self = this;

    Object.keys(options).forEach(function(key) {

      var option = options[key];

      if(option.interleave) {

        if(typeof(option.interleave) === 'string') {

          if(interleave && interleave !== option.interleave) {

              this.log.warn('Ignoring interleave "' + interleave +
                '", you can only have one!');
          }
          else {

            interleave = option.interleave;
          }

          interleaves.push(key);
        }
      }
    });

    if(interleaves.length > 0) {

      sql = util.format(' INTERLEAVE IN PARENT %s (%s)',
        self.escapeDDL(interleave),
        self.quoteDDLArr(interleaves).join(', ')
      );
    }

    return sql;
  },

  _applyExtensions: function(options) {

    var families = {},
        firstFamily,
        indizies = {},
        sql = [],
        self = this;

    Object.keys(options).forEach(function(key) {

      var option = options[key];

      if(option.family && typeof(option.family) === 'string') {

        families[option.family] = families[option.family] || [];
        families[option.family].push(self.escapeDDL(key));

        if(option.primaryKey === true)
          firstFamily = option.family;
      }

      if(option.foreignKey && option.primaryKey !== true &&
         typeof(option.foreignKey) === 'object') {

        indizies[option.foreignKey.name] = indizies[option.foreignKey.name] || [];
        indizies[option.foreignKey.name].push(self.escapeDDL(key));
      }
    });

    Object.keys(indizies).forEach(function(key) {

      sql.push(util.format('INDEX %s (%s)',
        self.escapeDDL(key),
        indizies[key].join(', ')
      ));
    });

    if(firstFamily) {

      sql.push(util.format('FAMILY %s (%s)',
        self.escapeDDL(firstFamily),
        families[firstFamily].join(', ')
      ));
    }


    Object.keys(families).forEach(function(key) {

      if(key !== firstFamily) {

        sql.push(util.format('FAMILY %s (%s)',
          self.escapeDDL(key),
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
