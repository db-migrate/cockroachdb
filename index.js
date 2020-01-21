const Base = require('db-migrate-pg').base;
const util = require('util');
let pg = require('pg');
const Promise = require('bluebird');

var CockroachDriver = Base.extend({
  init: function (connection, schema, intern) {
    this._super(connection, schema, intern);
  },

  addForeignKey: function (
    tableName,
    referencedTableName,
    keyName,
    fieldMapping,
    rules,
    callback
  ) {
    if (arguments.length === 5 && typeof rules === 'function') {
      callback = rules;
      // rules = {};
    }
    var columns = Object.keys(fieldMapping);
    var referencedColumns = columns.map(function (key) {
      return '"' + fieldMapping[key] + '"';
    });
    var sql = util.format(
      'ALTER TABLE "%s" ADD CONSTRAINT "%s" FOREIGN KEY (%s) REFERENCES "%s" (%s)',
      tableName,
      keyName,
      this.quoteDDLArr(columns),
      referencedTableName,
      referencedColumns
    );
    return this.runSql(sql).nodeify(callback);
  },

  _translateSpecialDefaultValues: function (
    spec,
    options,
    tableName,
    columnName
  ) {
    switch (spec.defaultValue.special) {
      case 'CURRENT_TIMESTAMP':
        spec.defaultValue.prep = 'CURRENT_TIMESTAMP()';
        break;
      default:
        this.super(spec, options, tableName, columnName);
        break;
    }
  },

  _handleMultiPrimaryKeys: function (primaryKeyColumns) {
    return util.format(
      ', PRIMARY KEY (%s)',
      this.quoteDDLArr(
        primaryKeyColumns
          .sort(function (a, b) {
            if (a.spec.interleave && b.spec.interleave) return 0;

            return a.spec.interleave ? -1 : 1;
          })
          .map(function (value) {
            return value.name;
          })
      ).join(', ')
    );
  },

  _applyTableOptions: function (options) {
    const columns = options.columns || options;
    const interleaves = [];
    const self = this;
    let sql = '';
    let interleave;

    Object.keys(columns).forEach(function (key) {
      var option = columns[key];

      if (option.interleave) {
        if (typeof option.interleave === 'string') {
          if (interleave && interleave !== option.interleave) {
            this.log.warn(
              'Ignoring interleave "' + interleave + '", you can only have one!'
            );
          } else {
            interleave = option.interleave;
          }

          interleaves.push(key);
        }
      }
    });

    if (interleaves.length > 0) {
      sql = util.format(
        ' INTERLEAVE IN PARENT %s (%s)',
        self.escapeDDL(interleave),
        self.quoteDDLArr(interleaves).join(', ')
      );
    }

    return sql;
  },

  _applyExtensions: function (options) {
    const columns = options.columns || options;
    const families = {};
    const indizies = {};
    const sql = [];
    const self = this;
    let firstFamily;

    Object.keys(columns).forEach(function (key) {
      var option = columns[key];

      if (option.family && typeof option.family === 'string') {
        families[option.family] = families[option.family] || [];
        families[option.family].push(self.escapeDDL(key));

        if (option.primaryKey === true) firstFamily = option.family;
      }

      if (option.foreignKey && typeof option.foreignKey === 'object') {
        indizies[option.foreignKey.name] =
          indizies[option.foreignKey.name] || [];
        indizies[option.foreignKey.name].push(self.escapeDDL(key));
      }
    });

    Object.keys(indizies).forEach(function (key) {
      sql.push(
        util.format(
          'INDEX %s (%s)',
          self.escapeDDL(key),
          indizies[key].join(', ')
        )
      );
    });

    if (firstFamily) {
      sql.push(
        util.format(
          'FAMILY %s (%s)',
          self.escapeDDL(firstFamily),
          families[firstFamily].join(', ')
        )
      );
    }

    Object.keys(families).forEach(function (key) {
      if (key !== firstFamily) {
        sql.push(
          util.format(
            'FAMILY %s (%s)',
            self.escapeDDL(key),
            families[key].join(', ')
          )
        );
      }
    });

    if (sql.length === 0) return '';

    return ', ' + sql.join(', ');
  },

  changeColumn: function (tableName, columnName, columnSpec, callback) {
    return setNotNull.call(this);

    function setNotNull () {
      // in cockroacdb you cannot add a null value afterwards
      if (columnSpec.notNull === true) {
        return setUnique.call(this);
      }

      var setOrDrop = 'DROP';
      var sql = util.format(
        'ALTER TABLE "%s" ALTER COLUMN "%s" %s NOT NULL',
        tableName,
        columnName,
        setOrDrop
      );

      return this.runSql(sql).nodeify(setUnique.bind(this));
    }

    function setUnique (err) {
      if (err) {
        return Promise.reject(err);
      }

      var sql;
      var constraintName = tableName + '_' + columnName + '_key';

      if (columnSpec.unique === true) {
        sql = util.format(
          'ALTER TABLE "%s" ADD CONSTRAINT "%s" UNIQUE ("%s")',
          tableName,
          constraintName,
          columnName
        );
        return this.runSql(sql).nodeify(setDefaultValue.bind(this));
      } else if (columnSpec.unique === false) {
        sql = util.format(
          'DROP INDEX "%s"@"%s" CASCADE',
          tableName,
          constraintName
        );
        return this.runSql(sql).nodeify(setDefaultValue.bind(this));
      } else {
        return setDefaultValue.call(this);
      }
    }

    function setDefaultValue (err) {
      if (err) {
        return Promise.reject(err).nodeify(callback);
      }

      var sql;

      if (columnSpec.defaultValue !== undefined) {
        var defaultValue = null;
        if (typeof columnSpec.defaultValue === 'string') {
          defaultValue = "'" + columnSpec.defaultValue + "'";
        } else {
          defaultValue = columnSpec.defaultValue;
        }
        sql = util.format(
          'ALTER TABLE "%s" ALTER COLUMN "%s" SET DEFAULT %s',
          tableName,
          columnName,
          defaultValue
        );
      } else {
        sql = util.format(
          'ALTER TABLE "%s" ALTER COLUMN "%s" DROP DEFAULT',
          tableName,
          columnName
        );
      }
      return this.runSql(sql)
        .then(setType.bind(this))
        .nodeify(callback);
    }

    function setType () {
      // no changes are possible afterwards in cockroachdb currently
      return Promise.resolve();
    }
  },

  mapDataType: function (str) {
    str = str.toLowerCase();
    switch (str) {
      case 'float':
      case 'TIMESTAMPTZ':
      case 'uuid':
      case 'jsonb':
        return str;
      case 'computed':
        return '';
    }
    return this._super(str);
  },

  createColumnConstraint: function (spec, options, tableName, columnName) {
    var constraint = [];
    var callbacks = [];
    var cb;
    let type = spec.type;

    // this must keep first as it wilkl replace against mapDataType
    if (type.toLowerCase() === 'computed') {
      type = `${this.mapDataType(spec.computedType)} AS (${spec.function})`;
      if (spec.stored) {
        type += ' STORED';
      }
      constraint.push(type);
    }

    if (spec.primaryKey) {
      if (spec.autoIncrement) {
        if (this.mapDataType(spec.type) === 'uuid') {
          constraint.push('UUID');
          spec.defaultValue = { prep: 'gen_random_uuid()' };
        } else {
          constraint.push('SERIAL');
        }
      }

      if (options.emitPrimaryKey) {
        constraint.push('PRIMARY KEY');
      }
    }

    if (spec.timezone) {
      constraint.push('WITH TIME ZONE');
    }

    if (spec.notNull === true) {
      constraint.push('NOT NULL');
    }

    if (spec.unique) {
      constraint.push('UNIQUE');
    }

    if (spec.defaultValue !== undefined) {
      constraint.push('DEFAULT');
      if (typeof spec.defaultValue === 'string') {
        constraint.push("'" + spec.defaultValue + "'");
      } else if (spec.defaultValue.raw) {
        constraint.push(spec.defaultValue.raw);
      } else if (spec.defaultValue.prep) {
        constraint.push(spec.defaultValue.prep);
      } else {
        constraint.push(spec.defaultValue);
      }
    }

    // keep foreignKey for backward compatiable, push to callbacks in the future
    if (spec.foreignKey) {
      cb = this.bindForeignKey(tableName, columnName, spec.foreignKey);
    }
    if (spec.comment) {
      // TODO: create a new function addComment is not callable from here
      callbacks.push(
        function (tableName, columnName, comment, callback) {
          var sql = util.format(
            "COMMENT on COLUMN %s.%s IS '%s'",
            tableName,
            columnName,
            comment
          );
          return this.runSql(sql).nodeify(callback);
        }.bind(this, tableName, columnName, spec.comment)
      );
    }

    return {
      foreignKey: cb,
      callbacks: callbacks,
      constraints: constraint.join(' ')
    };
  },

  removeIndex: function (tableName, indexName, callback) {
    var sql;
    if (arguments.length === 2 && typeof indexName === 'function') {
      callback = indexName;
      indexName = tableName;
      tableName = null;
    } else if (arguments.length === 1 && typeof tableName === 'string') {
      indexName = tableName;
      tableName = null;
    }

    if (tableName) {
      sql = util.format('DROP INDEX "%s"@"%s"', tableName, indexName);
    } else {
      sql = util.format('DROP INDEX "%s"', indexName);
    }

    return this.runSql(sql).nodeify(callback);
  },

  addIndex: function (tableName, indexName, columns, options, callback) {
    let unique = options === true;
    let inverted = '';

    if (typeof options === 'function') {
      callback = options;
    } else if (typeof options === 'object') {
      if (options.unique) unique = options.unique;
      if (options.inverted) inverted = 'INVERTED';
    }

    if (!Array.isArray(columns)) {
      columns = [columns];
    }
    var columnString = columns
      .map(column => {
        if (typeof column === 'object') {
          return (
            this.escapeDDL(column.name) +
            (column.DESC === true ? ' DESC' : ' ASC')
          );
        } else {
          return this.escapeDDL(column);
        }
      })
      .join(', ');

    var sql = util.format(
      'CREATE %s %s INDEX "%s" ON "%s" (%s)',
      unique ? 'UNIQUE' : '',
      inverted,
      indexName,
      tableName,
      columnString
    );

    return this.runSql(sql).nodeify(callback);
  },

  createMigrationsTable: function (callback) {
    var options = {
      columns: {
        id: {
          type: this.type.INTEGER,
          notNull: true,
          primaryKey: true,
          autoIncrement: true
        },
        name: { type: this.type.STRING, length: 255, notNull: true },
        run_on: { type: this.type.DATE_TIME, notNull: true }
      },
      ifNotExists: false
    };

    return this.all(
      "SELECT table_name FROM information_schema.tables WHERE table_name = '" +
        this.internals.migrationTable +
        "'" +
        (this.schema ? " AND table_catalog = '" + this.schema + "'" : '') +
        " AND table_schema = 'public'"
    )
      .then(
        function (result) {
          if (result && result.length < 1) {
            return this.createTable(this.internals.migrationTable, options);
          } else {
            return Promise.resolve();
          }
        }.bind(this)
      )
      .nodeify(callback);
  }
});

exports.connect = function (config, intern, callback) {
  if (config.native) {
    pg = pg.native;
  }
  var db = config.db || new pg.Client(config);

  db.connect(function (err) {
    if (err) {
      callback(err);
    }
    callback(null, new CockroachDriver(db, config.database, intern));
  });
};
