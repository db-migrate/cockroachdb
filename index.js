const Base = require('db-migrate-pg').base;
const util = require('util');
let pg = require('pg');
const Promise = require('bluebird');

function dummy () {
  arguments[arguments.length - 1]('not implemented');
}

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
      rules = {};
    }
    var columns = Object.keys(fieldMapping);
    var referencedColumns = columns.map(function (key) {
      return '"' + fieldMapping[key] + '"';
    });
    var sql = util.format(
      'ALTER TABLE "%s" ADD CONSTRAINT "%s" FOREIGN KEY (%s) REFERENCES "%s" (%s) ON DELETE %s ON UPDATE %s',
      tableName,
      keyName,
      this.quoteDDLArr(columns).join(', '),
      referencedTableName,
      referencedColumns.join(', '),
      rules.onDelete || 'NO ACTION',
      rules.onUpdate || 'NO ACTION'
    );
    return this.runSql(sql).nodeify(callback);
  },

  _prepareSpec: function (columnName, spec, options, tableName) {
    ['defaultValue', 'onUpdate'].forEach(c => {
      if (spec[c]) {
        if (spec[c].raw) {
          spec[c].prep = spec[c].raw;
        } else if (spec[c].special) {
          this._translateSpecialDefaultValues(
            spec[c],
            options,
            tableName,
            columnName
          );
        }
      }
    });
  },

  _translateSpecialDefaultValues: function (
    spec,
    options,
    tableName,
    columnName
  ) {
    switch (spec.special) {
      case 'CURRENT_TIMESTAMP':
        spec.prep = 'CURRENT_TIMESTAMP()';
        break;
      case 'NOW':
        spec.prep = 'NOW()';
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

    // ToDo: need flag from db-migrate to skip this after we finally
    // deprecate it
    // we might even ask during config for the cockroachdb version
    if (interleaves.length > 0) {
      this.log.warn(
        'Interleaving has been deprecated in the latest cockroachdb releases. ' +
          'Make sure to remove them later if you did not already!'
      );
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
    let options = {};
    this._prepareSpec(columnName, columnSpec, {}, tableName);
    if (typeof callback === 'object') {
      options = callback;
      callback = null;
    }

    return setNotNull.call(this).nodeify(callback);

    function setNotNull () {
      if (columnSpec.notNull === undefined) {
        return setUnique.call(this);
      }
      var setOrDrop = columnSpec.notNull === true ? 'SET' : 'DROP';
      var sql = util.format(
        'ALTER TABLE "%s" ALTER COLUMN "%s" %s NOT NULL',
        tableName,
        columnName,
        setOrDrop
      );

      return this.runSql(sql).then(setUnique.bind(this));
    }

    function setUnique () {
      var sql;
      var constraintName = tableName + '_' + columnName + '_key';

      if (columnSpec.unique === true) {
        sql = util.format(
          'ALTER TABLE "%s" ADD CONSTRAINT "%s" UNIQUE ("%s")',
          tableName,
          constraintName,
          columnName
        );
        return this.runSql(sql).then(setDefaultValue.bind(this));
      } else if (columnSpec.unique === false) {
        sql = util.format(
          'DROP INDEX "%s"@"%s" CASCADE',
          tableName,
          constraintName
        );
        return this.runSql(sql).then(setDefaultValue.bind(this));
      } else {
        return setDefaultValue.call(this);
      }
    }

    function setDefaultValue () {
      var sql;

      if (columnSpec.defaultValue !== undefined) {
        var defaultValue = null;
        if (typeof columnSpec.defaultValue === 'string') {
          defaultValue = "'" + columnSpec.defaultValue + "'";
        }
        if (columnSpec.defaultValue.prep) {
          defaultValue = columnSpec.defaultValue.prep;
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
      return this.runSql(sql).then(setOnUpdate.bind(this));
    }

    function setOnUpdate () {
      let sql;

      if (columnSpec.onUpdate !== undefined) {
        let onUpdate = null;
        if (typeof columnSpec.onUpdate === 'string') {
          onUpdate = "'" + columnSpec.onUpdate + "'";
        }
        if (columnSpec.onUpdate.prep) {
          onUpdate = columnSpec.onUpdate.prep;
        } else {
          onUpdate = columnSpec.onUpdate;
        }
        sql = util.format(
          'ALTER TABLE "%s" ALTER COLUMN "%s" SET ON UPDATE %s',
          tableName,
          columnName,
          onUpdate
        );
      } else {
        sql = util.format(
          'ALTER TABLE "%s" ALTER COLUMN "%s" DROP ON UPDATE',
          tableName,
          columnName
        );
      }
      return this.runSql(sql).then(setType.bind(this));
    }

    async function setType () {
      // no changes are possible afterwards in cockroachdb currently
      let sql = '';
      if (columnSpec.type !== undefined) {
        sql = util.format(
          'ALTER TABLE "%s" ALTER COLUMN "%s" TYPE %s%s',
          tableName,
          columnName,
          columnSpec.type,
          columnSpec.length ? `(${columnSpec.length})` : ''
        );
      }

      if (sql === '') {
        return Promise.resolve();
      } else {
        return this.runSql(sql);
      }
    }
  },

  mapDataType: function (str) {
    str = str.toLowerCase();
    switch (str) {
      case 'float':
      case 'timestamptz':
      case 'uuid':
      case 'jsonb':
      case 'enum':
        return str;
      case 'computed':
        return '';
    }
    return this._super(str);
  },

  createColumnDef: function (name, spec, options, tableName) {
    // add support for datatype timetz, timestamptz
    // https://www.postgresql.org/docs/9.5/static/datatype.html
    spec.type = spec.type.replace(/^(time|timestamp)tz$/, function ($, type) {
      spec.timezone = true;
      return type;
    });
    var type =
      spec.primaryKey && spec.autoIncrement
        ? ''
        : this.mapDataType(spec.type, spec);

    if (type === 'enum') {
      type = this.escapeDDL(spec.enumName);
    }

    var len = spec.length ? util.format('(%s)', spec.length) : '';
    var constraint = this.createColumnConstraint(
      spec,
      options,
      tableName,
      name
    );
    if (name.charAt(0) !== '"') {
      name = '"' + name + '"';
    }

    return {
      foreignKey: constraint.foreignKey,
      callbacks: constraint.callbacks,
      constraints: [name, type, len, constraint.constraints].join(' ')
    };
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
        constraint.push(this.escapeString(spec.defaultValue));
      } else if (spec.defaultValue.prep) {
        constraint.push(spec.defaultValue.prep);
      } else {
        constraint.push(spec.defaultValue);
      }
    }

    // available from cockroachdb v21.2
    if (spec.onUpdate !== undefined) {
      constraint.push('ON UPDATE');
      if (typeof spec.onUpdate === 'string') {
        constraint.push(this.escapeString(spec.onUpdate));
      } else if (spec.onUpdate.prep) {
        constraint.push(spec.onUpdate.prep);
      } else {
        constraint.push(spec.onUpdate);
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

  changePrimaryKey: function (table, newPrimary, callback) {
    return this.runSql(
      util.format(
        'ALTER TABLE %s DROP CONSTRAINT "primary", ADD PRIMARY KEY (%s)',
        this.escapeDDL(table),
        this.quoteDDLArr(newPrimary).join(', ')
      )
    ).nodeify(callback);
  },

  createEnum: function (name, definition, callback) {
    return this.runSql(
      util.format(
        'CREATE TYPE %s AS ENUM (%s)',
        this.escapeDDL(name),
        this.quoteArr(definition).join(', ')
      )
    ).nodeify(callback);
  },

  addEnumType: function (name, value, callback) {
    return this.runSql(
      util.format(
        'ALTER TYPE %s ADD VALUE %s',
        this.escapeDDL(name),
        this.escapeString(value)
      )
    ).nodeify(callback);
  },

  dropEnumType: function (name, value, callback) {
    return this.runSql(
      util.format(
        'ALTER TYPE %s DROP VALUE %s',
        this.escapeDDL(name),
        this.escapeString(value)
      )
    ).nodeify(callback);
  },

  dropEnum: function (name, callback) {
    return this.runSql(
      util.format('DROP TYPE %s', this.escapeDDL(name))
    ).nodeify(callback);
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
  },

  learnable: {
    changePrimaryKey: function (t, n) {
      if (this.schema[t]) {
        const _n = Object.keys(this.schema[t]).reduce((o, x) => {
          const c = this.schema[t][x];
          if (c.primaryKey === true) {
            delete c.primaryKey;
            o.push(x);
          }

          return o;
        }, []);
        for (const col of n) {
          if (!this.schema[t][col]) {
            throw new Error(`There is no ${col} column in schema!`);
          }
          this.schema[t][col].primaryKey = true;
        }

        this.modC.push({ t: 0, a: 'changePrimaryKey', c: [t, _n] });
      }

      return Promise.resolve();
    },

    createEnum: function (n, v) {
      if (!this.types) {
        this.types = {};
      }

      if (this.types[n] && this.types[n].t !== 'ENUM') {
        throw new Error(
          `This ENUM "${n}" already exists and collides with the ` +
            `type "${this.types[n].t}"`
        );
      }

      this.types[n] = { t: 'ENUM', v: JSON.parse(JSON.stringify(v)) };

      this.modC.push({ t: 0, a: 'dropEnum', c: [n, v] });

      return Promise.resolve();
    },

    addEnumType: function (n, v) {
      if (!this.types) {
        this.types = {};
      }

      // if (!this.types[n]) {
      //  throw new Error(`There is no such ENUM "${n}"`);
      // }

      // this.types[n].v.push(v);

      this.modC.push({ t: 0, a: 'dropEnumType', c: [n, v] });
    },

    dropEnumType: function (n, v) {
      if (!this.types) {
        this.types = {};
      }

      // if (!this.types[n]) {
      //  throw new Error(`There is no such ENUM "${n}"`);
      // }

      // delete this.types[n].v[this.types[n].v.findIndex(x => x === v)];

      this.modC.push({ t: 0, a: 'addEnumType', c: [n, v] });
    },

    dropEnum: function (n) {
      // if (!this.types[n]) {
      //  throw new Error(`There is no such ENUM "${n}"`);
      // }

      // const v = this.types[n].v;
      // delete this.types[n];

      this.modC.push({ t: 0, a: 'createEnum', c: [n, v] });

      return Promise.resolve();
    }
  },

  statechanger: {
    changePrimaryKey: function () {
      return this._default();
    },

    dropEnum: function () {
      return this._default();
    },

    createEnum: function () {
      return this._default();
    },

    addEnumType: function () {
      return this._default();
    },

    dropEnumType: function () {
      return this._default();
    }
  },

  _meta: {
    supports: { optionParam: true, columnStrategies: true }
  }
});

exports.connect = function (config, intern, callback) {
  if (config.native) {
    pg = pg.native;
  }
  var db = config.db || new pg.Client(config);

  intern.interfaces.MigratorInterface.changePrimaryKey = dummy;
  intern.interfaces.MigratorInterface.dropEnum = dummy;
  intern.interfaces.MigratorInterface.createEnum = dummy;
  intern.interfaces.MigratorInterface.addEnumType = dummy;
  intern.interfaces.MigratorInterface.dropEnumType = dummy;

  db.connect(function (err) {
    if (err) {
      callback(err);
    }
    callback(null, new CockroachDriver(db, config.database, intern));
  });
};
