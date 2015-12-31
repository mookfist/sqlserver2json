#!/usr/bin/env node
var sqlserver = require('mssql');
var fs        = require('fs');
var argv      = require('minimist')(process.argv.slice(2));
var CLI       = require('clui');
var clc       = require('cli-color');
var Q         = require('q');
var ansi      = require('ansi-escape-sequences');
var mkdirp    = require('mkdirp');
var jsonlint  = require('jsonlint');
var spawn     = require('child-process-promise').spawn;
Q.longStackSupport = true;

var doc = "Usage: sqlserver2json [options]\n"
        + "\n"
        + "  -h --help          Display this help\n"
        + "  --username USER    SQL Server username\n"
        + "  --password PASS    SQL Server password\n"
        + "  --database DBNAME  Database name\n"
        + "  --server   SERVER  Database host or IP address\n"
        + "  --port     PORT    Database port number\n"
        + "  --output   DIR     Directory to store json files\n"
        + "  --simple           Remove graphical widgets\n"
        + "  --timeout  SECS    Number of seconds to timeout\n"
        + "  --table    NAME    Dump a specific table name\n";
        + "  --test             Test JSON files for syntax errors after they are made\n";

if (argv['h'] === true || argv['help'] === true) {
  console.log(doc);
  process.exit(0);
}

if (argv['timeout']) {
  argv['connectionTimeout'] = parseInt(argv['timeout']) * 1000;
  argv['requestTimeout'] = parseInt(argv['timeout']) * 1000;
}

var data = {};

var prevReadProgress = 0;
var prevWriteProgress = 0;
var prevOverallProgress = 0;

function drawProgress(progress) {
  if (progress === null) {
    return;
  }
  var readProgress = Math.ceil(progress.readProgress * 100.0);
  var writeProgress = Math.ceil(progress.writeProgress * 100.0);
  var overallProgress = Math.ceil(progress.progress * 100.0);

  overallProgress = Math.ceil(progress.progress * 100.0);

  if (readProgress == prevReadProgress && overallProgress == prevOverallProgress && writeProgress == prevWriteProgress) {
    return;
  }

  prevOverallProgress = overallProgress;
  prevReadProgress = readProgress;
  prevWriteProgress = writeProgress;

  if (argv['simple'] === true) {
    console.log('Processing ' + progress.table + ' - ' + progress.msg + ' - r:' + readProgress + '%/w:' + writeProgress + '%/o:' + overallProgress + '%');
    return;
  }

  process.stdout.write(ansi.cursor.up(3));

  var overallProgressBar = new CLI.Progress(30);
  var overallProgressLine = new CLI.Line();
  overallProgressBar.padding = 2;

  var rowReadingBar = new CLI.Progress(30);
  var rowReadingLine = new CLI.Line();
  rowReadingBar.padding = 2;

  var rowWritingBar = new CLI.Progress(30);
  var rowWritingLine = new CLI.Line();
  rowWritingBar.padding = 2;

  overallProgressLine.column(progress.table + ' - ' + progress.msg, 50, [clc.cyan]);
  overallProgressLine.column(overallProgressBar.update(overallProgress, 100));
  overallProgressLine.fill();
  overallProgressLine.output();

  rowReadingLine.column('Reading', 50, [clc.cyan]);
  rowReadingLine.column(rowReadingBar.update(readProgress, 100));
  rowReadingLine.fill();
  rowReadingLine.output();

  rowWritingLine.column('Writing', 50, [clc.cyan]);
  rowWritingLine.column(rowWritingBar.update(writeProgress, 100));
  rowWritingLine.fill();
  rowWritingLine.output();
}

// var spinner = new CLI.Spinner();

function simpleCmd(msg, cb) {
  var spinner;
  if (argv['simple'] !== true) {
    spinner = new CLI.Spinner(msg + ' ... ');
    spinner.start();
  } else {
    process.stdout.write(msg + ' ... ');
  }

  var defer = Q.defer();

  return Q.fcall(cb)
  .then(function(result) {
    if (argv['simple'] !== true) {
      spinner.stop();
      process.stdout.write(msg + ' ... done!\n');
    } else {
      process.stdout.write('done!\n');
    }
    return result; 
    defer.resolve.apply(defer, arguments);
  })
  .fail(function(err) {
    
    if (argv['simple'] !== true) {
      spinner.stop();
      process.stdout.write(msg + ' ... failed!\n');
    } else {
      process.stdout.write('failed!\n');
    }

    throw err;
  });

  return defer.promise;
}

function getTables(options) {
  return simpleCmd('Feching list of tables', function() {
    if (options['table']) {
      return Q.fcall(function() {
        return [[{name: options['table']}]];
      });
    } else {
      var query = "SELECT * FROM sysobjects WHERE xtype='U'";
      var request = new sqlserver.Request();
      return Q.ninvoke(request, "query", query);
    }
  }).then(function(records) {
    return records[0];
  });
}

function connect(config) {
  return simpleCmd('Connecting to database', function() {
    return Q.ninvoke(sqlserver, 'connect', config);
  });
}

function disconnect(options) {
  return simpleCmd('Disconnecting from database', function() {
    return Q.ninvoke(sqlserver, "close");
  });
}

function getTableCount(tableName) {
  var query = 'SELECT COUNT(*) as totalRows FROM ' + tableName;
  var request = new sqlserver.Request();
  return Q.ninvoke(request, 'query', query)
  .then(function(results) {
    return results[0][0]['totalRows'];
  });
}

function getPrimaryKey(tableName) {
  var query = 'SELECT COLUMN_NAME '
            + 'FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE '
            + 'WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + \'.\' + CONSTRAINT_NAME), \'IsPrimaryKey\') = 1 '
            + 'AND TABLE_NAME = \'' + tableName + '\'';
  var request = new sqlserver.Request();
  return Q.ninvoke(request, 'query', query)
  .then(function(recordset) {
    var keys = [];
    recordset[0].forEach(function(row) {
      keys.push(row['COLUMN_NAME']);
    });
    return keys;
  });
}

function processTable(options, tableName) {
  var deferred = Q.defer();
  var jsonFileName = options['output'] + '/' + tableName + '.json';
  
  deferred.notify({table: tableName, msg: 'counting rows', progress: 0});

  var rowCounter = 0;
  var tableCount = 0;
  getTableCount(tableName)
  .then(function() {
    tableCount = arguments[0];
    deferred.notify({table: tableName, msg: 'getting primary key', progress: 0});
    return getPrimaryKey(tableName);
  })
  .then(function(pk) {
    var rowDeferral = Q.defer();
    var rowCounter = 0;
    var writerCounter = 0;
    var request = new sqlserver.Request();
    request.stream = true;

    var writeStream = fs.createWriteStream(jsonFileName);
    writeStream.on('close', function() {
      rowDeferral.resolve();
    });

    if (pk.length > 1) {
      writeStream.write('[{\n');
    } else {
      writeStream.write('{\n');
    }

    request.query('select * from ' + tableName);

    var indent = 0;
    var indentAmt = 2;

    var writtenKeys = {};
    request.on('row', function(row) {
      indent = indent + indentAmt;
      if (pk.length == 1) {
        var pkValue = row[pk[0]];
        writeStream.write(' '.repeat(indent) + '"' + pkValue + '": {\n');
        indent = indent + indentAmt;
      }

      var keys = [];
      
      Object.keys(row).forEach(function(key) {
        var value = '';
        if (typeof row[key] == "string") {
          value = row[key].replace(/\n/g, '\\n');
          value = value.replace(/\r/g, '\\r');
          value = value.replace(/\t/g, '\\t');
          value = '"' + value.replace(/"/g, '\\"').trim() + '"';
          
        } else {
          value = row[key];
        }
        keys.push(' '.repeat(indent) + '"' + key + '": ' + value);
      });
      var column = keys.join(',\n');
      writeStream.write(column, function() {
        writerCounter++;
        deferred.notify({table: tableName, msg: 'Written row', writeProgress: (writerCounter / tableCount), readProgress: (rowCounter / tableCount)});
      });
      
      rowCounter++;
      if (rowCounter != tableCount) {
        indent = indent - indentAmt;
        if (pk.length == 1) {
          writeStream.write('\n' + ' '.repeat(indent) + '},\n');
        } else {
          writeStream.write('\n' + ' '.repeat(indent) + '},{\n');
        }
      }
      deferred.notify({table: tableName, msg: 'Reading row', writeProgress: (writerCounter / tableCount), readProgress: (rowCounter / tableCount)});

      if (pk.length == 1) {
        indent = indent - indentAmt;
      }
    });

    request.on('done', function(row) {
      if (pk.length == 1) {
        writeStream.end('\n' + ' '.repeat(indent) + '}\n}');
      } else {
        writeStream.end('\n}]');
      }
    });

    return rowDeferral.promise;
  }).then(function() {
    deferred.resolve();
  }).fail(function(err) {
    deferred.reject(err);
  });
  return deferred.promise;
}

var totalTables, tableCounter;

function processTables(options, tables) {
  console.log('\n\n\n');
  var deferred = Q.defer();

  if (!totalTables) {
    totalTables = tables.length;
    tableCounter = 0;
  }
  
  var result = Q();
  tables.forEach(function(table) {
    result = result.then(processTable.bind(null, options, table.name)).then(function() {
      tableCounter++;
    });
  });
  
  return result.progress(function(progress) {
    if (!progress['readProgress']) {
      progress['readProgress'] = 0;
    }

    if (!progress['writeProgress']) {
      progress['writeProgress'] = 0;
    }

    progress['progress'] = tableCounter / totalTables;
    return progress;
  });
}

function makeDirs(dir) {
  return Q.nfcall(mkdirp, dir);
}

function testJsonFile(fn) {
  return simpleCmd('Testing ' + fn, function() {
    var args = [
      '--max-old-space-size=4096',
      'node_modules/jsonlint/lib/cli.js',
      fn
    ];
    return spawn('node', args, { capture: ['stdout', 'stderr']})
    .progress(function() {
      return null;
    });
  });
}

function testJsonFiles(dir) {
  return Q.nfcall(fs.readdir, dir)
  .then(function(results) {
    var result = Q();
    results.forEach(function(fn) {
      result = result.then(function() {
        return testJsonFile(dir + '/' + fn);
      });
    });
    return result;
  });
};

console.log('/------------------------------------------------\\');
console.log('| sqlserver2json                                 |');
console.log('| if you have very large tables, then go to the  |');
console.log('| pub, it will be a while.                       |');
console.log('\\------------------------------------------------/');
console.log(' ');

makeDirs(argv['output'])
.then(connect.bind(null, argv))
.then(getTables.bind(null, argv))
.then(processTables.bind(null, argv))
.then(function() {
  if (argv['test'] == true) {
    if (argv['table']) {
      return testJsonFile(argv['output'] + '/' + argv['table'] + '.json');
    } else {
      return testJsonFiles(argv['output']);
    }
  }
})
.then(disconnect.bind(null, argv))
.progress(drawProgress)
.fail(function(err) {
  console.log('== ERROR == ERROR == ERROR == ERROR == ERROR == ERROR ==');
  console.dir(err);
  console.log(err.stack);

  sqlserver.close();
})
.done();
