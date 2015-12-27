#!/usr/bin/env node
var sqlserver = require('mssql');
var fs        = require('fs');
var argv      = require('minimist')(process.argv.slice(2));
var CLI       = require('clui');
var clc       = require('cli-color');
var Q         = require('q');
var ansi      = require('ansi-escape-sequences');
var mkdirp    = require('mkdirp');

Q.longStackSupport = true;

var doc = "Usage: sqlserver2json [options]\n"
        + "\n"
        + "  -h --help          Display this help\n"
        + "  --username USER    SQL Server username\n"
        + "  --password PASS    SQL Server password\n"
        + "  --database DBNAME  Database name\n"
        + "  --server   SERVER  Database host or IP address\n"
        + "  --port     PORT    Database port number\n";
        + "  --output   DIR     Directory to store json files\n";

if (argv['h'] === true || argv['help'] === true) {
  console.log(doc);
  process.exit(0);
}

var data = {};

var progressBar = new CLI.Progress(30);
var progressLine = new CLI.Line();

var overallProgressBar = new CLI.Progress(30);
var overallProgressLine = new CLI.Line();
overallProgressBar.padding = 2;

var tableProgressBar = new CLI.Progress(30);
var tableProgressLine = new CLI.Line();
tableProgressBar.padding = 2;

function drawProgress(progress) {
  tableProgress = Math.ceil(progress.progress * 100.0);
  overallProgress = Math.ceil(progress.overallProgress * 100.0);

  process.stdout.write(ansi.cursor.up(2));

  var overallProgressBar = new CLI.Progress(30);
  var overallProgressLine = new CLI.Line();
  overallProgressBar.padding = 2;

  var tableProgressBar = new CLI.Progress(30);
  var tableProgressLine = new CLI.Line();
  tableProgressBar.padding = 2;

  overallProgressLine.column(progress.tableName, 30, [clc.cyan]);
  overallProgressLine.column(overallProgressBar.update(overallProgress, 100));
  overallProgressLine.fill();
  overallProgressLine.output();

  tableProgressLine.column(progress.msg, 30, [clc.cyan]);
  tableProgressLine.column(tableProgressBar.update(tableProgress, 100));
  tableProgressLine.fill();
  tableProgressLine.output();
}

var spinner = new CLI.Spinner();

function getTables() {
  var deferred = Q.defer();
  spinner.start();
  spinner.message('Fetching list of tables');
  var query = "SELECT * FROM sysobjects WHERE xtype='U' ";
  var request = new sqlserver.Request();
  Q.ninvoke(request, "query", query)
  .then(function(recordset) {
    spinner.stop();
    console.log('Fetched list of tables');
    deferred.resolve(recordset);
  })
  .catch(function(err) {
    spinner.stop();
    deferred.reject(err);
  });
  return deferred.promise;
}

function getData(tableName) {
  var deferred = Q.defer();
  
  var query = "SELECT * FROM " + tableName;
  var request = new sqlserver.Request();
  Q.ninvoke(request, "query", query)
  .then(function(recordset) {
   deferred.resolve(recordset);
  });
  return deferred.promise;
} 

function getTableCount(tableName) {
  var query = 'SELECT COUNT(*) as totalRows FROM ' + tableName;
  var request = new sqlserver.Request();
  return request.query(query);
}

function connect(config) {
  var deferred = Q.defer();
  spinner.start();
  spinner.message('Connecting to database');
  Q.ninvoke(sqlserver, "connect", config)
  .then(function() {
    spinner.stop();
    console.log('Connected to database');
    deferred.resolve();
  })
  .catch(function(err) {
    spinner.stop();
    deferred.reject(err);
  });

  return deferred.promise;
}

function disconnect(options) {
  var deferred = Q.defer();
  spinner.start();
  spinner.message('Disconnecting from database');
  Q.ninvoke(sqlserver, "close")
  .then(function() {
    spinner.stop();
    console.log('Disconnected from database');
    deferred.resolve();
  })
  .catch(function(err) {
    spinner.stop();
    deferred.reject(err);
  });

  return deferred.promise;
}

function processTable(options, tableName) {
  var deferred = Q.defer();
  var totalRows = 0;
 
  deferred.notify({msg: 'Counting rows', amt: 0.0});
  getTableCount(tableName)
  .then(function(rows) {
    totalRows = rows[0]['totalRows'];
    deferred.notify({msg: 'Fetching data', amt: 0.0});
    return getData(tableName);
  })
  .then(function(records) {
    var writerDeferred = Q.defer();

    var jsonFileName = options['output'] + '/' + tableName + '.json';

    var wstream = fs.createWriteStream(jsonFileName);

    wstream.on('finish', function() {
      writerDeferred.resolve()
    });

    wstream.write('[{\n');
    recordCounter = 0;
    records[0].forEach(function(record) {
      var keys = [];
      Object.keys(record).forEach(function(key) {
        keys.push('  "' + key + '": "' + record[key] + '"');
      });
      wstream.write(keys.join(',\n'));
      recordCounter++;
      
      if (recordCounter < records.length) {
        wstream.write('},{');
      }
      deferred.notify({msg: 'Writing Data', amt: (recordCounter / records[0].length)});
    });

    wstream.end('}]');

    return writerDeferred;
  })
  .then(function() {
    deferred.resolve();
  });

  return deferred.promise;
}

function processTables(options, tables) {
  var deferred = Q.defer();
  var totalTables = tables[0].length;
  var tableCounter = 0;

  var promises = [];

  tables[0].forEach(function(table) {
    var promise = processTable(options, table.name)
    .then(function() {
      tableCounter = tableCounter + 1;
    })
    .progress(function(progress) {
      deferred.notify({tableName: table.name, progress: progress.amt, msg: progress.msg, overallProgress: (tableCounter / totalTables)});
    })

    promises.push(promise);
  });

  Q.all(promises)
  .then(function() {
    deferred.resolve();
  });
  return deferred.promise;
}

function makeDirs(dir) {
  return Q.nfcall(mkdirp, dir);
}

makeDirs(argv['output'])
.then(connect.bind(null, argv))
.then(getTables.bind(null, argv))
.then(processTables.bind(null, argv))
.then(disconnect.bind(null, argv))
.progress(drawProgress)
.fail(function() {
  sqlserver.close();
  console.log('== ERROR == ERROR == ERROR == ERROR == ERROR == ERROR ==');
  console.dir(arguments);
})
.catch(function() {
  console.log('== EXCEPTION == EXCEPTION == EXCEPTION == EXCEPTION ==');
  console.log(arguments);

  sqlserver.close();
});
