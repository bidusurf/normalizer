'use strict';

var fs = require('fs');
var mysql = require('mysql');
var readline = require('readline');

var LABELS = 'labels.txt';
var openStatements = 0;

var decreaseStatementsAndCloseIfRequired = function() {
	openStatements--;
	if (openStatements === 0) {
		connection.end();
	}
};

var parseTimestamp = function(fromFile) {
	var splited = fromFile.split(' ');
	var dateSplited = splited[0].split('/');
	var timeSplited = splited[1].split(':');
	return new Date(dateSplited[0], 
					dateSplited[1] - 1,
					dateSplited[2], 
					timeSplited[0], 
					timeSplited[1], 
					timeSplited[2]);
};

var getTransportationMeanId = function(transportationMean, semanticSubTrajectoryValues, fn) {
	openStatements++;
	connection.query('SELECT * FROM TransportationMean WHERE description = ?', [transportationMean], function(err, rows, fields) {
  		if (err) throw err;

  		if (rows.length > 0) {
	  		semanticSubTrajectoryValues['idTransportationMean'] = rows[0].idTransportationMean;
	  		fn(semanticSubTrajectoryValues);
	  		decreaseStatementsAndCloseIfRequired();
  		} else {
			var transportationMeanValues = {description: transportationMean};
			connection.query('INSERT INTO TransportationMean SET ?', transportationMeanValues, function(err, result) {
  				if (err) throw err;

		  		semanticSubTrajectoryValues['idTransportationMean'] = result.insertId;
		  		fn(semanticSubTrajectoryValues);
		  		decreaseStatementsAndCloseIfRequired();
  			});
  		}
	});
};

var parseLabels = function(objectId, baseDir, dataDir) {
	openStatements++;
	var semanticTrajectoryValues = {idObject: objectId};
	connection.query('INSERT INTO SemanticTrajectory SET ?', semanticTrajectoryValues, function(err, result) {
  		if (err) throw err;

		var rd = readline.createInterface({
    		input: fs.createReadStream(baseDir + '/' + dataDir +'/' + LABELS),
    		output: process.stdout,
    		terminal: false
		});

		rd.on('line', function(line) {
			if (line.indexOf('Start') >= 0) {
				return;
			}

			var lineValues = line.split('\t');
			var start = new Date(lineValues[0]);
			var finish = new Date(lineValues[1]);
			var transportationMean = lineValues[2];
			openStatements++;
			var semanticSubTrajectoryValues = {
				idSemanticTrajectory: result.insertId,
				startTime: start,
				endTime: finish
			};
			
			getTransportationMeanId(transportationMean, semanticSubTrajectoryValues, function(semanticSubTrajectoryValues) {
				openStatements++;
				connection.query('INSERT INTO SemanticSubTrajectory SET ?', semanticSubTrajectoryValues, function(err, result) {
		  			if (err) throw err;
		  		});
		  		decreaseStatementsAndCloseIfRequired();
			});
		    // console.log(objectId, line);
		});

		decreaseStatementsAndCloseIfRequired();
  	});

};

var parseObjects = function (baseDir, dataDir) {
	openStatements++;
	connection.query('SELECT * FROM Object WHERE name = ?', [dataDir], function(err, rows, fields) {
  		if (err) throw err;

  		var objectId = 0;
		if (rows.length === 0) {
			var insValues = {name : dataDir};
			connection.query('INSERT INTO Object SET ?', insValues, function(err, result) {
		  		if (err) throw err;

		  		parseLabels(result.insertId, baseDir, dataDir);

		  		decreaseStatementsAndCloseIfRequired();
			});
		} else {

	  		parseLabels(rows[0].idObject, baseDir, dataDir);

	  		decreaseStatementsAndCloseIfRequired();
		}
	});
};

var readAsDir = function (dirName) {
	var dataDir = fs.readdirSync(dirName);
	for (var i = dataDir.length - 1; i >= 0; i--) {
		var anotherDir = fs.readdirSync(dirName + '/' + dataDir[i]);
		var indexOfLabels = anotherDir.lastIndexOf(LABELS);
		if (indexOfLabels >= 0) {
			parseObjects(dirName, dataDir[i]);
		}
	};
	
};

var connection = mysql.createConnection({
  host : 'localhost',
  database: 'tcc',
  user : 'pedro',
  password : 'bidu1',
  port: 3306
});

connection.connect(function (err) {
	if (err) {
		console.log(err);
	}
});

readAsDir('/home/pedro/desenvolvimento/Geolife Trajectories 1.2/Data');
