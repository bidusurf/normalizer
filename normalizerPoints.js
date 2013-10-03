'use strict';

var fs = require('fs');
var mysql = require('mysql');
var readline = require('readline');
var async = require('async');

var openStatements = 0;

var decreaseStatementsAndCloseIfRequired = function() {
	openStatements--;
	console.log(openStatements);
	if (openStatements === 0) {
		connection.end();
	}
};

var increaseStatements = function() {
	openStatements++;
	console.log(openStatements);
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

var parseTrajectories = function(objectId, baseDir, dataDir) {
	increaseStatements();
	var rawTrajectoryValues = {idObject: objectId};
	connection.query('INSERT INTO RawTrajectory SET ?', rawTrajectoryValues, function(err, result) {
  		if (err) throw err;

		var trajectoryFiles = fs.readdirSync(baseDir + '/' + dataDir + '/Trajectory');
		for (var i = trajectoryFiles.length - 1; i >= 0; i--) {

			var rd = readline.createInterface({
	    		input: fs.createReadStream(baseDir + '/' + dataDir +'/Trajectory/' + trajectoryFiles[i]),
	    		output: process.stdout,
	    		terminal: false
			});

			var lines = 0;
			rd.on('line', function(line) {
				if (lines < 6) {
					lines++;
					return;
				}

				var lineValues = line.split(',');
				var latitude = lineValues[0];
				var longitude = lineValues[1];
				var altitude = lineValues[2];
				var timestamp = new Date(lineValues[5] + ' ' + lineValues[6]);
				increaseStatements();
				var sql = 'INSERT INTO RawPoint SET idRawTrajectory = ' + result.insertId + 
					' the_geom = GeomFromText("POINT('+ latitude + ' ' + longitude + ')")' +
					' timestamp = ' + timestamp;
				connection.query(sql, function(err, result) {
		  			if (err) throw err;
			  		decreaseStatementsAndCloseIfRequired();
		  		});
			});

	  	}
		decreaseStatementsAndCloseIfRequired();
	});
};

var parseObjects = function (baseDir, dataDir) {
	increaseStatements();
	connection.query('SELECT * FROM Object WHERE name = ?', [dataDir], function(err, rows, fields) {
  		if (err) throw err;

  		if (rows.length > 0) {
	  		parseTrajectories(rows[0].idObject, baseDir, dataDir);

  			decreaseStatementsAndCloseIfRequired();
  		}
	});
};

var readAsDir = function (dirName) {
	var dataDir = fs.readdirSync(dirName);
	for (var i = dataDir.length - 1; i >= 0; i--) {
		parseObjects(dirName, dataDir[i]);
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
