'use strict';

var fs = require('fs');
var mysql = require('mysql');
var readline = require('readline');
var async = require('async');

var conns = 0;
var points = 0;
var processingObject = false;

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

var instPointFunction = function(trajectoryId, baseDir, trajectoryFiles, idx, err, insertPointConn) {
		  			if (err) throw err;
					var rd = readline.createInterface({
			    		input: fs.createReadStream(baseDir + trajectoryFiles[idx]),
			    		output: process.stdout,
			    		terminal: false
					});

					rd.on('close', function(){
						if (idx == trajectoryFiles.length - 1) {
					  		insertPointConn.destroy();
						}
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
						var timestamp = lineValues[5] + ' ' + lineValues[6];

						console.log(++points);
						var sql = 'INSERT INTO RawPoint SET idRawTrajectory = ' + trajectoryId + 
							', the_geom = GeomFromText("POINT('+ latitude + ' ' + longitude + ')")' +
							', timestamp = "' + timestamp + '"';
						insertPointConn.query(sql, function(err, result) {
				  			if (err) throw err;
				  		});
					});
				};

var parseTrajectories = function(objectId, baseDir, dataDir) {
	console.log(++conns);
	pool.getConnection(function(err, insertTrajectoryConn) {
  		if (err) throw err;

		var rawTrajectoryValues = {idObject: objectId};
		insertTrajectoryConn.query('INSERT INTO RawTrajectory SET ?', rawTrajectoryValues, function(err, result) {
	  		if (err) throw err;

	  		insertTrajectoryConn.destroy();
			var trajectoryFiles = fs.readdirSync(baseDir + '/' + dataDir + '/Trajectory');
			for (var i = 0; i < trajectoryFiles.length; i++) {

				console.log(++conns);
				var fileName = trajectoryFiles[i];
				var idx = i;
				pool.getConnection(instPointFunction.bind(undefined, result.insertId, baseDir + '/' + dataDir +'/Trajectory/', trajectoryFiles, idx));
		  	}
		});
	});
};

var parseObjects = function (baseDir, dataDir) {
	console.log(++conns);
	pool.getConnection(function(err, selectObjectConn) {
  		if (err) throw err;

		selectObjectConn.query('SELECT * FROM Object WHERE name = ?', [dataDir], function(err, rows, fields) {
	  		if (err) throw err;

	  		selectObjectConn.destroy();
	  		if (rows.length > 0) {
		  		parseTrajectories(rows[0].idObject, baseDir, dataDir);
	  		}
		});
	});
};

var readAsDir = function (dirName) {
	var dataDir = fs.readdirSync(dirName);
	for (var i = dataDir.length - 1; i >= 0; i--) {
		while (processingObject);
		processingObject = true;
		parseObjects(dirName, dataDir[i]);
	};
	
};

var pool = mysql.createPool({
  host : 'localhost',
  database: 'tcc',
  user : 'pedro',
  password : 'bidu1',
  port: 3306
});

parseObjects('/home/pedro/desenvolvimento/Geolife Trajectories 1.2/Data', process.argv[2]);
// readAsDir('/home/pedro/desenvolvimento/Geolife Trajectories 1.2/Data');
