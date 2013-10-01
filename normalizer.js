'use strict';
var LABELS = 'labels.txt';
var fs = require('fs');
var mysql = require('mysql');
var objects = 0;

var parseLabels = function (baseDir, dataDir) {
	objects++;
	connection.query('SELECT * FROM Object WHERE name = ?', [dataDir], function(err, rows, fields) {
  		if (err) throw err;

		if (rows.length === 0) {
			var insValues = {name : dataDir};
			connection.query('INSERT INTO Object SET ?', insValues, function(err, result) {
		  		if (err) throw err;

		  		console.log(result.insertId);
		  		objects--;
		  		if (objects === 0) {
		  			connection.end();
		  		}
			});
		} else {
	  		objects--;
	  		if (objects === 0) {
	  			connection.end();
	  		}

		}
	});
	var fileData = fs.readFileSync(baseDir + '/' + dataDir + '/' + LABELS);
};

var readAsDir = function (dirName) {
	var dataDir = fs.readdirSync(dirName);
	for (var i = dataDir.length - 1; i >= 0; i--) {
		var anotherDir = fs.readdirSync(dirName + '/' + dataDir[i]);
		var indexOfLabels = anotherDir.lastIndexOf(LABELS);
		if (indexOfLabels >= 0) {
			parseLabels(dirName, dataDir[i]);
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
