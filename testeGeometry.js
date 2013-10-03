'use strict';

var mysql = require('mysql');

var openStatements = 0;

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
	} else {
		var values = [
			1,
			"GeomFromText('POINT(3 4)')"
		];
		connection.query('INSERT INTO RawPoint SET idRawTrajectory = ?, the_geom = ?', values, function(err, result){
			if (err) throw err;

			connection.query('SELECT * FROM RawPoint', function(err, rows, fields) {
				if (err) throw err;

				console.log(rows);

				connection.end();
			});
		});
	}
});
