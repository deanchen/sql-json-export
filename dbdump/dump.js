var TEST = false

var mysql = require('mysql');
var async = require('async');
var fs = require('fs');
var _ = require('underscore');

var args = process.argv.splice(2);
var threads = args[0];
var worker = args[1];

var config = JSON.parse(fs.readFileSync('config.json', 'utf8'));
var writeStream = fs.createWriteStream('../dump.json', {'flags': 'a'});

var client = mysql.createClient({
    host: config.host,
    user: config.user,
    password: config.pass,
});

client.useDatabase(config.database);

var primary = config.tables.primary;
var seconary = config.tables.secondary;

var batchSize = 100000;
if (TEST) batchSize = 3
var queueSize = 2;

async.waterfall([
    disableQueryCache,
    totalRows,
    processQuery
], function(err, res) {
    console.log(worker, " complete");
    writeStream.end();
    client.end();
});


function processQuery(rows, cb1) {
    console.log(worker + ": processing " + rows + " rows");
    var startTime = new Date();
    var q = async.queue(function(task, cb2) {
            query(task.start, task.limit, function(err, res) {
                if (err) return console.log(err);
                res = res.map(processName); 

                var content = JSON.stringify(res).replace(/},{/g, '}\n{');
                content = content.substring(1, content.length - 1) + '\n';

                writeStream.write(content);
                var elapsed = ((new Date() - startTime)/(60 * 1000)); 

                var estimate = (rows/(task.start + batchSize) - 1) * elapsed 
                console.log(task.start + ": " + Math.round(estimate) + " minutes remaining");
                cb2(err, content);
            }); 
        },
        queueSize
    );
    q.drain = function() {
        cb1(null);
    }

    var tasks = [];
    var offset = worker * batchSize;
    
    if (TEST) rows = 20;
    for (var i = offset; i < rows; i += batchSize * threads) {
        tasks.push({start: i, limit: batchSize});
    }

    q.push(tasks);
}

function processName(row) {
    row.lname = row.lname || "";
    row.fname =row.fname || "";
    row.name = _.zip(row.lname.split('||'), row.fname.split('||'));
    delete row.lname;
    delete row.fname;
    return row;
}

function query(start, limit, cb) {
    client.query(
        "SELECT " +
        "    p.id," + 
        "    p.title," +  
        "    p.year," +  
        "    p.journal_id as jid," +
        "    GROUP_CONCAT(a.firstname SEPARATOR '||') as fname, " +
        "    GROUP_CONCAT(a.lastname SEPARATOR '||') as lname " +
        "FROM papers as p, paper_authors as a " +
        "WHERE p.id = a.paper_id GROUP BY p.id LIMIT " + start + ", " + limit,
        cb
    );
}

function totalRows(cb) {
    client.query(
        "SELECT COUNT('id') as rows FROM " + primary.name,
        function(err, res) {
            cb(err, res[0].rows);
        }
    );
}

function disableQueryCache(cb) {
    client.query(
        "SET SESSION query_cache_type = OFF;",
        function(err, res) {
	    if (err) return console.log(err);
            cb(err);
        }
    );
}

