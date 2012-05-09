var TEST = false;

var mysql = require('db-mysql');
var async = require('async');
var fs = require('fs');
var _ = require('underscore');

var args = process.argv.splice(2);
var threads = args[0];
var worker = args[1];

var config = JSON.parse(fs.readFileSync('config.json', 'utf8'));
var writeStream = fs.createWriteStream('../dump.json', {'flags': 'a'});

var db = new mysql.Database({
    hostname: config.host,
    user: config.user,
    password: config.pass,
    database: config.database
});

var primary = config.tables.primary;
var seconary = config.tables.secondary;

var batchSize = 100000;
if (TEST) batchSize = 3
var queueSize = 2;

async.waterfall([
    dbConnect,
    disableQueryCache,
    totalRows,
    processQuery
], function(err, res) {
    console.log("worker " + worker + " complete");
    writeStream.end();
    db.disconnect();
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

                var overloaded  = !writeStream.write(content);
                var elapsed = ((new Date() - startTime)/(60 * 1000)); 

                var estimate = (rows/(task.start + batchSize) - 1) * elapsed 
                console.log(task.start + ": " + Math.round(estimate) + " minutes remaining");
                
                if (overloaded) {
                    writeStream.once('drain', function() {
                        cb2(err);
                    }); 
                } else {
                    cb2(err);
                }
            }); 
        },
        queueSize
    );
    q.drain = function() {
        cb1(null);
    }

    var tasks = [];
    var offset = worker * batchSize;
    
    if (TEST) rows = 1000;
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
    db.query(
        "SELECT " +
        "    p.id," + 
        "    p.title," +  
        "    p.year," +  
        "    p.journal_id as jid," +
        "    GROUP_CONCAT(a.firstname SEPARATOR '||') as fname, " +
        "    GROUP_CONCAT(a.lastname SEPARATOR '||') as lname " +
        "FROM papers as p " + 
        "LEFT JOIN paper_authors as a " +
        "ON p.id = a.paper_id " +
        "WHERE p.id >= " + start + " " +
        "AND p.id < " + (start + limit) + " " +
        "GROUP BY p.id"
    ).execute(cb);
}

function dbConnect(cb) {
    db.connect(function(err) {
        if (err) return console.log(err);
        cb(err);
    });
}


function totalRows(cb) {
    db.query(
        "SELECT COUNT('id') as rows FROM " + primary.name
    ).execute(function(err, res) {
        cb(err, res[0].rows);
    });
}

function disableQueryCache(cb) {
    db.query(
        "SET SESSION query_cache_type = OFF;"
    ).execute(function(err, res) {
        if (err) return console.log(err);
        cb(err);
    });
}
