var mysql = require('mysql');
var async = require('async');
var fs = require('fs');
var _ = require('underscore');

var config = JSON.parse(fs.readFileSync('config.json', 'utf8'));
var writeStream = fs.createWriteStream('output.json');

var client = mysql.createClient({
    host: config.host,
    user: config.user,
    password: config.pass,
});

client.useDatabase(config.database);

var primary = config.tables.primary;
var seconary = config.tables.secondary;

var batchSize = 100000;
var queueSize = 2;

async.waterfall([
    totalRows,
    process
], function(err, res) {
    console.log("complete");
    writeStream.end();
    client.end();
});


function process(rows, cb1) {
    console.log("Processing " + rows + " rows");
    writeStream.write("[");
    var q = async.queue(function(task, cb2) {
            query(task.start, task.limit, function(err, res) {
                if (err) return console.log(err);
                res = res.map(processName); 

                var content = JSON.stringify(res);
                content = content.substring(1, content.length - 1) + ',\n';
                if (task.last) content = content.substring(0, content.length - 2);

                writeStream.write(content);
                console.log("finished " + task.start);
                cb2(err, content);
            }); 
        },
        queueSize
    );
    q.drain = function() {
        writeStream.write("]");
        cb1(null);
    }

    var tasks = [];
    for (var i = 0; i < 20; i += batchSize) {
        tasks.push({start: i, limit: batchSize});
    }
    tasks[tasks.length - 1].last = true;

    q.push(tasks);
}

function processName(row) {
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

