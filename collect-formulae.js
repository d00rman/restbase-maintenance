#!/usr/bin/env nodejs

'use strict';


const cassandra = require('cassandra-driver');
const P = require('bluebird');
const yaml = require('js-yaml');
const fs = P.promisifyAll(require('fs'));
const yargs = require('yargs');


const argv = yargs
    .usage('Usage: $0 [options]')
    .options('h', {alias: 'help'})
    .options('H', {
        alias: 'hostname',
        default: 'localhost',
        describe: 'Contact hostname',
        type: 'string'
    })
    .options('P', {
        alias: 'port',
        default: 9042,
        describe: 'Contact port number',
        type: 'number'
    })
    .options('u', {
        alias: 'username',
        default: 'cassandra',
        describe: 'Cassandra username',
        type: 'string'
    })
    .options('p', {
        alias: 'password',
        default: 'cassandra',
        describe: 'Cassandra password',
        type: 'string'
    })
    .options('f', {
        alias: 'file',
        default: 'formulae.yaml',
        describe: 'Output file name',
        type: 'string'
    })
    .options('o', {
        alias: 'hashonly',
        default: false,
        describe: 'Whether to output only the hashes of the formulae',
        type: 'boolean'
    })
    .argv;

if (argv.help) {
    yargs.showHelp();
    process.exit(0);
}

const host    = argv.hostname;
const port    = argv.port;
const contact = `${host}:${port}`;
const user    = argv.username;
const pass    = argv.password;
const file = argv.file;
const hashOnly = argv.hashonly;


/** Creates a single connection pool. */
function connect() {
    const client = new cassandra.Client({
        contactPoints: [ contact ],
        authProvider: new cassandra.auth.PlainTextAuthProvider(user, pass),
        sslOptions: { ca: '/dev/null' },
        promiseFactory: P.fromCallback,
        queryOptions: { consistency: cassandra.types.consistencies.localQuorum },
        localDataCenter: 'eqiad',
        datacenters: ['eqiad', 'codfw'],
        keyspace: 'system'
    });
    return client.connect().then(() => client);
}


function _nextPage(client, query, params, pageState, options) {
    return P.try(() => client.execute(query, params, {
        prepare: true,
        fetchSize: options.fetchSize || 5,
        pageState,
    }))
    .catch((err) => {
        if (!options.retries) {
            throw err;
        }
        options.retries--;
        return _nextPage(client, query, params, pageState, options);
    });
}


/**
 * Async-safe Cassandra query execution
 *
 * Client#eachRow in the Cassandra driver relies upon a synchronous callback
 * to provide back-pressure during paging; This function can safely execute
 * async callback handlers.
 *
 * @param   {object} cassandra-driver Client instance
 * @param   {string} CQL query string
 * @param    {array} CQL query params
 * @param   {object} options map
 * @param {function} function to invoke for each row result
 */
function eachRow(client, query, params, options, handler) {
    options.log = options.log || (() => {});
    const origOptions = Object.assign({}, options);
    function processPage(pageState) {
        options.retries = origOptions.retries;
        return _nextPage(client, query, params, pageState, options)
        .then((res) => P.try(() => {
            if (!res || !res.rows) { return P.resolve(); }
            return handler(res.rows);
        }).then(() => {
            if (!res || !res.pageState) {
                return P.resolve();
            } else {
                return processPage(res.pageState);
                // Break the promise chain, so that we don't hold onto a
                // previous page's memory.
                //process.nextTick(() => P.try(() => processPage(res.pageState)).catch((e) => {
                    // there's something going on, ignore 
                //}).then(() => resolve()));
            }
        }));
    }

    return processPage(null);
}


let count = 0;
let lastT = 0;
let cc;
let fd = fs.openSync(file, 'w');
return connect().then((client) => {
    cc = client;
    return eachRow(
        client,
        'SELECT key, value FROM "globaldomain_T_mathoid__ng_input".data',
        {},
        {
            retries: 10,
            fetchSize: 512,
            log: console.log
        },
        (rows) => {
            const toWrite = {};
            rows.forEach((row) => {
                if (hashOnly) {
                    toWrite[row.key] = 1;
                } else {
                    let value;
                    try {
                        value = JSON.parse(row.value);
                        toWrite[row.key] = value;
                    } catch(e) {}
                }
            });
            return fs.writeAsync(fd, yaml.dump(toWrite)).then(() => {
                Object.keys(toWrite).forEach((key) => { delete toWrite[key]; });
                count += rows.length;
                if (Math.floor(count / 10000) != lastT) {
                    lastT = Math.floor(count / 10000);
                    console.log(count);
                    return new P((resolve) => process.nextTick(() => resolve()));
                }
                return P.resolve();
            });
        }
    );
}).then(() => console.log(`Total count: ${count}`)).finally(() => { return fs.closeAsync(fd).then(() => cc && cc.shutdown()); });

