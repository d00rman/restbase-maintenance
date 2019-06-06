#!/usr/bin/env nodejs

'use strict';


const cassandra = require('cassandra-driver');
const P         = require('bluebird');
const preq      = require('preq');

const yargs     = require('yargs');
const argv      = yargs
    .usage('Usage: $0 <restbase-url>')
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
    .argv;

if (argv.help || !argv._[0]) {
    yargs.showHelp();
    process.exit(0);
}

const host    = argv.hostname;
const port    = argv.port;
const rbUri   = `${argv._[0]}/wikimedia.org/v1/media/math`;
const contact = `${host}:${port}`;
const user    = argv.username;
const pass    = argv.password;


/** Creates a single connection pool. */
function connect() {
    const client = new cassandra.Client({
        contactPoints: [ contact ],
        authProvider: new cassandra.auth.PlainTextAuthProvider(user, pass),
        sslOptions: { ca: '/dev/null' },
        promiseFactory: P.fromCallback,
        queryOptions: { consistency: cassandra.types.consistencies.one },
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
        .then((res) => P.try(() => P.map(res.rows, row => handler(row), { concurrency: 32 }))
        .then(() => {
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
let cc;
let startTime = Date.now();
return connect().then((client) => {
    cc = client;
    return eachRow(
        client,
        'SELECT key, value FROM "local_group_globaldomain_T_mathoid_input".data',
        {},
        {
            retries: 10,
            fetchSize: 512,
            log: console.log
        },
        (row) => {
            let value;
            try {
                value = JSON.parse(row.value);
            } catch(e) {
                console.error(`${row.key}: Cannot parse value - ${e.message}`);
                return P.resolve();
            }
            return P.try(() => preq.post({
                uri: `${rbUri}/check/${value.type}`,
                headers: { 'content-type': 'application/json' },
                body: { q: value.q },
                encoding: null
            }).then((res) => preq.get({
                uri: `${rbUri}/render/svg/${res.headers['x-resource-location']}`,
                encoding: null
            }))).catch((e) => {
                console.error(`(${count}) ${row.key}: Error while requesting: ${e.message}`);
            }).then(() => {
                value = undefined;
                count++;
                if(count % 10000 === 0) {
                    console.log(`- ${count}\t${(Date.now() - startTime) / 1000.0}`);
                    startTime = Date.now();
                }
                if(count % 128 === 0) {
                    return new P((resolve) => process.nextTick(() => resolve()));
                }
                return P.resolve();
            });
        }
    );
}).then(() => console.log(`Total count: ${count}`)).finally(() => cc.shutdown());

