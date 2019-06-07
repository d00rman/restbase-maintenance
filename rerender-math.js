#!/usr/bin/env node

'use strict';


const P = require('bluebird');
const fs = P.promisifyAll(require('fs'));
const preq = require('preq');
const yargs = require('yargs');
const yaml = require('js-yaml');


const argv = yargs
    .usage('Usage: $0 <input> <restbase-url>')
    .options('h', {alias: 'help'})
    .options('s', {
        alias: 'start',
        describe: 'the formula to start with',
        default: '',
        type: 'string'
    })
    .options('c', {
        alias: 'concurrency',
        describe: 'the number of simultaneous requests',
        default: 20,
        type: 'number'
    })
    .argv;

if (argv.help || argv._.length < 2) {
    yargs.showHelp();
    process.exit(0);
}

const inFile = argv._[0];
const rbUri = `${argv._[1]}/media/math`;
const concurrency = argv.concurrency;
const start = argv.start;
let noHashes = 0;
let startTime;


function getFormula(hash) {
    return preq.get(`${rbUri}/formula/${hash}`).then(res => res.body);
}


function checkFormula(formula) {
    return preq.post({
        uri: `${rbUri}/check/${formula.type}`,
        headers: { 'content-type': 'application/json', 'cache-control': 'no-cache' },
        body: { q: formula.q },
        encoding: null
    }).thenReturn({ status: 200 });
}


function render(hash) {
    return preq.get({
        uri: `${rbUri}/render/svg/${hash}`,
        headers: { 'cache-control': 'no-cache' },
        encoding: null
    }).catch(e => {}).thenReturn(true);
}


return fs.readFileAsync(inFile, 'utf8').then(f => P.resolve(Object.keys(yaml.safeLoad(f)).sort()))
.then(hashes => {
    if (start) {
        hashes.splice(0, hashes.indexOf(start));
    }
    console.log('[*] Issuing requests...');
    startTime = Date.now();
    return new P((resolve) => process.nextTick(() => resolve(hashes)));
}).map(hash => getFormula(hash).then(formula => checkFormula(formula)).then(() => render(hash)).catchReturn(true).then(() => {
    noHashes++;
    if (noHashes % 10000 === 0) {
        console.log(`    - hash: ${hash} | time: ${Math.ceil((Date.now() - startTime) / 1000)} | count: ${noHashes}`);
        startTime = Date.now();
        return new P((resolve) => process.nextTick(() => resolve()));
    }
    return P.resolve();
}), { concurrency }).then(() => {
    console.log(`Total     : ${noHashes}`);
    return true;
});

