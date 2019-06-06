#!/usr/bin/env node

'use strict';


const P = require('bluebird');
const fs = P.promisifyAll(require('fs'));
const preq = require('preq');
const yargs = require('yargs');
const yaml = require('js-yaml');


const argv = yargs
    .usage('Usage: $0 <input> <old-restbase-url> <new-restbase-url>')
    .options('h', {alias: 'help'})
    .argv;

if (argv.help || argv._.length < 3) {
    yargs.showHelp();
    process.exit(0);
}

const inFile = argv._[0];
const oldRbUri = `${argv._[1]}/wikimedia.org/v1/media/math`;
const newRbUri = `${argv._[2]}/wikimedia.org/v1/media/math`;
let noHashes;
const errorRetrieving = {};
const notFound = [];
const checkFail = {};
const oldFail = {};
const newFail = {};


function checkFormula(formula) {
    return preq.post({
        uri: `${newRbUri}/check/${formula.type}`,
        headers: { 'content-type': 'application/json', 'cache-control': 'no-cache' },
        body: { q: formula.formula }
    }).thenReturn({ status: 200 })
    .catch((e) => {
        checkFail[formula.hash] = {
            formula: formula.formula,
            type: formula.type,
            error: e.body ? e.body.detail || e.body.message || e.message : `${e}`
        };
        return {};
    })
}


function checkRender(formula) {
    return preq.get({
        uri: `${newRbUri}/render/svg/${formula.hash}`,
        headers: { 'cache-control': 'no-cache' }
    }).catch((ee) => {
        if(ee.status == 404 || ee.status == 504) {
            return preq.get({
                uri: `${newRbUri}/render/svg/${formula.hash}`,
                headers: { 'cache-control': 'no-cache' }
            });
        }
        throw ee;
    }).catch((e) => {
        return preq.get(`${oldRbUri}/render/svg/${formula.hash}`)
        .catch((ee) => {
            if(ee.status == 404 || ee.status == 504) {
                return preq.get({
                    uri: `${oldRbUri}/render/svg/${formula.hash}`,
                    headers: { 'cache-control': 'no-cache' }
                });
            }
            throw ee;
        }).then((res) => {
            newFail[formula.hash] = {
                formula: formula.formula,
                type: formula.type,
                error: e.body ? e.body.detail || e.body.message || e.message : `${e}`
            };
        }).catch((e2) => {
            oldFail[formula.hash] = {
                formula: formula.formula,
                type: formula.type,
                error: e.body ? e.body.detail || e.body.message || e.message : `${e}`
            };
        });
    }).thenReturn(true);
}


return fs.readFileAsync(inFile, 'utf8')
.then(contents => P.resolve(contents.split("\n").filter(x => x.length))).tap((hashes) => {
    noHashes = hashes.length;
    console.log(`[*] Getting ${noHashes} formulae ...`);
}).map(hash => preq.get(`${oldRbUri}/formula/${hash}`)
    .catch({ status: 504 }, () => preq.get(`${oldRbUri}/formula/${hash}`))
    .then((res) => { return {hash, type: res.body.type, formula: res.body.q}; })
    .catch((e) => {
        if(e.status == 404) {
            notFound.push(hash);
        } else {
            errorRetrieving[hash] = { status: e.status, error: e.body ? e.body.detail || e.body.message || e.message : `${e}` };
        }
        return undefined;
    }
), { concurrency: 64 }).filter(x => x).tap(() => console.log('[*] Issuing check and render requests ...'))
.map((formula) => checkFormula(formula)
    .then((res) => {
        if(!res.status) {
            return P.resolve(true);
        }
        return checkRender(formula);
    })
, { concurrency: 24 }).tap(() => console.log('[*] Writing the results to disk ...'))
.then(
    () => fs.writeFileAsync('retrieve.yaml', yaml.dump({ not_found: notFound || [], retrieve_fail: errorRetrieving || {} }))
).then(
    () => fs.writeFileAsync('check.yaml', yaml.dump(checkFail))
).then(
    () => fs.writeFileAsync('render_new.yaml', yaml.dump(newFail))
).then(
    () => fs.writeFileAsync('render_both.yaml', yaml.dump(oldFail))
).then(() => {
    let noRetrieve = notFound.length + Object.keys(errorRetrieving).length;
    let noCheck = Object.keys(checkFail).length;
    let noNew = Object.keys(newFail).length;
    let noBoth = Object.keys(oldFail).length;
    let noSuccess = noHashes - noRetrieve - noCheck - noNew - noBoth;
    console.log(`Total     : ${noHashes}`);
    console.log(`Success   : ${noSuccess}`);
    console.log(`Retrieve  : ${noRetrieve}`);
    console.log(`Check     : ${noCheck}`);
    console.log(`New fail  : ${noNew}`);
    console.log(`Both fail : ${noBoth}`);
    return true;
});

