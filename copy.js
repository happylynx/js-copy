const fs = require('fs')
const path = require('path')
const util = require('util')

const pfs = {
    access: util.promisify(fs.access),
    close: util.promisify(fs.close),
    copyFile: util.promisify(fs.copyFile),
    fsync: util.promisify(fs.fsync),
    mkdir: util.promisify(fs.mkdir),
    open: util.promisify(fs.open),
    readdir: util.promisify(fs.readdir),
    stat: util.promisify(fs.stat)
}

const NUMBER_OF_CONCURRENT_IOS = 8

const pendingPromises = new Set()
const pendingFileRecords = [] // deep first search stack

/**
 * @param {Set<Promise<any>>} setOfPromises
 * @return {Promise<{ result: any, promise: Promise<any> }>} first resolved promise and value
 */
async function resoveOne(setOfPromises) {
    const wrappedPromises = Array.from(setOfPromises)
        .map(promise => promise.then(value => ({ value, promise })))
    return await Promise.race(wrappedPromises)
}

/**
 *
 * @param {string} globalSource
 * @param {string} source
 * @param {string} globalDestination
 * @return {string} destination
 */
function resolveDestination(globalSource, source, globalDestination) {
    const relativePath = path.relative(globalSource, source)
    const destination = path.resolve(globalDestination, relativePath)
    return destination
}

function getRecordType(stat) {
    if (stat.isBlockDevice()) {
        return 'block device'
    }
    if (stat.isCharacterDevice()) {
        return 'character device'
    }
    if (stat.isFIFO()) {
        return 'fifo'
    }
    if (stat.isSocket()) {
        return 'socker'
    }
    if (stat.isSymbolicLink()) {
        return 'symbolic link'
    }
    return 'unknown'
}

async function waifForOneIo() {
    const { promise } = await resoveOne(pendingPromises)
    pendingPromises.delete(promise)
}

async function copyFile(source, destination) {
    console.log('start', source, '->', destination)
    const { COPYFILE_EXCL } = fs.constants
    try {
        await pfs.copyFile(source, destination, COPYFILE_EXCL)
    } catch (error) {
        if (error.code === 'EEXIST') {
            console.warn('Skipping copy of ', source, '->', destination, 'Destination already exists.')
            return
        }
        throw error
    }
    const fd = await pfs.open(destination, 'r')
    await pfs.fsync(fd)
    await pfs.close(fd)
    console.log('done', source, '->', destination)
}

async function copyDirectory(source, destination) {
    console.log('start', source, '->', destination)
    try {
        await pfs.mkdir(destination)
    } catch (error) {
        if (error.code === 'EEXIST') {
            console.warn('Destination directory', destination, 'already exists.')
        } else {
            throw error
        }
    }
    const entries = (await pfs.readdir(source))
        .map(entry => path.resolve(source, entry))
    entries.forEach(entry => pendingFileRecords.push(entry))
    console.log('done', source, '->', destination)
}

async function main() {
    const globalSource = ''
    const globalDestination = ''

    pendingFileRecords.push(globalSource)
    while (pendingFileRecords.length > 0 || pendingPromises.size > 0) {
        // console.log('cycle start', pendingFileRecords, pendingPromises)
        if (pendingPromises.size >= NUMBER_OF_CONCURRENT_IOS || pendingFileRecords.length === 0) {
            await waifForOneIo()
        }
        if (pendingFileRecords.length === 0) {
            console.log('no file records on stack')
            continue
        }
        const source = pendingFileRecords.pop()
        let stat
        try {
            stat = await pfs.stat(source)
        } catch (statError) {
            console.error('error for entry=', source, 'error=', statError)
            continue
        }
        const destination = resolveDestination(globalSource, source, globalDestination)
        if (stat.isFile()) {
            // console.log('about to copy file', source)
            pendingPromises.add(copyFile(source, destination))
        } else if (stat.isDirectory()) {
            // console.log('about to copy dir', source)
            pendingPromises.add(copyDirectory(source, destination))
        } else {
            console.warn('Entry ', source, 'is of unexpected type', getRecordType(stat))
        }
    }
    console.log('while condition', pendingFileRecords.length > 0 || pendingPromises.size > 0)
    console.log('after cycle', pendingFileRecords, pendingPromises)
    console.log('copying done')
}

main()