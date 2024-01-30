const { redisClient } = require("./redisConnection");

const resultCachePrefix = 'result_cache:';
const processedTaskPrefix = 'processed_tasks:';

async function getProcessedTaskCount(workerNode) {
    const key = processedTaskPrefix + workerNode;

    return await new Promise((resolve, reject) => {
        redisClient.get(key, (err, result) => {
            if (err) {
                reject(err);
            } else {
                resolve(result ? parseInt(result, 10) : 0);
            }
        });
    });
}

async function incrementProcessedTaskCount(workerNode) {
    const key = processedTaskPrefix + workerNode;

    await new Promise((resolve, reject) => {
        redisClient.incr(key, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

async function cacheResult(taskId, result) {
    return new Promise((resolve, reject) => {
        redisClient.set(resultCachePrefix + taskId, result, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

async function getFromCache(taskId) {
    return new Promise((resolve, reject) => {
        redisClient.get(resultCachePrefix + taskId, (err, result) => {
            if (err) {
                reject(err);
            } else {
                resolve(result);
            }
        });
    });
}

module.exports = {
    getProcessedTaskCount,
    incrementProcessedTaskCount,
    cacheResult,
    getFromCache
}