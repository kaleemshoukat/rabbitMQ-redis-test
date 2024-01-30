const redis = require('redis');

const redisClient = redis.createClient({
    host: 'localhost',
    port: 6379,
});

redisClient.ping((err, result) => {
    if (err) {
        console.error('Error connecting to Redis:', err);
    } else {
        console.log('Connected to Redis:', result);
    }
});

module.exports = {
    redisClient
};