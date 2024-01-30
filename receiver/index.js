const amqp = require('amqplib');
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

async function processTask(task) {
    try {
        // Simulate time-consuming task processing
        await new Promise(resolve => setTimeout(resolve, task.complexity * 1000));

        const result = `Processed task ${task.id} with complexity ${task.complexity}`;
        console.log(result);

        // Cache the result in Redis
        redisClient.set(task.id, JSON.stringify(result));

        return result;
    } catch (error) {
        console.error('Error processing task:', error);
        throw error;
    }
}

async function startWorker() {
    try {
        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();
        const queue = 'tasks_queue';

        await channel.assertQueue(queue, { durable: false });

        console.log('Worker node waiting for tasks...');

        await channel.consume(queue, async (msg) => {
            try {
                const task = JSON.parse(msg.content.toString());
                console.log(`Received task: ${JSON.stringify(task)}`);

                // Check if result is cached
                const cachedResult = await new Promise(resolve => redisClient.get(task.id, (_, result) => resolve(result)));
                if (cachedResult) {
                    console.log(`Result already cached: ${cachedResult}`);
                } else {
                    const result = await processTask(task);
                    console.log(`Sending result back: ${result}`);
                }

                channel.ack(msg);
            } catch (error) {
                console.error('Error processing message:', error);
                channel.reject(msg, false); // Reject the message to remove it from the queue
            }
        });
    } catch (error) {
        console.error('Error connecting to RabbitMQ:', error);
        setTimeout(() => process.exit(1), 5000); // Exit the process after 5 seconds on error
    }
}

startWorker();
