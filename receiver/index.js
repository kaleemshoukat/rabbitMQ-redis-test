const amqp = require('amqplib');
const {
    getFromCache,
    cacheResult,
    getProcessedTaskCount,
    incrementProcessedTaskCount
} = require("./helper");

async function startSupervisor() {
    const taskQueue = 'tasks_queue';
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    await channel.assertQueue(taskQueue, { durable: false });

    console.log('Supervisor is running. Waiting for tasks...');

    await channel.consume(taskQueue, async (msg) => {
        const task = JSON.parse(msg.content.toString());
        console.log(`Received task: ${JSON.stringify(task)}`);

        // Check if result is cached
        const cachedResult = await getFromCache(task.id);
        if (cachedResult) {
            console.log(`Result already cached: ${cachedResult}`);
        } else {
            // Distribute task to workers
            const result = await distributeTask(task);

            // Cache the result
            await cacheResult(task.id, result);

            console.log(`Result cached: ${result}`);
        }

        channel.ack(msg);
    });
}

async function distributeTask(task) {
    const workerNodes = ['localhost'];

    // Get the number of tasks processed by each worker
    const tasksProcessed = await Promise.all(workerNodes.map(async (workerNode) => {
        const processedTaskCount = await getProcessedTaskCount(workerNode);
        return { workerNode, processedTaskCount };
    }));

    // Sort workers by the number of processed tasks in ascending order
    const sortedWorkers = tasksProcessed.sort((a, b) => a.processedTaskCount - b.processedTaskCount);

    // Select the worker with the least processed tasks
    const selectedWorker = sortedWorkers[0].workerNode;

    // Send the task to the selected worker
    const result = await sendMessageToWorker(selectedWorker, task);

    // Update the processed task count for the selected worker
    await incrementProcessedTaskCount(selectedWorker);

    return result;
}

async function sendMessageToWorker(workerNode, task) {
    const connection = await amqp.connect(`amqp://${workerNode}`);
    const channel = await connection.createChannel();

    await channel.assertQueue('', { exclusive: true });     //auto unique queue name
    const queue = channel.queue;

    // Send task to worker
    await channel.sendToQueue(queue, Buffer.from(JSON.stringify(task)));

    // Wait for the result
    const resultMessage = await new Promise(resolve => {
        channel.consume(queue, msg => {
            resolve(msg.content.toString());
            channel.ack(msg);
        }, { noAck: false });
    });

    connection.close();

    return resultMessage;
}

startSupervisor();
