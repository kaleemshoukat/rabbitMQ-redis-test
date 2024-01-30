const amqp = require('amqplib');

function getRandomInt(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

async function sender() {
    let connection;
    try {
        const queue = "tasks_queue";
        const task = {
            id: getRandomInt(1, 100),
            complexity: getRandomInt(1, 10),
        };

        connection = await amqp.connect("amqp://localhost");
        const channel = await connection.createChannel();

        await channel.assertQueue(queue, { durable: false });
        channel.sendToQueue(queue, Buffer.from(JSON.stringify(task)));

        console.log("Task Sent: ", task);

        await channel.close();
    } catch (err) {
        console.warn(err);
    } finally {
        if (connection) await connection.close();
    }
}

sender();