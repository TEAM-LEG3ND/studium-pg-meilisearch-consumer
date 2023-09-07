const { MeiliSearch } = require('meilisearch')
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
    sasl: {
        mechanism: 'scram-sha-256',
        username: 'USERNAME',
        password: 'PASSWORD',
    },
    ssl: true,
    brokers: ['BROKER1']
});

const consumer = kafka.consumer({ groupId: '$GROUP_NAME' });

const client = new MeiliSearch({
    host: 'HOST',
    apiKey: 'KEY'
});

const run = async() => {
    await consumer.connect();
    await consumer.subscribe({topic: 'TOPIC'});

    await consumer.run({
        eachMessage: async({topic, partition, message}) => {


            const parsedValue = JSON.parse(message.value);

            console.log({
                partition,
                offset: message.offset,
                value: parsedValue,
            });

            if (parsedValue !== null) {
                const operation = parsedValue.payload.op;
                const table = parsedValue.payload.source.table;

                const before = parsedValue.payload.before;
                const after = parsedValue.payload.after;

                const index = client.index(table);

                // assume that pk column name is always id

                switch (operation) {
                    case 'c':
                        const res = await index.addDocuments([after]);
                        console.log(res);
                        break;
                    case 'u':
                        const updRes = await index.addDocuments([after]);
                        console.log(updRes);
                        break;
                    case 'd':
                        const delRes = await index.deleteDocument(before.id);
                        console.log(delRes);
                        break;
                    default:
                }
            }
        }
    });
}

run().catch(console.error);
