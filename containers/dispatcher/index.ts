/**
 * We are suppressing some errors related to installed modules,
 * Since we will run everything in Docker, so no problem with the installed modules.
 *  */
// @ts-ignore
import { Kafka, logLevel } from 'kafkajs';

// @ts-ignore
const GROUP_ID = process.env.GROUP_ID || 'default-group';


// @ts-ignore
const BUCKET_SIZE = parseInt(process.argv[2]);

import {
   unboxKafkaMessage,
   newStreamEndedMessage,
   isStreamEnded,
   newMessageValue,
   STREAM_ENDED_KEY,
   bitwiseHash,
   parseSourceKey,
   getPipelineID,
   DISPATCHER_TOPIC,
   PIPELINE_UPDATE_TOPIC,
   MAP_TOPIC,
   SHUFFLE_TOPIC,
   REDUCE_TOPIC,
   OUTPUT_TOPIC,

   // @ts-ignore
} from "./utils";
// TODO ugly import

// @ts-ignore
import { createClient } from 'redis';


const kafka = new Kafka({
   clientId: 'mapreduce',
   brokers: ['kafka:9092'],
   logLevel: logLevel.NOTHING
});
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: GROUP_ID });

const redis = createClient({
   url: 'redis://redis:6379' // Redis URL matches the service name in docker-compose
})


const admin = kafka.admin();
const createTopic = async () => {
   await admin.connect();
   await admin.createTopics({
      topics: [
         // No parallelization for the first two topics
         {
            topic: PIPELINE_UPDATE_TOPIC,
            numPartitions: 1,
            replicationFactor: 1
         },
         {
            topic: DISPATCHER_TOPIC,
            numPartitions: 1,
            replicationFactor: 1
         },

         {
            topic: MAP_TOPIC,
            numPartitions: BUCKET_SIZE,
            replicationFactor: 1
         },
         {
            topic: SHUFFLE_TOPIC,
            numPartitions: BUCKET_SIZE,
            replicationFactor: 1
         },
         {
            topic: REDUCE_TOPIC,
            numPartitions: BUCKET_SIZE,
            replicationFactor: 1
         },
         {
            topic: OUTPUT_TOPIC,
            numPartitions: BUCKET_SIZE,
            replicationFactor: 1
         },
      ]
   });
   await admin.disconnect();
};


// Source mode: Reads files from a folder and sends messages to Kafka
async function dispatcherMode() {

   // if BUCKET_SIZE is not a positive integer or not provided, exit
   if (isNaN(BUCKET_SIZE) || BUCKET_SIZE <= 0) {
      console.error(`[ERROR] BUCKET_SIZE must be a positive integer`);
      // @ts-ignore
      process.exit(1);
   }

   await redis.connect();
   await redis.set('BUCKET_SIZE', BUCKET_SIZE);
   await redis.quit();

   await createTopic().catch(console.error);

   await producer.connect();
   await consumer.connect();
   await consumer.subscribe({ topic: DISPATCHER_TOPIC, fromBeginning: true });

   await consumer.run({
      eachMessage: async ({ message }) => {

         const { key, val } = unboxKafkaMessage(message);
         console.log(`[DISPATCHER] Received message ${key}`);

         const pipelineID = getPipelineID(key);
         if (pipelineID === null) return;

         if (isStreamEnded(message)) {
            // send to all partitions
            for (let i = 0; i < BUCKET_SIZE; i++) {
               await producer.send({
                  topic: MAP_TOPIC,
                  messages: [{
                     key: `${pipelineID}__${STREAM_ENDED_KEY}`,
                     value: JSON.stringify(newStreamEndedMessage(pipelineID, null)),
                     partition: i
                  }],
               });
            }
            return;
         }


         const { keyStr } = parseSourceKey(key);
         // check if keyStr can be converted to numer
         let k = parseInt(keyStr);
         if (isNaN(k)) {
            k = bitwiseHash(keyStr);
            console.error(`Key ${keyStr} is not a number, hashed it to ${k}`);
            return;
         }

         // index is used to compute the partition to send the message to
         const index = k % BUCKET_SIZE;

         // Send message to map
         await producer.send({
            topic: MAP_TOPIC,
            messages: [{ key: pipelineID + "__source-record__" + index, value: JSON.stringify(newMessageValue(val.data, pipelineID)) }]
         });
         console.log(`[DISPATCHER] Sent pipeline ${pipelineID} message to map`);
      },
   });

}





async function main() {
   await dispatcherMode();
}

main().catch((error) => {
   console.error(`[ERROR] ${error.message}`);
   // If error is that kafka does not host the topic, wait and retry
   if (error.message.includes('This server does not host this topic-partition')) {
      setTimeout(() => {
         main();
      }, 5000);
   }
});