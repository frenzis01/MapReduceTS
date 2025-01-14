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
            topic: OUTPUT_TOPIC,
            numPartitions: BUCKET_SIZE,
            replicationFactor: 1
         },
      ]
   });
};


async function topicExists(topic: string): Promise<boolean> {
   const topics = await admin.listTopics();
   return topics.includes(topic);
};

async function addPipeline(pipelineID: string, rawValue: any) {
   console.log(`[DISPATCHER] 00 Received something ${JSON.stringify(rawValue)}`);
   await admin.createTopics({
      topics: [
         {
            topic: `${MAP_TOPIC}---${pipelineID}`,
            numPartitions: BUCKET_SIZE,
            replicationFactor: 1
         },
         {
            topic: `${SHUFFLE_TOPIC}---${pipelineID}`,
            numPartitions: BUCKET_SIZE,
            replicationFactor: 1
         },
         {
            topic: `${REDUCE_TOPIC}---${pipelineID}`,
            numPartitions: BUCKET_SIZE,
            replicationFactor: 1
         },
      ]
   });
   console.log(`[DISPATCHER] 10 Received something`);
   // TODO evaluate if there are multiple pipelineIDs
   await producer.send({
      topic: PIPELINE_UPDATE_TOPIC,
      messages: [{
         key: pipelineID,
         value: JSON.stringify(rawValue)
      }]
   })
   console.log(`[DISPATCHER] 20 Received ${pipelineID}`);
   // Sleep 10 seconds
   await new Promise(resolve => setTimeout(resolve, 2000));
}

// Source mode: Reads files from a folder and sends messages to Kafka
async function dispatcherMode() {
   // const knownPipelines = new Set<string>();
   const knownPipelines: { [key: string]: boolean } = {};
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
         // console.log(`[DISPATCHER] knownPipelines: ${JSON.stringify(knownPipelines)}`)

         const pipelineID = getPipelineID(key);
         if (pipelineID === null) return;

         // if pipeline is not known add it and create topics
         // TODO check whether val truly holds a pipeline
         // Ideally the first message of a pipeline should always be the
         // stringified version of pipelineID, but you may never know.
         if (!knownPipelines[pipelineID]) {
            knownPipelines[pipelineID] = true;
            await addPipeline(pipelineID, val);
            return;
         }

         if (isStreamEnded(message)) {
            // send to all partitions
            for (let i = 0; i < BUCKET_SIZE; i++) {
               await producer.send({
                  topic: `${MAP_TOPIC}---${pipelineID}`,
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
            topic: `${MAP_TOPIC}---${pipelineID}`,
            messages: [{ 
               key: pipelineID + "__source-record__" + index, 
               value: JSON.stringify(newMessageValue(val.data, pipelineID)), 
               // partition: index 
            }]
         });
         console.log(`[DISPATCHER] Sent record:${index} to ${MAP_TOPIC}---${pipelineID}`);
      },
   });

}





async function main() {
   await admin.connect();
   await dispatcherMode();
   await admin.disconnect();
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