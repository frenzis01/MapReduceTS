/**
 * We are suppressing some errors related to installed modules,
 * Since we will run everything in Docker, so no problem with the installed modules.
 *  */
// @ts-ignore
import fs from 'fs';
// @ts-ignore
import path from 'path';
// @ts-ignore
import { Kafka, logLevel, KafkaMessage } from 'kafkajs';

// @ts-ignore
const MODE = process.argv[2];
// @ts-ignore
const GROUP_ID = process.env.GROUP_ID || 'default-group';


const kafka = new Kafka({
   clientId: 'mapreduce',
   brokers: ['kafka:9092'],
   logLevel: logLevel.NOTHING
});
const consumer = kafka.consumer({ groupId: GROUP_ID || 'default-group' });


const OUTPUT_FOLDER = './output';

const OUTPUT_TOPIC = 'output-topic';

// Unique worker ID
const WORKER_ID = `worker-${Math.random().toString(36).substring(2, 15)}`;


// SINK mode: Writes reduced results to disk
async function outputMode() {
   console.log('[SINK MODE] Waiting for results to be written to output folder...');
   await consumer.connect();
   await consumer.subscribe({ topic: OUTPUT_TOPIC, fromBeginning: true });

   let counter = 0;
   console.log("Connected")
   consumer.run({
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         counter++;
         // console.log(`[${MODE}/${WORKER_ID}]`)
         if (!message.value || !message.key) return;
         const key = message.key?.toString();
         const value = JSON.parse(message.value.toString());

         if (key && value) {
            // Prepend the worker ID to allow multiple sinks to work simultaneously without operating on the same file
            const outputPath = path.join(OUTPUT_FOLDER, `${WORKER_ID}_${value.pipelineID}_result.txt`);
            fs.appendFileSync(outputPath, `${key}: ${value.data}\n`);
            
            if (counter % 350 == 0) {
               console.log(`[SINK MODE] Wrote ${counter} lines. Now wrote result: ${value.pipelineID}/${key}: ${value.data}`);
            }
         }
      },
   });
}

async function main() {
   if (MODE === '--output') {
      await outputMode();
   }
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