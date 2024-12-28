/**
 * We are suppressing some errors related to installed modules,
 * Since we will run everything in Docker, so no problem with the installed modules.
 *  */
// @ts-ignore
import fs from 'fs';
// @ts-ignore
import path from 'path';
// @ts-ignore
import { Kafka, logLevel, KafkaMessage, Partitioners } from 'kafkajs';

// @ts-ignore
const MODE = process.argv[2];

import {
   MessageType,
   MessageValue,
   unboxKafkaMessage,
   newStreamEndedMessage,
   isStreamEnded,
   newMessageValue,
   newMessageValueShuffled,
   PipelineConfig,
   stringifyPipeline,
   STREAM_ENDED_KEY
} from './utils';
// TODO ugly import

const kafka = new Kafka({
   clientId: 'mapreduce',
   brokers: ['kafka:9092'],
   logLevel: logLevel.NOTHING
});
const producer = kafka.producer();

const admin = kafka.admin();
const createTopic = async () => {
   await admin.connect();
   await admin.createTopics({
      topics: [{
         topic: 'map-topic',
         numPartitions: BUCKET_SIZE, // Specify the number of partitions
         replicationFactor: 1
      },
      {
         topic: 'shuffle-topic',
         numPartitions: BUCKET_SIZE, // Specify the number of partitions
         replicationFactor: 1
      }
      ]
   });
   await admin.disconnect();
};


const INPUT_FOLDER = './input';

const PIPELINE_UPDATE_TOPIC = 'pipeline-updates';
const MAP_TOPIC = 'map-topic';

const BUCKET_SIZE = 10;

const pipelineWordCount: PipelineConfig = {
   pipelineID: 'word-count',
   keySelector: (message: any) => message.word,
   mapFn: (value: any) => {
      console.log(`[MAP MODE] Mapping type of value: ${typeof value}:${JSON.stringify(value)}`);
      // Filter is used to avoid having empty strings "" in the array	
      const words = value.split(/[^a-zA-Z0-9]+/).filter(Boolean);
      return words.map((word: string) => ({ word, count: 1 }));
   },
   reduceFn: (key: string, values: any[]) => {
      return values.reduce((acc, curr) => acc + curr.count, 0);
   },
}

// Source mode: Reads files from a folder and sends messages to Kafka
async function sourceMode() {
   await createTopic().catch(console.error);

   // await new Promise(f => setTimeout(f, 1000));
   console.log('[SOURCE MODE] Monitoring input folder...');
   await producer.connect();
   console.log('[SOURCE MODE] Connected to producer...');

   const pipelinesProducer = kafka.producer();
   await pipelinesProducer.connect();

   // TODO make this customizable
   // TODO move to another container
   console.log(`[SOURCE MODE] Sending pipelineID: ${JSON.stringify(pipelineWordCount.pipelineID)}`);
   await pipelinesProducer.send({
      topic: PIPELINE_UPDATE_TOPIC,
      messages: [{ value: stringifyPipeline(pipelineWordCount) }],
   });
   console.log(`[SOURCE MODE] Sent pipelineID: ${JSON.stringify(pipelineWordCount.pipelineID)}`);

   const processFile = async (filePath: string) => {
      console.log(`[SOURCE MODE] Processing file: ${filePath}/${fs.existsSync(filePath)}`);
      if (fs.existsSync(filePath)) {
         console.log(`[SOURCE MODE] Found new file: ${filePath}`);
         const fileContent = fs.readFileSync(filePath, 'utf-8');
         const data = fileContent.split('\n')
         const pipelineID = pipelineWordCount.pipelineID;

         let shuffled: { [key: string]: string[] } = {};

         data.forEach((record: any, index: number) => {
            console.log(`[SOURCE MODE] type of record : ${typeof record} ${index}`);
            producer.send({
               topic: MAP_TOPIC,
               // We add an index to the key as a reference for partitioning
               // we specify the partition to send the message to
               // note that this is an upper bound on the parallelism degree.
               messages: [{
                  key: pipelineID + "__source-record__" + index % BUCKET_SIZE,
                  value: JSON.stringify(newMessageValue(record, pipelineID)),
                  partition: (index) % 3
               }],
            });
            console.log(`[SOURCE MODE] Sent record: ${JSON.stringify(record)}`);
            // Compute locally and sequentially
            const results = pipelineWordCount.mapFn(record);
            results.forEach((v) => {
               const key = pipelineWordCount.keySelector(v);
               if (!shuffled[key]) {
                  shuffled[key] = [];
               }
               shuffled[key].push(v);

            });
            if (index % 10 === 0) {
            }
         });

         const reduced = Object.keys(shuffled).map((key) => {
            console.log(`[SOURCE MODE] Reducing: ${key}`);
            return [key, pipelineWordCount.reduceFn(key, shuffled[key])];
         });

         reduced.forEach(async ([key, value]) => {
            console.log(`[SOURCE MODE] Sending reduced: ${key}: ${value}`);
            await producer.send({
               topic: 'output-topic',
               messages: [{
                  key: key,
                  value: JSON.stringify(newMessageValueShuffled(value, "seq-word-count")),
               }],
            });
            console.log(`[SOURCE MODE] Sent reduced: ${key}: ${value}`);
         });


         // Send to shuffle consumer special value to start feeding the reduce
         console.log(`[SOURCE MODE] Sending stream ended message to MAP...`);
         // send on all partitions
         for (let i = 0; i < BUCKET_SIZE; i++) {
            await producer.send({
               topic: MAP_TOPIC,
               messages: [{
                  key: `${pipelineID}__${STREAM_ENDED_KEY}`,
                  value: JSON.stringify(newStreamEndedMessage(pipelineID, data.length)),
                  partition: i
               }],
            });
         }
      }
   }

   let files = fs.readdirSync(INPUT_FOLDER);
   console.log(`[SOURCE MODE] Found ${files.length} existing files in ${INPUT_FOLDER}`);
   files.forEach(async (file: any) => {
      console.log(`[SOURCE MODE] Processing existing file: ${file} in ${INPUT_FOLDER}`);
      const filePath = path.join(INPUT_FOLDER, file);
      await processFile(filePath);
   });

}





async function main() {
   await sourceMode();
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