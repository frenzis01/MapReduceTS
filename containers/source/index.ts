/**
 * We are suppressing some errors related to installed modules,
 * Since we will run everything in Docker, so no problem with the installed modules.
 *  */
// @ts-ignore
import fs from 'fs';
// @ts-ignore
import path from 'path';
// @ts-ignore
import { Kafka, logLevel } from 'kafkajs';

// @ts-ignore
const MODE = process.argv[2];

import {
   newStreamEndedMessage,
   newMessageValue,
   newMessageValueShuffled,
   PipelineConfig,
   stringifyPipeline,
   STREAM_ENDED_KEY,
   DISPATCHER_TOPIC,
   OUTPUT_TOPIC,
   bitwiseHash
   // @ts-ignore
} from "./utils";

const kafka = new Kafka({
   clientId: 'mapreduce',
   brokers: ['kafka:9092'],
   logLevel: logLevel.NOTHING
});
const producer = kafka.producer();


const INPUT_FOLDER = './input';

// Pipeline implementing the typical word-count example
const createPipelineWordCount = (name: string): PipelineConfig => {
   if (name.includes('_')) {
      name = bitwiseHash(name).toString();
   };
   // Add random suffix of 6 chars to avoid conflicts for files with the same name
   const suffix = Math.random().toString(16).substring(2, 8);
   name = `${name}${suffix}`

   return {

      pipelineID: 'word-count-' + name,
      keySelector: (message: any) => message.word,
      dataSelector: (message: any) => message.count,
      mapFn: (key: string = "", value: any) => {
         // Filter is used to avoid having empty strings "" in the array	
         const words = value.split(/[^a-zA-Z0-9]+/).filter(Boolean);
         // Return an array with a single element, which is an array of (word, 1)
         return words.map((word: string) => ([{ word, count: 1 }])); //
      },
      reduceFn: (key: string, values: any[]) => {
         // Reduce takes as input a key and an array of `data` (count)
         return values.reduce((acc, count) => acc + count, 0);
      }
   }
};


// Source mode: Reads files from a folder and sends messages to Kafka
async function sourceMode() {

   console.log('[SOURCE MODE] Monitoring input folder...');
   await producer.connect();
   console.log('[SOURCE MODE] Producer connected to Kafka...');


   const pipelinesProducer = kafka.producer();
   await pipelinesProducer.connect();
   
   // function invoked for each file insise input folder
   const processFile = async (filePath: string) => {
      console.log(`[SOURCE MODE] Processing file: ${filePath}/${fs.existsSync(filePath)}`);
      // if file exists and is .txt
      if (fs.existsSync(filePath) && fs.lstatSync(filePath).isFile() && filePath.endsWith('.txt') 
      ) {
         console.log(`[SOURCE MODE] Found new file: ${filePath}`);
         // if filepath is empty, return. This is a safeguard, should not happen
         if (!filePath || filePath.trim().length === 0) {
            console.log('[SOURCE MODE] filePath empty: skip.');
            return;
         }
         const pipelineWordCount = createPipelineWordCount(path.basename(filePath));
         const pipelineID = pipelineWordCount.pipelineID;
         console.log(`[SOURCE MODE] Sending pipelineID: ${JSON.stringify(pipelineID)}`);
         await pipelinesProducer.send({
            topic: DISPATCHER_TOPIC,
            messages: [{
               key: pipelineID + "__source-record__0",
               value: JSON.stringify(newMessageValue(stringifyPipeline(pipelineWordCount),pipelineID))}],
         });
         console.log(`[SOURCE MODE] Sent pipelineID: ${JSON.stringify(pipelineID)}`);


         const fileContent = fs.readFileSync(filePath, 'utf-8');
         const data = fileContent.split('\n')

         let shuffled: { [key: string]: string[] } = {};

         console.log(`[SOURCE MODE] Sending ${data.length} records to DISPATCHER...`);
         data.forEach((record: any, index: number) => {
            producer.send({
               topic: DISPATCHER_TOPIC,
               // We add an index to the key as a reference for partitioning
               messages: [{
                  key: pipelineID + "__source-record__" + index,
                  value: JSON.stringify(newMessageValue(record, pipelineID)),
                  // NO explicit partitioning here, dispatcher will handle it
                  // partition: ...f(index)...
               }],
            });

            // ------------ SEQUENTIAL COMPUTATION ------------
            /**
             * SEQUENTIAL COMPUTATION
             * We compute sequentially and locally the map and reduce functions
             * so that we can compare the results with the parallelized version
             */
            // Compute locally and sequentially
            const results = pipelineWordCount.mapFn(record);
            // [[{word: 'abg', count: 1}], [{word: 'abg', count: 1}], [{word: 'agg', count: 1}]]
            results.forEach((v: Object[]) => {
               v.forEach((v) => {
                  const key = pipelineWordCount.keySelector(v);
                  if (!shuffled[key]) {
                     shuffled[key] = [];
                  }
                  shuffled[key].push(pipelineWordCount.dataSelector(v));
               })

            });
         });


         /**
          * SEQUENTIAL COMPUTATION
          * Reduce locally and sequentially
          * This helps to get a reference to check if the final parallelized result is correct
          */
         const reduced = Object.keys(shuffled).map((key) => {
            return [key, pipelineWordCount.reduceFn(key, shuffled[key])];
         });

         console.log(`[SOURCE MODE/seq] Sending ${reduced.length} sequentially reduced records to OUTPUT_TOPIC...`);
         reduced.forEach(async ([key, value]) => {
            await producer.send({
               topic: OUTPUT_TOPIC,
               messages: [{
                  key: key,
                  value: JSON.stringify(newMessageValueShuffled(value, "seq-word-count-"+ path.basename(filePath))),
               }],
            });
         });


         // Send special value to signal the end of the data stream
         console.log(`[SOURCE MODE] Sending stream ended message to DISPATCHER...`);
         await producer.send({
            topic: DISPATCHER_TOPIC,
            messages: [{
               key: `${pipelineID}__${STREAM_ENDED_KEY}`,
               value: JSON.stringify(newStreamEndedMessage(pipelineID, data.length)),
            }],
         });
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