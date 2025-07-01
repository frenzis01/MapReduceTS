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

import {
   newStreamEndedMessage,
   newMessageValue,
   newMessageValueShuffled,
   PipelineConfig,
   stringifyPipeline,
   STREAM_ENDED_KEY,
   DISPATCHER_TOPIC,
   OUTPUT_TOPIC,
   bitwiseHash,
   // @ts-ignore
} from "./utils";
// TODO ugly import

console.log(`[SOURCE MODE] Starting source mode...`);
const kafka = new Kafka({
   clientId: 'mapreduce',
   brokers: ['kafka:9092'],
   logLevel: logLevel.NOTHING
});
const producer = kafka.producer({
   allowAutoTopicCreation: false,
});


const INPUT_FOLDER = './input';

// Pipeline implementing the typical word-count example
const createPipelineWordCount = (name: string): PipelineConfig => {
   if (name.includes('_')) {
      name = bitwiseHash(name).toString();
   };
   // Add random suffix of 6 chars to avoid conflicts for files with the same name
   const suffix = Math.random().toString(16).substring(2, 8);
   name = `${name}${suffix}`

   // TODO if name contains '_', hash name
   // TODO if pipelineID+name already exist as topic, increment
   return {
      pipelineID: 'word-count-' + name,
      keySelector: (message: any) => message.word,
      dataSelector: (message: any) => message.count,
      // Filter is used to avoid having empty strings "" in the array	
      mapFn: (value: any) => {
         // Map should take (k,v) and return an array of (k2,v2) pairs
         // Here we don't have any v.
         // We just have a string, so we need to split it into words
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
   // This seems to cause the TimeoutNegativeWarning
   await producer.connect();
   const pipelinesProducer = kafka.producer();

   await pipelinesProducer.connect();

   // This function is invoked for each file insise input folder
   const processFile = async (filePath: string) => {
      // if file exists and is .txt
      if (fs.existsSync(filePath) && fs.lstatSync(filePath).isFile() && filePath.endsWith('.txt')
         // TODO debug line
         // && filePath.includes('short')
      ) {
         console.log(`[SOURCE MODE] Processing file: ${filePath}`);

         // Create a pipeline for the word count
         const pipelineWordCount = createPipelineWordCount(path.basename(filePath));
         const pipelineID = pipelineWordCount.pipelineID;
         // Announce the pipeline to the DISPATCHER
         await pipelinesProducer.send({
            topic: DISPATCHER_TOPIC,
            messages: [{
               key: pipelineID + "__source-record__0",
               value: JSON.stringify(newMessageValue(stringifyPipeline(pipelineWordCount), pipelineID))
            }],
         });
         console.log(`[SOURCE MODE] Sent pipelineID: ${JSON.stringify(pipelineID)}`);


         const fileContent = fs.readFileSync(filePath, 'utf-8');
         const data = fileContent.split('\n')

         // shuffled is used for SEQUENTIAL COMPUTATION
         let shuffled: { [key: string]: string[] } = {};

         data.forEach((record: any, index: number) => {
            producer.send({
               topic: DISPATCHER_TOPIC,
               // We add an index to the key as a reference for partitioning
               // so that dispatcher knows the partition to send the message to
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
            // console.log(`[SOURCE MODE/seq] Reducing: ${key}`);
            return [key, pipelineWordCount.reduceFn(key, shuffled[key])];
         });

         reduced.forEach(async ([key, value]) => {
            // console.log(`[SOURCE MODE/seq] Sending reduced: ${key}: ${value}`);
            await producer.send({
               topic: OUTPUT_TOPIC,
               messages: [{
                  key: key,
                  value: JSON.stringify(newMessageValueShuffled(value, "seq-word-count")),
               }],
            });
         });
         // ------------ END OF SEQUENTIAL COMPUTATION ------------
         console.log(`[SOURCE] Sequential computation done`);


         // Send to special value to signal the end of the data stream
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

   // Invoke the above function for each text file in the input folder
   let files = fs.readdirSync(INPUT_FOLDER);
   console.log(`[SOURCE MODE] Found ${files.length} existing files in ${INPUT_FOLDER}`);
   files.forEach(async (file: any) => {
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