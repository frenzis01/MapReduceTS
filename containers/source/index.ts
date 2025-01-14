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
   // @ts-ignore
} from "./utils";
// TODO ugly import

const kafka = new Kafka({
   clientId: 'mapreduce',
   brokers: ['kafka:9092'],
   logLevel: logLevel.NOTHING
});
const producer = kafka.producer();


const INPUT_FOLDER = './input';

// Pipeline implementing the typical word-count example
const createPipelineWordCount = (name: string): PipelineConfig => ({
   // TODO if name contains '_', hash name
   // TODO if pipelineID+name already exist as topic, increment
   pipelineID: 'word-count-' + name,
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
});


// Source mode: Reads files from a folder and sends messages to Kafka
async function sourceMode() {

   console.log('[SOURCE MODE] Monitoring input folder...');
   await producer.connect();
   console.log('[SOURCE MODE] Producer connected to Kafka...');


   const pipelinesProducer = kafka.producer();
   await pipelinesProducer.connect();

   // TODO make this customizable
   
   // function invoked for each file insise input folder
   const processFile = async (filePath: string) => {
      console.log(`[SOURCE MODE] Processing file: ${filePath}/${fs.existsSync(filePath)}`);
      // if file exists and is .txt
      if (fs.existsSync(filePath) && fs.lstatSync(filePath).isFile() && filePath.endsWith('.txt') 
         // TODO debug line
         && filePath.includes('short')
      ) {
         console.log(`[SOURCE MODE] Found new file: ${filePath}`);
         
         const pipelineWordCount = createPipelineWordCount(path.basename(filePath));
         const pipelineID = pipelineWordCount.pipelineID;
         console.log(`[SOURCE MODE] Sending pipelineID: ${JSON.stringify(pipelineID)}`);
         await pipelinesProducer.send({
            topic: DISPATCHER_TOPIC,
            messages: [ {
               key: pipelineID + "__source-record__0",
               value: JSON.stringify(newMessageValue(stringifyPipeline(pipelineWordCount), pipelineID))} ],
         });
         console.log(`[SOURCE MODE] Sent pipelineID: ${JSON.stringify(pipelineID)}`);


         const fileContent = fs.readFileSync(filePath, 'utf-8');
         const data = fileContent.split('\n')

         let shuffled: { [key: string]: string[] } = {};

         data.forEach((record: any, index: number) => {
            console.log(`[SOURCE MODE] type of record : ${typeof record} ${index} / ${pipelineID}`);
            producer.send({
               topic: DISPATCHER_TOPIC,
               // We add an index to the key as a reference for partitioning
               // we specify the partition to send the message to
               // note that this is an upper bound on the parallelism degree.
               messages: [{
                  key: pipelineID + "__source-record__" + index,
                  value: JSON.stringify(newMessageValue(record, pipelineID)),
                  // partition: (index) % 3
               }],
            });
            console.log(`[SOURCE MODE] Sent record: ${JSON.stringify(record)}`);

            // Compute locally and sequentially
            const results = pipelineWordCount.mapFn(record);
            results.forEach((v: string) => {
               const key = pipelineWordCount.keySelector(v);
               if (!shuffled[key]) {
                  shuffled[key] = [];
               }
               shuffled[key].push(v);

            });
         });


         /**
          * Reduce locally and sequentially
          * This helps to get a reference to check if the final parallelized result is correct
          */
         const reduced = Object.keys(shuffled).map((key) => {
            // console.log(`[SOURCE MODE/seq] Reducing: ${key}`);
            return [key, pipelineWordCount.reduceFn(key, shuffled[key])];
         });

         // reduced.forEach(async ([key, value]) => {
         //    console.log(`[SOURCE MODE/seq] Sending reduced: ${key}: ${value}`);
         //    await producer.send({
         //       topic: OUTPUT_TOPIC,
         //       messages: [{
         //          key: key,
         //          value: JSON.stringify(newMessageValueShuffled(value, "seq-word-count")),
         //       }],
         //    });
         // });


         // Send to shuffle consumer special value to start feeding the reduce
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