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
const createPipelineWordRand = (name: string): PipelineConfig => ({
   // TODO if name contains '_', hash name
   // TODO if pipelineID+name already exist as topic, increment
   pipelineID: 'word-rand-' + name,
   keySelector: (message: any) => message.word,
   dataSelector: (message: any) => message.data,
   // Filter is used to avoid having empty strings "" in the array	
   // console.log(`[MAP MODE] Mapping type of value: ${typeof value}:${JSON.stringify(value)}`);
   mapFn: (value: any) => {
      const words = value.split(/[^a-zA-Z0-9]+/).filter(Boolean);
      const foo = (s: string) => {
         // Take each letter and repeat it a random number of times
         return s
            .split('')
            .map((char) => {
               const randomRepeat = Math.floor(Math.random() * 10);
               return char.repeat(randomRepeat);
            })
            // .join(''); // Join the array of repeated characters into a string
      }
      return words.map((word: string) => ([
         { word: word.toLowerCase(), data: foo(word.toLowerCase()) },
         { word: word.toUpperCase(), data: foo(word.toUpperCase()) }
      ])); // Call foo(word) here
   },
   reduceFn: (key: string, values: any[]) => {
      // Count how many times key is contained inside the repeated characters
      
      // cast values to string[] from any[]
      // data is an array of strings of repeated characters like
      // ['aa', 'bbb', 'aaaa', 'ccc']
      console.log(`[REDUCE MODE] Reducing: ${key} with data: ${JSON.stringify(values)}`);

      
      let maxCount = 0;
         const keyCharCount : { [c: string]: number } = {};
         // Populate and initiate the keyCharCount object
         for (const char of key) {
            keyCharCount[char] = 0;
         }
         
         // for each char in data count occurences/length
         for (const str of values) {
            // Here we assume that the strings are actually only one character repeated,
            // with no more than one distinct char
            const char = str.charAt(0);
            if (char in keyCharCount) {
            keyCharCount[char] += str.length;
         }
      }
      
      
      // Subtract from keyCharCount the key chars until we get a negative value
      let count = 0;
      counterLoop: while (count < Math.max(...Object.values(keyCharCount))) {
         for (const char of key) {
            keyCharCount[char]--;
            if (keyCharCount[char] < 0)
               break counterLoop;
         }
         count++;
      }
      return count;
   },
});


// Source mode: Reads files from a folder and sends messages to Kafka
async function sourceMode() {

   console.log('[SOURCE MODE] Monitoring input folder...');
   // This seems to cause the TimeoutNegativeWarning
   await producer.connect();
   const pipelinesProducer = kafka.producer();

   await pipelinesProducer.connect();

   // TODO make this customizable
   
   // This function is invoked for each file insise input folder
   const processFile = async (filePath: string) => {
      console.log(`[SOURCE MODE] Processing file: ${filePath}/${fs.existsSync(filePath)}`);
      // if file exists and is .txt
      if (fs.existsSync(filePath) && fs.lstatSync(filePath).isFile() && filePath.endsWith('.txt') 
         // TODO debug line
         // && filePath.includes('short')
      ) {
         console.log(`[SOURCE MODE] Found new file: ${filePath}`);
         
         // Create a pipeline for the word count
         const pipelineWordRand = createPipelineWordRand(path.basename(filePath));
         const pipelineID = pipelineWordRand.pipelineID;
         console.log(`[SOURCE MODE] Sending pipelineID: ${JSON.stringify(pipelineID)}`);
         // Announce the pipeline to the DISPATCHER
         await pipelinesProducer.send({
            topic: DISPATCHER_TOPIC,
            messages: [ {
               key: pipelineID + "__source-record__0",
               value: JSON.stringify(newMessageValue(stringifyPipeline(pipelineWordRand), pipelineID))} ],
         });
         console.log(`[SOURCE MODE] Sent pipelineID: ${JSON.stringify(pipelineID)}`);


         const fileContent = fs.readFileSync(filePath, 'utf-8');
         const data = fileContent.split('\n')

         // Used for SEQUENTIAL COMPUTATION
         let shuffled: { [key: string]: string[] } = {};

         data.forEach((record: any, index: number) => {
            console.log(`[SOURCE MODE] type of record : ${typeof record} ${index} / ${pipelineID}`);
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
            console.log(`[SOURCE MODE] Sent record: ${JSON.stringify(record)}`);

            // ------------ SEQUENTIAL COMPUTATION ------------
            /**
             * SEQUENTIAL COMPUTATION
             * We compute sequentially and locally the map and reduce functions
             * so that we can compare the results with the parallelized version
             */
            console.log(`[SOURCE MODE/seq] Mapping: ${record}`);
            const results = pipelineWordRand.mapFn(record);
            results
               .flat()
               .forEach((v: any) => {
               const key = pipelineWordRand.keySelector(v);
               // console.log(`[SOURCE MODE/seq] Shuffling: ${key}`);
               if (!shuffled[key]) {
                  shuffled[key] = [];
               }
               shuffled[key].push(pipelineWordRand.dataSelector(v));

            });
         });


         /**
          * SEQUENTIAL COMPUTATION
          * Reduce locally and sequentially
          * This helps to get a reference to check if the final parallelized result is correct
          */
         const reduced = Object.keys(shuffled).map((key) => {
            console.log(`[SOURCE MODE/seq] Reducing: ${key}`);
            return [key, pipelineWordRand.reduceFn(key, shuffled[key].flat())];
         });

         reduced.forEach(async ([key, value]) => {
            // console.log(`[SOURCE MODE/seq] Sending reduced: ${key}: ${value}`);
            await producer.send({
               topic: OUTPUT_TOPIC,
               messages: [{
                  key: key,
                  value: JSON.stringify(newMessageValueShuffled(value, "seq-word-rand")),
               }],
            });
         });
         // ------------ END OF SEQUENTIAL COMPUTATION ------------


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