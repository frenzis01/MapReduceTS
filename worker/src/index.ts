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

import {
   MessageType,
   MessageValue,
   newStreamEndedMessage,
   isStreamEnded,
   newMessageValue,
   newMessageValueShuffled,
   PipelineConfig,
   stringifyPipeline
} from './utils';

const kafka = new Kafka({
   clientId: 'mapreduce',
   brokers: ['kafka:9092'],
   logLevel: logLevel.NOTHING
});
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: GROUP_ID || 'default-group' });


const INPUT_FOLDER = './input';
const OUTPUT_FOLDER = './output';

const PIPELINE_UPDATE_TOPIC = 'pipeline-updates';
const MAP_TOPIC = 'map-topic';
const SHUFFLE_TOPIC = 'shuffle-topic';
const REDUCE_TOPIC = 'reduce-topic';
const OUTPUT_TOPIC = 'output-topic';

// Unique worker ID
const WORKER_ID = `worker-${Math.random().toString(36).substring(2, 15)}`;

let pipelines: { [pipelineID: string]: PipelineConfig } = {};

// Listen for pipeline updates
async function listenForPipelineUpdates() {
   // Use unique worker id so that every consumer in map/shuffle/reduce mode gets the pipeline updates
   const pipelinesConsumer = kafka.consumer({ groupId: WORKER_ID });
   // TODO actually shuffler doesn't need pipelineConfig updates
   await pipelinesConsumer.connect();
   await pipelinesConsumer.subscribe({ topic: PIPELINE_UPDATE_TOPIC, fromBeginning: true });

   pipelinesConsumer.run({
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         console.log(`[Pipeline Update] [${GROUP_ID}/${WORKER_ID}] received msg with TS ${message.timestamp} | ${message.value}`);
         if (!message.value) return;
         const pipelineConfig = JSON.parse(message.value.toString());
         pipelines[pipelineConfig.pipelineID] = {
            pipelineID: pipelineConfig.pipelineID,
            keySelector: eval(pipelineConfig.keySelector),
            mapFn: eval(pipelineConfig.mapFn),
            reduceFn: eval(pipelineConfig.reduceFn),
         };

         console.log(`[Pipeline Update] [${GROUP_ID}/${WORKER_ID}] Updated pipeline: ${pipelineConfig.pipelineID}`);
         if (MODE === '--map' && unprocessedMessages[pipelineConfig.pipelineID]) {
            processUnprocessedMessages(pipelineConfig.pipelineID,processMessageMap);
         }

      },
   });
}

function processUnprocessedMessages(pipelineID: string, callback: (arg0: any, arg1: PipelineConfig) => any) {
   if (!unprocessedMessages[pipelineID]) return;
   console.log(`[MAP MODE] Processing unprocessed messages for pipeline: ${pipelineID} | ${unprocessedMessages[pipelineID]?.length}`);
   // TODO make this async?
   unprocessedMessages[pipelineID].forEach( (messageValue) => {
      console.log(`[MAP MODE] Processing unprocessed message: ${typeof messageValue}:${messageValue}`);
       callback(messageValue, pipelines[pipelineID]);
   });
}

const pipelineWordCount: PipelineConfig = {
   pipelineID: 'word-count',
   keySelector: (message: any) => message.word,
   mapFn: (value: any) => {
      console.log(`[MAP MODE] Mapping type of value: ${typeof value}:${JSON.stringify(value)}`);
      const words = value.split(/[^a-zA-Z0-9]+/);
      return words.map((word: string) => ({ word, count: 1 }));
   },
   reduceFn: (key: string, values: any[]) => {
      return values.reduce((acc, curr) => acc + curr.count, 0);
   },
}



// TODO remove or use in pipelineUpdates
const parsePipeline = (pipeline_str: string) => {
   const pipelineConfig = JSON.parse(pipeline_str);
   return {
      pipelineID: pipelineConfig.pipelineID,
      keySelector: eval(pipelineConfig.keySelector),
      mapFn: eval(pipelineConfig.mapFn),
      reduceFn: eval(pipelineConfig.reduceFn),
   };
}

const STREAM_ENDED_KEY = 'STREAM_ENDED';
const STREAM_ENDED_TYPE = 'STREAM_ENDED';
const STREAM_DATA_TYPE = 'STREAM_DATA';
const STREAM_ENDED_VALUE = null;
// Source mode: Reads files from a folder and sends messages to Kafka
async function sourceMode() {
   console.log('[SOURCE MODE] Monitoring input folder...');
   await producer.connect();
   console.log('[SOURCE MODE] Connected to producer...');

   const pipelinesProducer = kafka.producer();
   await pipelinesProducer.connect();

   // TODO make this customizable
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


         for (const record of data) {
            console.log(`[SOURCE MODE] type of record : ${typeof record}`);
            await producer.send({
               topic: MAP_TOPIC,
               // TODO optional add index to record, but consider performance
               messages: [{ key: "source-record", value: JSON.stringify(newMessageValue(record,pipelineWordCount.pipelineID)) }],
            });
            console.log(`[SOURCE MODE] Sent record: ${JSON.stringify(record)}`);
         }

         // Send to shuffle consumer special value to start feeding the reduce
         console.log(`[SOURCE MODE] Sending stream ended message to MAP...`);
         await producer.send({
            topic: MAP_TOPIC,
            // messages: [{ key: STREAM_ENDED_KEY, value: JSON.stringify({ type: STREAM_ENDED_TYPE, data: STREAM_ENDED_VALUE, pipelineID: pipelineWordCount.pipelineID }), }],
            messages: [{ key: STREAM_ENDED_KEY, value: JSON.stringify(newStreamEndedMessage(pipelineWordCount.pipelineID)), }],
         });

         // fs.unlinkSync(filePath); // Optionally remove the file after processing
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

let unprocessedMessages: { [pipelineID: string]: any[] } = {}; // Queue for messages with missing pipelineConfig

function enqueueUnprocessedMessage (pipelineID: string, data: string, key: string = "generic-record") {
   // TODO pause consumer if no pipeline available
   console.log(`[ERROR] No pipeline found for ID: ${pipelineID}. Pausing consumer...`);
   // Add entry to unprocessedMessages queue if missing
   if (!unprocessedMessages[pipelineID]) {
      unprocessedMessages[pipelineID] = [];
   }

   // Add message to queue
   unprocessedMessages[pipelineID].push(data);
   // TODO check ordering of push+foreach

   return; // Skip processing this message for now
}

// Map mode: Applies the map function to incoming messages
async function mapMode() {
   await consumer.connect();
   await consumer.subscribe({ topic: MAP_TOPIC, fromBeginning: true });

   await producer.connect();

   consumer.run({
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         console.log(`[${MODE}/${WORKER_ID}] reading 00 ${message.value?.toString()}`)
         // console.log(`[${MODE}/${WORKER_ID}] reading 01 ${message.value?.toString()}`)
         // console.log(!message.value);
         // console.log(`[${MODE}/${WORKER_ID}] reading 02 ${message.value?.toString()}`)
         if (!message.value) {
            console.log(`[MAP MODE] No message value found. Skipping...`);
            return;
         }
         const value = JSON.parse(message.value.toString());
         const pipelineID = value.pipelineID;
         const pipelineConfig = pipelines[pipelineID];
         const data = value.data;

         if (isStreamEnded(message)) {
            console.log(`[MAP MODE] Received stream ended message. Processing cached messages...`);
            processUnprocessedMessages(pipelineID, processMessageMap);
            
            console.log(`[MAP MODE] Propagating stream ended message to shuffle...`);
            // Send to shuffle consumer special value to start feeding the reduce
            await producer.send({
               topic: SHUFFLE_TOPIC,
               messages: [{ key: STREAM_ENDED_KEY, value: JSON.stringify(newStreamEndedMessage(pipelineID)), }],
            });
            return;
         }
         // console.log(`[${MODE}/${WORKER_ID}] reading 10 ${message.value?.toString()}`)

         // if (!pipelineConfig) {
         //    // TODO pause consumer if no pipeline available
         //    console.log(`[ERROR] No pipeline found for ID: ${pipelineID}. Pausing consumer...`);
         //    // Add entry to unprocessedMessages queue if missing
         //    if (!unprocessedMessages[pipelineID]) {
         //       unprocessedMessages[pipelineID] = [];
         //    }

         //    // Add message to queue
         //    unprocessedMessages[pipelineID].push(data);

         //    return; // Skip processing this message for now
         // }

         if (!pipelineConfig) enqueueUnprocessedMessage(pipelineID,data);
         
         await processMessageMap(data, pipelineConfig);
      },
   });
}

/**
 * to be invoked on a message value ready to be processed in map mode
 * @param messageValue message.value
 * @param pipelineConfig this is here only to enforce that the pipelineConfig is passed in and should be available when processing
 */
async function processMessageMap(data: any, pipelineConfig: PipelineConfig) {
   const mapResults = pipelineConfig.mapFn(data);
   for (const result of mapResults) {
      await producer.send({
         topic: SHUFFLE_TOPIC,
         // messages: [{ key: pipelineConfig.keySelector(result), value: JSON.stringify({ type: STREAM_DATA_TYPE, data: result, pipelineID: pipelineConfig.pipelineID }) }],
         messages: [{ key: pipelineConfig.keySelector(result), value: JSON.stringify(newMessageValue(result,pipelineConfig.pipelineID)) }],
      });
   }
   console.log(`[MAP MODE] Processed: ${data}`);
}




/**
 * Shuffle mode: Groups messages by key for each stream/pipeline
 * When it receives a special message, it sends all stored values to the reduce topic
 */
async function shuffleMode() {
   await consumer.connect();
   await consumer.subscribe({ topic: SHUFFLE_TOPIC, fromBeginning: true });

   await producer.connect();
   // TODO add layer for each pipeline
   // const keyValueStore: { [key: string]: string[] } = {};
   // For each pipelineID, we store the key-value pairs
   const keyValueStore: { [pipelineID: string]: { [key: string]: string[] } } = {};

   consumer.run({
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         console.log(`[${MODE}/${WORKER_ID}] -> ${!message.key || !message.value} | ${message.key?.toString()} ${message.value?.toString()}`)

         if (!message.key || !message.value) return;


         const value = JSON.parse(message.value.toString());
         const data = value.data;
         const pipelineID = value.pipelineID;
         const key = message.key.toString();
         console.log(`[${MODE}/${WORKER_ID}] -> ${key} -> ${data} | ${pipelineID}`)

         // IF not received this pipelinedID before, add it 
         if (!keyValueStore[pipelineID]) {
            keyValueStore[pipelineID] = {};
         }

         if (isStreamEnded(message)) {
            // Send stored values to reduce
            for (const key of Object.keys(keyValueStore[pipelineID])) {
               // Remove tmp
               const tmp = JSON.stringify(newMessageValueShuffled(keyValueStore[pipelineID][key],pipelineID));
               await producer.send({
                  topic: REDUCE_TOPIC,
                  messages: [{ "key": key, "value": tmp }],
               });
               console.log(tmp);
               console.log(`[SHUFFLE MODE] Sending: ${key} -> ${tmp}`);
            }
         }
         if (!keyValueStore[pipelineID][key]) {
            keyValueStore[pipelineID][key] = [];
         }
         keyValueStore[pipelineID][key].push(data);

         console.log(`[SHUFFLE MODE] Received: ${key} -> ${JSON.stringify(data)}`);

      },
   });
}

// Reduce mode: Applies the reduce function and forwards results
async function reduceMode() {
   await consumer.connect();
   await consumer.subscribe({ topic: REDUCE_TOPIC, fromBeginning: true });

   await producer.connect();

   consumer.run({
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         console.log(`[${MODE}/${WORKER_ID}]`)
         if (!message.key || !message.value) return;

         const key = message.key.toString();
         const value = JSON.parse(message.value.toString());
         // const list = value.values;
         console.log(`[REDUCE MODE] Received: ${key} -> ${JSON.stringify(value)}`);
         const pipelineID = value.pipelineID;
         const pipelineConfig = pipelines[pipelineID];

         if (!pipelineConfig) {
            console.log(`[ERROR] No pipeline found for ID: ${pipelineID}`);
            // TODO delay execution and retry
            // enqueueUnprocessedMessage(pipelineID)
            return;
         }

         // At this point we are sure that the pipelineConfig is available, 
         // otherwise the message would not have been processed in map
         const reducedResult = pipelineConfig.reduceFn(key, value.data);
         console.log(`[REDUCE MODE] Reduced: ${key} -> ${reducedResult}`);

         await producer.send({
            topic: OUTPUT_TOPIC,
            messages: [{ "key": key, value: JSON.stringify(newMessageValue(reducedResult,pipelineID)) }],
         });
      },
   });
}

// function processMessageReduce(data: any, pipelineConfig: PipelineConfig, key: string) {
//    const reducedResult = pipelineConfig.reduceFn();
//    console.log(`[REDUCE MODE] Reduced: ${data.key} -> ${reducedResult}`);
// }


// Output mode: Writes reduced results to disk
async function outputMode() {
   console.log('[OUTPUT MODE] Waiting for results to be written to output folder...');
   await consumer.connect();
   await consumer.subscribe({ topic: OUTPUT_TOPIC, fromBeginning: true });

   consumer.run({
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         console.log(`[${MODE}/${WORKER_ID}]`)
         if (!message.value || !message.key) return;
         const key = message.key?.toString();
         const value = JSON.parse(message.value.toString());

         if (key && value) {
            // const outputPath = path.join(OUTPUT_FOLDER, `result-${key}.txt`);
            const outputPath = path.join(OUTPUT_FOLDER, `${value.pipelineID}_result.txt`);
            fs.appendFileSync(outputPath, `${key}: ${value.data}\n`);
            console.log(`[OUTPUT MODE] Wrote result: ${key}: ${value.data}`);
         }
      },
   });
}

async function main() {
   if (MODE === '--source') {
      await sourceMode();
   } else if (MODE === '--map') {
      await listenForPipelineUpdates();
      await mapMode();
   } else if (MODE === '--shuffle') {
      await listenForPipelineUpdates();
      await shuffleMode();
   } else if (MODE === '--reduce') {
      await listenForPipelineUpdates();
      await reduceMode();
   } else if (MODE === '--output') {
      await outputMode();
   } else {
      console.log('[ERROR] No valid mode provided. Use --source, --map, --shuffle, --reduce, or --output.');
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