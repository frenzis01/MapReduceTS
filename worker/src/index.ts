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
   unboxKafkaMessage,
   newStreamEndedMessage,
   isStreamEnded,
   newMessageValue,
   newMessageValueShuffled,
   PipelineConfig,
   stringifyPipeline,
   STREAM_ENDED_KEY
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
         else if (MODE === '--reduce' && unprocessedMessages[pipelineConfig.pipelineID]) {
            processUnprocessedMessages(pipelineConfig.pipelineID, processMessageReduce)
         }

      },
   });
}

function processUnprocessedMessages(pipelineID: string, callback: (key: string, val: MessageValue, pipelineConfig: PipelineConfig) => any) {
   if (!unprocessedMessages[pipelineID]) return;
   console.log(`[MAP MODE] Processing unprocessed messages for pipeline: ${pipelineID} | ${unprocessedMessages[pipelineID]?.length}`);
   // TODO make this async?
   unprocessedMessages[pipelineID].forEach( (msg:{key: string, val: MessageValue, timestamp: any}) => {
      console.log(`[MAP MODE] Processing unprocessed message: ${msg.timestamp}`);
       callback(msg.key, msg.val, pipelines[pipelineID]);
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

let unprocessedMessages: { [pipelineID: string]: any[] } = {}; // Queue for messages with missing pipelineConfig

function enqueueUnprocessedMessage (pipelineID: string, val: MessageValue, key: string, timestamp: any = null) {
   // TODO pause consumer if no pipeline available
   console.log(`[ERROR] No pipeline found for ID: ${pipelineID}. Pausing consumer...`);
   // Add entry to unprocessedMessages queue if missing
   if (!unprocessedMessages[pipelineID]) {
      unprocessedMessages[pipelineID] = [];
   }

   // Add message to queue
   unprocessedMessages[pipelineID].push({key, val, timestamp});
   // TODO check ordering of push+foreach

   return; // Skip processing this message for now
}

// Map mode: Applies the map function to incoming messages
async function mapMode() {
   await consumer.connect();
   await consumer.subscribe({ topic: MAP_TOPIC, fromBeginning: true });

   await producer.connect();

   consumer.run({
      // TODO
      // partitionsConsumedConcurrently: 3, // Default: 1
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         console.log(`[${MODE}/${WORKER_ID}] reading 00 ${message.value?.toString()}`)
         if (!message.value) {
            console.log(`[MAP MODE] No message value found. Skipping...`);
            return;
         }
         const { key, val } = unboxKafkaMessage(message);
         const pipelineConfig = pipelines[val.pipelineID];
         
         if (isStreamEnded(message)) {
            console.log(`[MAP MODE] Received stream ended message. Processing cached messages...`);
            processUnprocessedMessages(val.pipelineID, processMessageMap);
            
            console.log(`[MAP MODE] Propagating stream ended message to shuffle...`);
            // Send to shuffle consumer special value to start feeding the reduce
            await producer.send({
               topic: SHUFFLE_TOPIC,
               messages: [{ key: STREAM_ENDED_KEY, value: JSON.stringify(newStreamEndedMessage(val.pipelineID)), }],
            });
            return;
         }

         
         // TODO pause consumer if no pipeline available
         if (!pipelineConfig) enqueueUnprocessedMessage(val.pipelineID,val,key,message.timestamp);
         
         await processMessageMap(key,val, pipelineConfig);
      },
   });
}

/**
 * to be invoked on a message value ready to be processed in map mode
 * We enforce this by explicitly passing the pipelineConfig.
 * @param key is present only to match the signature of processMessageReduce
 * @param messageValue message.value
 * @param pipelineConfig this is here only to enforce that the pipelineConfig is passed in and should be available when processing
 */
async function processMessageMap(key:string, val: MessageValue, pipelineConfig: PipelineConfig) {
   // check if isStreamEndedMessage should not be necessary here...
   const mapResults = pipelineConfig.mapFn(val.data);
   for (const result of mapResults) {
      await producer.send({
         topic: SHUFFLE_TOPIC,
         messages: [{ key: pipelineConfig.keySelector(result), value: JSON.stringify(newMessageValue(result,pipelineConfig.pipelineID)) }],
      });
   }
   console.log(`[MAP MODE] Processed: ${val.data}`);
}



/**
 * Shuffle mode: Groups messages by key for each stream/pipeline
 * When it receives a special message, it sends all stored values to the reduce topic
 */
async function shuffleMode() {
   await consumer.connect();
   await consumer.subscribe({ topic: SHUFFLE_TOPIC, fromBeginning: true });

   await producer.connect();
   // For each pipelineID, we store the key-value pairs
   const keyValueStore: { [pipelineID: string]: { [key: string]: string[] } } = {};

   consumer.run({
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         console.log(`[${MODE}/${WORKER_ID}] -> ${!message.key || !message.value} | ${message.key?.toString()} ${message.value?.toString()}`)

         if (!message.key || !message.value) return;

         const { key, val } = unboxKafkaMessage(message);
         const pipelineID = val.pipelineID;
         // IF not received this pipelineID before, add it 
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
         keyValueStore[pipelineID][key].push(val.data);

         console.log(`[SHUFFLE MODE] Received: ${key} -> ${JSON.stringify(val.data)}`);

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


         const {key,val} = unboxKafkaMessage(message);
         const pipelineConfig = pipelines[val.pipelineID];

         
         // At this point we are almost sure that the pipelineConfig is available, 
         // otherwise the message would not have been processed in map, there must have
         // been a huge delay or some other issue related to the pipeline update message 
         if (!pipelineConfig) {
            console.log(`[ERROR] No pipeline found for ID: ${val.pipelineID}`);
            // TODO delay execution and retry
            enqueueUnprocessedMessage(val.pipelineID, val,key, message.timestamp);
            return;
         }
         processMessageReduce(key, val, pipelineConfig);
      },
   });
}

async function processMessageReduce(key: string, val: MessageValue,  pipelineConfig: PipelineConfig) {
   const reducedResult = pipelineConfig.reduceFn(key, val.data);
   console.log(`[REDUCE MODE] Reduced: ${key} -> ${reducedResult}`);

   await producer.send({
      topic: OUTPUT_TOPIC,
      messages: [{ "key": key, value: JSON.stringify(newMessageValue(reducedResult,val.pipelineID)) }],
   });
}


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
            const outputPath = path.join(OUTPUT_FOLDER, `${value.pipelineID}_result.txt`);
            fs.appendFileSync(outputPath, `${key}: ${value.data}\n`);
            console.log(`[OUTPUT MODE] Wrote result: ${key}: ${value.data}`);
         }
      },
   });
}

async function main() {
   if (MODE === '--map') {
      await listenForPipelineUpdates();
      await mapMode();
   } else if (MODE === '--shuffle') {
      // TODO perhaps this may be avoided for shuffle
      // await listenForPipelineUpdates();
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