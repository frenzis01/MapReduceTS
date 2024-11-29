import fs from 'fs';
import path from 'path';
import { Kafka, logLevel } from 'kafkajs';

const kafka = new Kafka({
   clientId: 'mapreduce',
   brokers: ['kafka:9092'],
   logLevel: logLevel.NOTHING
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: process.env.GROUP_ID || 'default-group' });

const MODE = process.argv[2];

const INPUT_FOLDER = './input';
const OUTPUT_FOLDER = './output';

const PIPELINE_UPDATE_TOPIC = 'pipeline-updates';
const MAP_TOPIC = 'map-output';
const SHUFFLE_TOPIC = 'shuffle-output';
const REDUCE_TOPIC = 'reduce-output';

// Unique worker ID
const WORKER_ID = `worker-${Math.random().toString(36).substring(2, 15)}`;

interface PipelineConfig {
   pipelineID: string;
   keySelector: (message: any) => string;
   mapFn: (value: any) => any[];
   reduceFn: (key: string, values: any[]) => any;
}

let pipelines: { [pipelineID: string]: PipelineConfig } = {};

// Listen for pipeline updates
async function listenForPipelineUpdates() {
   // Use unique worker id so that every consumer in map/shuffle/reduce mode gets the pipeline updates
   const pipelinesConsumer = kafka.consumer({ groupId: WORKER_ID});
   await pipelinesConsumer.connect();
   await pipelinesConsumer.subscribe({ topic: PIPELINE_UPDATE_TOPIC, fromBeginning: true });

   pipelinesConsumer.run({
      eachMessage: async ({ message }) => {
         if (!message.value) return;
         const pipelineConfig = JSON.parse(message.value.toString());
         pipelines[pipelineConfig.pipelineID] = {
            pipelineID: pipelineConfig.pipelineID,
            keySelector: eval(pipelineConfig.keySelector),
            mapFn: eval(pipelineConfig.mapFn),
            reduceFn: eval(pipelineConfig.reduceFn),
         };
         console.log(`[Pipeline Update] Loaded pipeline: ${pipelineConfig.pipelineID} from group ${process.env.GROUP_ID}`);
      },
   });
}

const pipelineWordCount: PipelineConfig = {
   pipelineID: 'word-count',
   keySelector: (message: any) => message.word,
   mapFn: (value: any) => {
      const words = value.text.split(' ');
      return words.map((word: string) => ({ word, count: 1 }));
   },
   reduceFn: (key: string, values: any[]) => {
      return values.reduce((acc, curr) => acc + curr.count, 0);
   },
}

const PIPELINED_ENDED_KEY = 'PIPELINE_ENDED';
const PIPELINE_ENDED_VALUE = null;
// Source mode: Reads files from a folder and sends messages to Kafka
async function sourceMode() {
   console.log('[SOURCE MODE] Monitoring input folder...');
   await producer.connect();

   const pipelinesProducer = kafka.producer();
   await pipelinesProducer.connect();

   
   fs.watch(INPUT_FOLDER, async (eventType, filename) => {
      if (eventType === 'rename' && filename) {
         const filePath = path.join(INPUT_FOLDER, filename);
         if (fs.existsSync(filePath)) {
            console.log(`[SOURCE MODE] Found new file: ${filename}`);
            const fileContent = fs.readFileSync(filePath, 'utf-8');
            // const data = JSON.parse(fileContent);
            const data = fileContent.split('\n')
            // TODO make this customizable
         
            await pipelinesProducer.send({
               topic: PIPELINE_UPDATE_TOPIC,
               messages: [{ value: JSON.stringify(pipelineWordCount) }],
            });
            console.log(`[SOURCE MODE] Sent pipelineID: ${JSON.stringify(pipelineWordCount.pipelineID)}`);

            
            for (const record of data) {
               await producer.send({
                  topic: MAP_TOPIC,
                  messages: [{ value: JSON.stringify({ value: record, pipelineID: pipelineWordCount.pipelineID }) }],
               });
               console.log(`[SOURCE MODE] Sent record: ${JSON.stringify(record)}`);
            }

            // Send to shuffle consumer special value to start feeding the reduce
            await producer.send({
               topic: SHUFFLE_TOPIC,
               messages: [{ key: PIPELINED_ENDED_KEY, value: PIPELINE_ENDED_VALUE }],
            });

            fs.unlinkSync(filePath); // Optionally remove the file after processing
         }
      }
   });
}

let unprocessedMessages: { [pipelineID: string]: any[] } = {}; // Queue for messages with missing pipelineConfig

// Map mode: Applies the map function to incoming messages
async function mapMode() {
   await consumer.connect();
   await consumer.subscribe({ topic: MAP_TOPIC, fromBeginning: true });

   await producer.connect();

   consumer.run({
      eachMessage: async ({ message }) => {
         if (!message.value) return;

         const messageValue = JSON.parse(message.value.toString());
         const pipelineID = messageValue.pipelineID;
         const pipelineConfig = pipelines[pipelineID];


         if (!pipelineConfig) {
            console.log(`[ERROR] No pipeline found for ID: ${pipelineID}. Pausing consumer...`);
            
            // Add entry to unprocessedMessages queue if missing
            if (!unprocessedMessages[pipelineID]) {
               unprocessedMessages[pipelineID] = [];
            }

            // Add message to queue
             unprocessedMessages[pipelineID].push(messageValue);
       
             return; // Skip processing this message for now
         }
            
         const mapResults = pipelineConfig.mapFn(messageValue);
         for (const result of mapResults) {
            await producer.send({
               topic: SHUFFLE_TOPIC,
               messages: [{ key: pipelineConfig.keySelector(result), value: JSON.stringify(result) }],
            });
         }
         console.log(`[MAP MODE] Processed: ${messageValue}`);
      },
   });
}

/**
 * to be invoked on a message value ready to be processed in map mode
 * @param messageValue message.value
 * @param pipelineConfig this is here only to enforce that the pipelineConfig is passed in and should be available when processing
 */
async function processMessageMap(messageValue: any, pipelineConfig: PipelineConfig) {
   const mapResults = pipelineConfig.mapFn(messageValue);
   for (const result of mapResults) {
      await producer.send({
         topic: SHUFFLE_TOPIC,
         messages: [{ key: pipelineConfig.keySelector(result), value: JSON.stringify(result) }],
      });
   }
   console.log(`[MAP MODE] Processed: ${messageValue}`);
}


/**
 * Shuffle mode: Groups messages by key, does not need the pipelineConfig
 * When it receives a special message, it sends all stored values to the reduce topic
 */
async function shuffleMode() {
   await consumer.connect();
   await consumer.subscribe({ topic: SHUFFLE_TOPIC, fromBeginning: true });

   await producer.connect();
   const keyValueStore: { [key: string]: string[] } = {};

   consumer.run({
      eachMessage: async ({ message }) => {
         if (message.key === PIPELINED_ENDED_KEY && message.value === PIPELINE_ENDED_VALUE) {	
            // Send stored values to reduce
            for (const key of Object.keys(keyValueStore)) {
               await producer.send({
                  topic: REDUCE_TOPIC,
                  messages: [{ key, value: JSON.stringify({ key, values: keyValueStore[key] }) }],
               });
               console.log(`[SHUFFLE MODE] Sending: ${key} -> ${JSON.stringify(keyValueStore[key])}`);

            }
         }
         if (!message.key || !message.value) return;

         const messageValue = JSON.parse(message.value.toString());
         const key = message.key.toString();

         if (!keyValueStore[key]) {
            keyValueStore[key] = [];
         }
         keyValueStore[key].push(messageValue);

         console.log(`[SHUFFLE MODE] Received: ${key} -> ${messageValue}`);

      },
   });
}

// Reduce mode: Applies the reduce function and forwards results
async function reduceMode() {
   await consumer.connect();
   await consumer.subscribe({ topic: REDUCE_TOPIC, fromBeginning: true });

   await producer.connect();

   consumer.run({
      eachMessage: async ({ message }) => {
         if (!message.value) return;

         const messageValue = JSON.parse(message.value.toString());
         const pipelineID = messageValue.pipelineID;
         const pipelineConfig = pipelines[pipelineID];

         if (!pipelineConfig) {
            console.log(`[ERROR] No pipeline found for ID: ${pipelineID}`);
            return;
         }

         const reducedResult = pipelineConfig.reduceFn(messageValue.key, messageValue.values);
         console.log(`[REDUCE MODE] Reduced: ${messageValue.key} -> ${reducedResult}`);

         await producer.send({
            topic: REDUCE_TOPIC,
            messages: [{ key: messageValue.key, value: JSON.stringify(reducedResult) }],
         });
      },
   });
}

// Output mode: Writes reduced results to disk
async function outputMode() {
   console.log('[OUTPUT MODE] Waiting for results to be written to output folder...');
   await consumer.connect();
   await consumer.subscribe({ topic: REDUCE_TOPIC, fromBeginning: true });

   consumer.run({
      eachMessage: async ({ message }) => {
         const key = message.key?.toString();
         const value = message.value?.toString();

         if (key && value) {
            // const outputPath = path.join(OUTPUT_FOLDER, `result-${key}.txt`);
            const outputPath = path.join(OUTPUT_FOLDER, `result.txt`);
            fs.appendFileSync(outputPath, `${key}: ${value}\n`);
            console.log(`[OUTPUT MODE] Wrote result: ${key}: ${value}`);
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
});
