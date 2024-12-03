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
   const pipelinesConsumer = kafka.consumer({ groupId: WORKER_ID });
   await pipelinesConsumer.connect();
   await pipelinesConsumer.subscribe({ topic: PIPELINE_UPDATE_TOPIC, fromBeginning: true });

   pipelinesConsumer.run({
      eachMessage: async ({ message }) => {
         console.log(`[Pipeline Update] [${process.env.GROUP_ID}/${WORKER_ID}] received msg with TS ${message.timestamp}`);
         if (!message.value) return;
         const pipelineConfig = JSON.parse(message.value.toString());
         pipelines[pipelineConfig.pipelineID] = {
            pipelineID: pipelineConfig.pipelineID,
            keySelector: eval(pipelineConfig.keySelector),
            mapFn: eval(pipelineConfig.mapFn),
            reduceFn: eval(pipelineConfig.reduceFn),
         };
         // console.log(`[Pipeline Update] Loaded pipeline: ${pipelineConfig.pipelineID} from group ${process.env.GROUP_ID}/${WORKER_ID}`);
         // console.log(`[Pipeline Update] ${process.env.GROUP_ID}/${WORKER_ID} Loaded pipeline: ${JSON.stringify(pipelineConfig)}/${JSON.stringify(pipelineConfig.mapFn)}`);
         if (MODE === '--map' && unprocessedMessages[pipelineConfig.pipelineID]) {
            console.log(`[MAP MODE] Processing unprocessed messages for pipeline: ${pipelineConfig.pipelineID}`);
            unprocessedMessages[pipelineConfig.pipelineID].forEach(async (messageValue) => {
               console.log(`[MAP MODE] Processing unprocessed message: ${typeof messageValue}:${messageValue}`);
               await processMessageMap(messageValue, pipelines[pipelineConfig.pipelineID]);
            });
         }

         // if (isConsumerPaused) {
         //    isConsumerPaused = false;
         //    console.log(`[Pipeline Update] Resuming consumer...`);
         //    const MY_TOPIC = MODE === '--map' ? MAP_TOPIC : (MODE === '--shuffle' ? SHUFFLE_TOPIC : REDUCE_TOPIC);
         //    await consumer.resume([{ topic: MY_TOPIC }]);
         // }

      },
   });
}

const pipelineWordCount: PipelineConfig = {
   pipelineID: 'word-count',
   keySelector: (message: any) => message.word,
   mapFn: (value: any) => {
      console.log(`[MAP MODE] Mapping type of value: ${typeof value}:${JSON.stringify(value)}`);
      const words = value.split(' ');
      return words.map((word: string) => ({ word, count: 1 }));
   },
   reduceFn: (key: string, values: any[]) => {
      return values.reduce((acc, curr) => acc + curr.count, 0);
   },
}

const stringifyPipeline = (pipeline: PipelineConfig) => {
   const tmp = {
      pipelineID: pipeline.pipelineID,
      keySelector: pipeline.keySelector.toString(),
      mapFn: pipeline.mapFn.toString(),
      reduceFn: pipeline.reduceFn.toString(),
   }
   return JSON.stringify(tmp);
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

const PIPELINED_ENDED_KEY = 'PIPELINE_ENDED';
const PIPELINE_ENDED_TYPE = 'PIPELINE_ENDED';
const PIPELINE_DATA_TYPE = 'PIPELINE_DATA';
const PIPELINE_ENDED_VALUE = null;
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
         // const data = JSON.parse(fileContent);
         const data = fileContent.split('\n')


         for (const record of data) {
            console.log(`[SOURCE MODE] type of record : ${typeof record}`);
            await producer.send({
               topic: MAP_TOPIC,
               messages: [{ value: JSON.stringify({ data: record, pipelineID: pipelineWordCount.pipelineID }) }],
            });
            console.log(`[SOURCE MODE] Sent record: ${JSON.stringify(record)}`);
         }

         // Send to shuffle consumer special value to start feeding the reduce
         console.log(`[SOURCE MODE] Sending pipeline ended message to MAP...`);
         await producer.send({
            topic: MAP_TOPIC,
            messages: [{ key: PIPELINED_ENDED_KEY, value: JSON.stringify({ type: PIPELINE_ENDED_TYPE, data: PIPELINE_ENDED_VALUE }), }],
         });

         // fs.unlinkSync(filePath); // Optionally remove the file after processing
      }
   }

   let files = fs.readdirSync(INPUT_FOLDER);
   console.log(`[SOURCE MODE] Found ${files.length} existing files in ${INPUT_FOLDER}`);
   files.forEach(async (file) => {
      console.log(`[SOURCE MODE] Processing existing file: ${file} in ${INPUT_FOLDER}`);
      const filePath = path.join(INPUT_FOLDER, file);
      await processFile(filePath);
   });

}

let unprocessedMessages: { [pipelineID: string]: any[] } = {}; // Queue for messages with missing pipelineConfig
let isConsumerPaused = false;

function pipelineEnded(message: any): boolean {
   const parsedMessage = JSON.parse(message.value.toString());
   const tmp = message.key === PIPELINED_ENDED_KEY && parsedMessage.value.type === PIPELINE_ENDED_TYPE && parsedMessage.value.data === PIPELINE_ENDED_VALUE;
   console.log(`[MAP MODE] Received message: ${message.key} -> ${JSON.stringify(parsedMessage)} | ${!parsedMessage || !message.key} | ${tmp}}`);
   if (!parsedMessage || !message.key) return false;
   return message.key === PIPELINED_ENDED_KEY && parsedMessage.value.type === PIPELINE_ENDED_TYPE && parsedMessage.value.data === PIPELINE_ENDED_VALUE;
}

// Map mode: Applies the map function to incoming messages
async function mapMode() {
   await consumer.connect();
   await consumer.subscribe({ topic: MAP_TOPIC, fromBeginning: true });

   await producer.connect();


   // if no pipelines, pause consumer
   // if (Object.keys(pipelines).length === 0) {
   //    isConsumerPaused = true;
   //    console.log(`[ERROR] No pipelines found. Pausing consumer...`);
   //    await consumer.pause([{ topic: MAP_TOPIC }]);
   // }
   consumer.run({
      eachMessage: async ({ message }) => {
         console.log(`[${MODE}/${WORKER_ID}] reading ${message.value?.toString()}`)
         if (!message.value) return;

         if (pipelineEnded(message)) {
            console.log(`[MAP MODE] Received pipeline ended message. Sending to shuffle...`);
            // Send to shuffle consumer special value to start feeding the reduce
            await producer.send({
               topic: SHUFFLE_TOPIC,
               messages: [{ key: PIPELINED_ENDED_KEY, value: JSON.stringify({ type: PIPELINE_ENDED_TYPE, data: PIPELINE_ENDED_VALUE }), }],
            });
            return;
         }

         const value = JSON.parse(message.value.toString());
         const pipelineID = value.pipelineID;
         const pipelineConfig = pipelines[pipelineID];
         const data = value.data;

         if (!pipelineConfig) {
            console.log(`[ERROR] No pipeline found for ID: ${pipelineID}. Pausing consumer...`);
            // Add entry to unprocessedMessages queue if missing
            if (!unprocessedMessages[pipelineID]) {
               unprocessedMessages[pipelineID] = [];
            }

            // Add message to queue
            unprocessedMessages[pipelineID].push(data);

            return; // Skip processing this message for now
         }

         const mapResults = pipelineConfig.mapFn(data);
         for (const result of mapResults) {
            await producer.send({
               topic: SHUFFLE_TOPIC,
               messages: [{ key: pipelineConfig.keySelector(result), value: JSON.stringify({ type: PIPELINE_DATA_TYPE, data: result }) }],
            });
         }
         console.log(`[MAP MODE] Processed: ${data}`);
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
         messages: [{ key: pipelineConfig.keySelector(result), value: JSON.stringify({ type: PIPELINE_DATA_TYPE, data: result }) }],
      });
   }
   console.log(`[MAP MODE] Processed older: ${data}`);
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


   // if no pipelines, pause consumer
   // if (Object.keys(pipelines).length === 0) {
   //    isConsumerPaused = true;
   //    console.log(`[ERROR] No pipelines found. Pausing consumer...`);
   //    await consumer.pause([{ topic: MAP_TOPIC }]);
   // }
   consumer.run({
      eachMessage: async ({ message }) => {
         console.log(`[${MODE}/${WORKER_ID}] ${message.key?.toString()} ${message.value?.toString()}`)


         if (!message.key || !message.value) return;
         if (pipelineEnded(message)) {
            // Send stored values to reduce
            for (const key of Object.keys(keyValueStore)) {
               await producer.send({
                  topic: REDUCE_TOPIC,
                  messages: [{ key, value: JSON.stringify({ key, values: keyValueStore[key] }) }],
               });
               console.log(`[SHUFFLE MODE] Sending: ${key} -> ${JSON.stringify(keyValueStore[key])}`);
               return;
            }
         }

         const messageValue = JSON.parse(message.value.toString());
         const key = message.key.toString();

         if (!keyValueStore[key]) {
            keyValueStore[key] = [];
         }
         keyValueStore[key].push(messageValue);

         console.log(`[SHUFFLE MODE] Received: ${key} -> ${JSON.parse(messageValue)}`);

      },
   });
}

// Reduce mode: Applies the reduce function and forwards results
async function reduceMode() {
   await consumer.connect();
   await consumer.subscribe({ topic: REDUCE_TOPIC, fromBeginning: true });

   await producer.connect();


   // if no pipelines, pause consumer
   // if (Object.keys(pipelines).length === 0) {
   //    isConsumerPaused = true;
   //    console.log(`[ERROR] No pipelines found. Pausing consumer...`);
   //    await consumer.pause([{ topic: MAP_TOPIC }]);
   // }
   consumer.run({
      eachMessage: async ({ message }) => {
         console.log(`[${MODE}/${WORKER_ID}]`)
         if (!message.value) return;

         const messageValue = JSON.parse(message.value.toString());
         const pipelineID = messageValue.pipelineID;
         const pipelineConfig = pipelines[pipelineID];

         if (!pipelineConfig) {
            console.log(`[ERROR] No pipeline found for ID: ${pipelineID}`);
            return;
         }

         // At this point we are sure that the pipelineConfig is available, 
         // otherwise the message would not have been processed in map
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
         console.log(`[${MODE}/${WORKER_ID}]`)
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
