/**
 * We are suppressing some errors related to installed modules,
 * Since we will run everything in Docker, so no problem with the installed modules.
 *  */
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
         console.log(JSON.stringify(Object.keys(unprocessedMessages)));
         console.log(`${pipelineConfig.pipelineID} | ${!unprocessedMessages[pipelineConfig.pipelineID]}`);
         // TODO remove prints
         if (MODE === '--map' && unprocessedMessages[pipelineConfig.pipelineID]) {
            processUnprocessedMessages(pipelineConfig, processMessageMap);
         }
         else if (MODE === '--reduce' && unprocessedMessages[pipelineConfig.pipelineID]) {
            processUnprocessedMessages(pipelineConfig, processMessageReduce)
         }

      },
   });
}

function processUnprocessedMessages(pipelineConfig: PipelineConfig, callback: (message: KafkaMessage, pipelineConfig: PipelineConfig) => any) {
   const pipelineID = pipelineConfig.pipelineID;
   console.log(`[MAP MODE] Processing unprocessed messages for pipeline: ${pipelineID} | ${unprocessedMessages[pipelineID]?.length}`);
   // TODO make this async?
   while (unprocessedMessages[pipelineID].length > 0) {
      const msg = unprocessedMessages[pipelineID].shift(); // consume the first item
      if (!msg) { // TODO should not happen... right? 
         console.log(`[ERROR] No message found in unprocessedMessages`);
         return;
      }
      console.log(`[MAP MODE] Processing unprocessed message: ${msg.timestamp}`);
      callback(msg, pipelines[pipelineID]);
   }
}

let unprocessedMessages: { [pipelineID: string]: KafkaMessage[] } = {}; // Queue for messages with missing pipelineConfig

function enqueueUnprocessedMessage(message: KafkaMessage, pipelineID: string) {
   // TODO pause consumer if no pipeline available
   console.log(`[${WORKER_ID}][ERROR] No pipeline found for ID: ${pipelineID}. Delaying message processing`);
   // Add entry to unprocessedMessages queue if missing
   if (!unprocessedMessages[pipelineID]) {
      unprocessedMessages[pipelineID] = [];
   }

   // Add message to queue
   unprocessedMessages[pipelineID].push(message);
   // TODO check ordering of push+foreach

   return; // Skip processing this message for now
}


function getPipelineID(input: string): string | null {
   // const splitPattern = new RegExp(`__(source-record__\\d+|${STREAM_ENDED_KEY})$`);
   // Regular expression to match either 
   // - '__source-record__1325' or 
   // - '__STREAM_ENDED_KEY'
   // - '__shuffle-record__key'
   // with key being any string that does not contain a double underscore __
   const splitPattern = new RegExp(
      `__(source-record__\\d+|${STREAM_ENDED_KEY}|shuffle-record__[^_]+(?:_[^_]+)*)$`
   );
   // Split the input string using the regular expression
   const parts = input.split(splitPattern);

   // the expected content parts is: [pipelineID, delimiter, ""]
   // Note the empty string at the end
   // The pipelineID is the first part if the split was successful
   return parts.length === 3 ? parts[0] : null;
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
         if (!message.value || !message.key) {
            console.log(`[MAP MODE] No message value or key found. Skipping...`);
            return;
         }

         const pipelineID = getPipelineID(message.key.toString());
         if (!pipelineID) return; // Throw error?
         const pipelineConfig = pipelines[pipelineID];


         // TODO need to handle stream_ended_key
         // TODO pause consumer if no pipeline available
         if (!pipelineConfig) {
            enqueueUnprocessedMessage(message, pipelineID);
            return;
         }

         await processMessageMap(message, pipelineConfig);
      },
   });
}

/**
 * to be invoked on a message value ready to be processed in map mode
 * We enforce this by explicitly passing the pipelineConfig.
 * @param pipelineConfig this is here only to enforce that the pipelineConfig is passed in and should be available when processing
 */
async function processMessageMap(message: KafkaMessage, pipelineConfig: PipelineConfig) {
   const { key, val } = unboxKafkaMessage(message);
   if (isStreamEnded(message)) {
      console.log(`[MAP MODE] Received stream ended message. `);

      console.log(`[MAP MODE] Propagating stream ended message to shuffle...`);
      // Send to shuffle consumer special value to start feeding the reduce
      await producer.send({
         topic: SHUFFLE_TOPIC,
         messages: [{ key: `${pipelineConfig.pipelineID}__${STREAM_ENDED_KEY}`, value: JSON.stringify(newStreamEndedMessage(val.pipelineID)), }],
      });
      return;
   }

   const mapResults = pipelineConfig.mapFn(val.data);
   for (const result of mapResults) {
      await producer.send({
         topic: SHUFFLE_TOPIC,
         messages: [{ key: pipelineConfig.keySelector(result), value: JSON.stringify(newMessageValue(result, pipelineConfig.pipelineID)) }],
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
               const tmp = JSON.stringify(newMessageValueShuffled(keyValueStore[pipelineID][key], pipelineID));
               await producer.send({
                  topic: REDUCE_TOPIC,
                  messages: [{ "key": `${pipelineID}__shuffle-record__${key}`, "value": tmp }],
               });
               console.log(tmp);
               console.log(`[SHUFFLE MODE] Sending: ${key} -> ${tmp}`);
            }
            return;
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
         if (!message.key || !message.value) return;
         console.log(`[${MODE}/${WORKER_ID}] -> ${(message.key.toString())}`)


         const pipelineID = getPipelineID(message.key.toString());
         console.log("reduce 00");

         if (!pipelineID) return; // Throw error?
         const pipelineConfig = pipelines[pipelineID];
         console.log("reduce 10");
         // At this point we are almost sure that the pipelineConfig is available, 
         // otherwise the message would not have been processed in map, there must have
         // been a huge delay or some other issue related to the pipeline update message 
         if (!pipelineConfig) {
            console.log("reduce 20");
            enqueueUnprocessedMessage(message, pipelineID);
            return;
         }
         console.log("reduce 30");
         processMessageReduce(message, pipelineConfig);
      },
   });
}

async function processMessageReduce(message: KafkaMessage, pipelineConfig: PipelineConfig) {
   const { key, val } = unboxKafkaMessage(message);

   if (key.toString().split('__').length !== 3) {
      return;
      // TODO throw error?
   }
   // Trim to get from the second __ till the end, i.e. the word
   // TODO ensure safety
   const word = key.split('__')[2];
   const reducedResult = pipelineConfig.reduceFn(word, val.data);
   console.log(`[REDUCE MODE] Reduced: ${word} -> ${reducedResult}`);

   await producer.send({
      topic: OUTPUT_TOPIC,
      messages: [{ "key": word, value: JSON.stringify(newMessageValue(reducedResult, val.pipelineID)) }],
   });
}

async function main() {
   if (MODE === '--map') {
      await listenForPipelineUpdates();
      await mapMode();
   } else if (MODE === '--shuffle') {
      // This ain't necessary for shuffle
      // await listenForPipelineUpdates();
      await shuffleMode();
   } else if (MODE === '--reduce') {
      await listenForPipelineUpdates();
      await reduceMode();
   } else {
      console.log('[ERROR] No valid mode provided. Use --map, --shuffle, or  --reduce.');
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