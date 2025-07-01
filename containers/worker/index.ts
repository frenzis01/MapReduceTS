/**
 * We are suppressing some errors related to installed modules,
 * Since we will run everything in Docker, so no problem with the installed modules.
 *  */
// @ts-ignore
import { Kafka, logLevel, KafkaMessage } from 'kafkajs';
// TODO does KafkaMessage really work?

// @ts-ignore
import { createClient } from 'redis';

// @ts-ignore
const MODE = process.argv[2];
// @ts-ignore
const GROUP_ID = process.env.GROUP_ID || 'default';

// if MODE or GROUP_ID are not provided, fail
if (!MODE || GROUP_ID == 'default') {
   // @ts-ignore
   process.exit(2);
}

// @ts-ignore
import {
   unboxKafkaMessage,
   newStreamEndedMessage,
   isStreamEnded,
   newMessageValue,
   newMessageValueShuffled,
   PipelineConfig,
   bitwiseHash,
   getPipelineID,
   STREAM_ENDED_KEY,
   PIPELINE_UPDATE_TOPIC,
   MAP_TOPIC,
   SHUFFLE_TOPIC,
   REDUCE_TOPIC,
   OUTPUT_TOPIC
   // @ts-ignore
} from "./utils";

const kafka = new Kafka({
   clientId: 'mapreduce',
   brokers: ['kafka:9092'],
   logLevel: logLevel.ERROR,
   requestTimeout: 25000,
   connectionTimeout: 3000,
});
const producer = kafka.producer({ allowAutoTopicCreation: false });
const consumer = kafka.consumer({ groupId: GROUP_ID });

let BUCKET_SIZE: number = 1;

const redis = createClient({
   url: 'redis://redis:6379' // Redis URL matches the service name in docker-compose
})

// Map mode: Applies the map function to incoming messages
async function mapMode() {

   console.log(`Started consuming messages`);
   // We are already connected to redis 
   // redis is needed for synchronization purposes

   await consumer.run({
      // TODO test this
      // partitionsConsumedConcurrently: 3, // Default: 1
      eachMessage: async ({ topic, message }: { topic: any, message: KafkaMessage }) => {

         try {

            if (!message.value || !message.key) {
               console.log(`[MAP/${WORKER_ID}] No message value or key found. Skipping...`);
               return;
            }

            const pipelineID = getPipelineID(message.key.toString());
            if (!pipelineID) return; // Throw error?
            const pipelineConfig = pipelines[pipelineID];

            // TODO pause consumer if no pipeline available
            if (!pipelineConfig) {
               return;
            }

            // TODO delay await?
            await processMessageMap(message, pipelineConfig);
         }
         catch (error) {
            if (error instanceof Error) {
               console.error(`[MAP/${WORKER_ID}] [${GROUP_ID}/${WORKER_ID}] ${error.message}`);
               if (error.stack) {
                  console.error(`[MAP/${WORKER_ID}] [${GROUP_ID}/${WORKER_ID}] ${error.stack}`);
               }
            } else {
               console.error(`[MAP/${WORKER_ID}] [${GROUP_ID}/${WORKER_ID}] Unknown error`, error);
            }
         }
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
   // We don't have to wait to propagate it as in shuffle.
   // We can propagate it immediately, and leave the synchronization to the shuffle container
   const pipelineID = pipelineConfig.pipelineID;
   if (isStreamEnded(message)) {


      // Increment the counter for the number of ended messages
      await redis.incr(`${pipelineID}-MAP-ENDED-counter`);

      // Dispatcher sends one STREAM_ENDED message for each partition, i.e. BUCKET_SIZE times
      // We need to wait for all the STREAM_ENDED messages to arrive before starting to send to shuffle
      const counter = await redis.get(`${pipelineID}-MAP-ENDED-counter`);
      if (!counter || Number(counter) !== BUCKET_SIZE) {
         // In case we have not yet received all the STREAM_ENDED messages, we simply return,
         // as we are not ready to send to shuffle yet, and we have already incremented the counter
         console.log(`[MAP/${WORKER_ID}] Received stream ended message. Got ${counter}/${BUCKET_SIZE} messages... for ${pipelineID}`);
         return;
      }
      else {
         // One of the mappers will receive the last STREAM_ENDED message from the dispatcher
         // and enter this else branch. Here, we propagate the STREAM_ENDED message to the shuffle,
         // One for each partition, i.e. BUCKET_SIZE times
         console.log(`[MAP/${WORKER_ID}] Received last STREAM_ENDED message. `);

         // Send to shuffle consumer special value to start feeding the reduce
         // Send onto all partitions
         for (let i = 0; i < BUCKET_SIZE; i++) {
            await producer.send({
               topic: `${SHUFFLE_TOPIC}`,
               messages: [{
                  key: `${pipelineID}__${STREAM_ENDED_KEY}`,
                  value: JSON.stringify(newStreamEndedMessage(val.pipelineID, null)),
                  partition: i,
               }],
            });
         }
         console.log(`[MAP/${WORKER_ID}] Propagated stream ended message to shuffle...`);

         // Pause and subscribe to newPipelines
         await subscribeToNewPipelines();
         return;
      }
   }
   const mapResults = pipelineConfig.mapFn(val.data);
   // mapResults is something like
   // [[{word: 'abg', count: 1}], [{word: 'abg', count: 1}], [{word: 'agg', count: 1}]]
   for (const arr of mapResults) {

      for (const result of arr) {
         const resData = pipelineConfig.dataSelector(result);
         const resKey = pipelineConfig.keySelector(result);
         // TODO this shold not be necessary, see below
         // const hashed = bitwiseHash(pipelineConfig.keySelector(result)) % BUCKET_SIZE;
         if (Math.random() < 0.0001) {
            console.log(`[MAP/${WORKER_ID}] Mapped: ${resKey} -> ${resData}`);
         }
         await producer.send({
            topic: `${SHUFFLE_TOPIC}`,
            // items with the same key should go to the same partition, i.e. the same shuffler
            // No need to flatten the keys since the #partitions is fixed
            messages: [{
               key: resKey,
               value: JSON.stringify(newMessageValue(resData, pipelineID)),
               // partition: hashed 
            }],
         });
      }
   }
}



/**
 * Shuffle mode: Groups messages by key for each stream/pipeline
 * When it receives a special message, it sends all stored values to the reduce topic
 */
async function shuffleMode() {
   // Connect and subscribe to Kafka topics
   await consumer.subscribe({ topic: `${SHUFFLE_TOPIC}`, fromBeginning: true });

   // We are already connected to redis 
   // redis is needed for synchronization purposes

   // For each pipelineID, we store the key-value pairs
   const keyValueStore: { [pipelineID: string]: { [key: string]: string[] } } = {};

   let counter = 0;

   await consumer.run({
      // partitionsConsumedConcurrently: 3, // Default: 1
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         if (!message.key || !message.value) return;

         counter++;

         const { key, val } = unboxKafkaMessage(message);
         const pipelineID = val.pipelineID;

         if (!pipelineID) return; // Throw error?

         // If not received this pipelineID before, add it 
         if (!keyValueStore[pipelineID]) {
            keyValueStore[pipelineID] = {};
         }

         if (isStreamEnded(message)) {

            // TODO improve the logic of these ifs
            if (!await redis.get(`${pipelineID}-SHUFFLE-READY-flag`)) {
               await redis.incr(`${pipelineID}-SHUFFLE-ENDED-counter`);
               const streamEndedCounter = await redis.get(`${pipelineID}-SHUFFLE-ENDED-counter`);
               console.log(`[SHUFFLE/${WORKER_ID}] Got ${streamEndedCounter} STREAM_ENDED messages...`);
            }
            const streamEndedCounter = await redis.get(`${pipelineID}-SHUFFLE-ENDED-counter`);
            if (!streamEndedCounter || Number(streamEndedCounter) < BUCKET_SIZE) {
               return;
            }
            // if reached the number of STREAM_ENDED messages, wake everyone with a stream Ended message 
            // having a flag set to make them recognize it as a dummy message
            if (!(await redis.get(`${pipelineID}-SHUFFLE-READY-flag`))) {
               await redis.set(`${pipelineID}-SHUFFLE-READY-flag`, `true`);
               for (let i = 0; i < BUCKET_SIZE; i++) {
                  await producer.send({
                     topic: `${SHUFFLE_TOPIC}`,
                     messages: [{
                        key: `${pipelineID}__${STREAM_ENDED_KEY}`,
                        value: JSON.stringify(newStreamEndedMessage(val.pipelineID, val.data)),
                        partition: i
                     }],
                  });
               }
            }

            // Avoid 'else' so that we send stuff after setting the flag
            if (await redis.get(`${pipelineID}-SHUFFLE-READY-flag`)) {
               const len = Object.keys(keyValueStore[pipelineID]).length;
               if (len === 0)
                  return;

               console.log(`[SHUFFLE/${WORKER_ID}] [STREAM_ENDED] Sending ${len} keys to reduce... from ${counter} messages`);

               await Promise.all(Object.keys(keyValueStore[pipelineID]).map(async (key) => {
                  const tmp = JSON.stringify(
                     newMessageValueShuffled(
                        // Flatten the array of arrays [[v1],[v2],...] -> [v1,v2,...]
                        keyValueStore[pipelineID][key].flat(),
                        pipelineID));
                  await producer.send({
                     topic: `${REDUCE_TOPIC}---${pipelineID}`,
                     messages: [{ "key": `${pipelineID}__shuffle-record__${key}`, "value": tmp }],
                  });

                  // print one message every 1000 messages (on average)
                  if (Math.random() < 0.01) {
                     console.log(`[SHUFFLE/${WORKER_ID}] Currently sending: ${key} -> ${tmp}`);
                  }

                  // Remove the key from keyValueStore
                  delete keyValueStore[pipelineID][key];
               }));
            }
            return;
         }
         if (!keyValueStore[pipelineID][key]) {
            keyValueStore[pipelineID][key] = [];
         }
         keyValueStore[pipelineID][key].push(val.data);

         if (Math.random() < 0.01) {
            console.log(`[SHUFFLE/${WORKER_ID}] Received: ${key} -> ${JSON.stringify(val.data)}`);
         }
      }
   });
}

// Reduce mode: Applies the reduce function and forwards results
async function reduceMode() {
   console.log(`Started consuming messages`);
   reduceStartedFlag = true;
   await consumer.run({
      // partitionsConsumedConcurrently: 3, // Default: 1
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         if (!message.key || !message.value) return;


         const pipelineID = getPipelineID(message.key.toString());

         if (!pipelineID) return; // TODO Throw error?
         const pipelineConfig = pipelines[pipelineID];

         // If no pipelineConfig is found, enqueue the message for later processing
         if (!pipelineConfig) {
            // TODO Error?
            return;
         }
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
   if (Math.random() < 0.0001) {
      console.log(`[REDUCE/${WORKER_ID}] Reduced: ${word} -> ${reducedResult}`);
   }

   await producer.send({
      topic: OUTPUT_TOPIC,
      messages: [{ "key": word, value: JSON.stringify(newMessageValue(reducedResult, val.pipelineID)) }],
   });
}

const getBucketSizeWrapper = async () => {
   await getBucketSizeWithRetry()
      .then(bucketSize => {
         console.log('BUCKET_SIZE:', bucketSize);
         BUCKET_SIZE = bucketSize;
      })
      .catch(error => {
         console.error(`[Bucket ERROR]`)
         console.error(error.message);
      });
}


async function getBucketSizeWithRetry(maxRetries = 5) {
   let retries = 0;

   await redis.connect();

   while (retries < maxRetries) {
      const value = await redis.get('BUCKET_SIZE');

      if (value !== null) {
         const size = parseInt(value);
         // if BUCKET_SIZE is not a positive integer, exit
         if (isNaN(BUCKET_SIZE) || BUCKET_SIZE <= 0) {
            console.error(`[ERROR] BUCKET_SIZE must be a positive integer`);
            // @ts-ignore
            process.exit(1);
         }
         return size; // Successfully retrieved the value
      }

      retries++;
      console.log(`Retry ${retries}/${maxRetries}: BUCKET_SIZE not set. Retrying in 1 second...`);

      await new Promise(resolve => setTimeout(resolve, 1000)); // Wait for 1 second
   }

   throw new Error('BUCKET_SIZE not set after maximum retries.');
}


/**
 * --------------------- COMMON AREA ---------------------
 * The following functions are used by both map and reduce.
 * BUCKET_SIZE functions are needed also by shuffle.
 * 
 * 
 * The process to subscribe to new topics requires the consumer NOT to be
 * in the running status. Besides, it is needed also to disconnect and reconnect:
 * skipping this step led to issues with duplicated messages.
 * 
 * - mappers check for new pipelines when a STREAM ENDS: if any new pipelines (topics)
 * are available, they stop, disconnect, subscribe and reconnect.
 * 
 * - shufflers do not need to receive pipeline updates, they need none of the functions
 * defined in PipelineConfig to work.
 * 
 * - reducers may instead stop at any time to disconnect, subscribe and reconnect. As soon as
 * a new pipeline is available, they perform the necessary steps.
 * 
 */
// Unique worker ID
const WORKER_ID = `${Math.random().toString(36).substring(2, 15)}`;

let pipelines: { [pipelineID: string]: PipelineConfig } = {};
let newPipelines: { [pipelineID: string]: boolean } = {};
let mapStartedFlag: boolean = false;
let reduceStartedFlag: boolean = false;
let pauseCounter: number = 4;
// Use unique worker id so that every consumer in map/reduce mode gets the pipeline updates
const pipelinesConsumer = kafka.consumer({ groupId: WORKER_ID });

// Listen for pipeline updates
async function listenForPipelineUpdates() {
   await pipelinesConsumer.connect();
   await pipelinesConsumer.subscribe({ topic: PIPELINE_UPDATE_TOPIC, fromBeginning: true });

   pipelinesConsumer.run({
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         try {
            console.log(`[Pipeline Update] [${GROUP_ID}/${WORKER_ID}] received msg with TS ${message.timestamp}`);
            if (!message.value) return;
            const { key, val } = unboxKafkaMessage(message);
            const pipelineConfig = JSON.parse(val.data.toString());
            pipelines[pipelineConfig.pipelineID] = {
               pipelineID: pipelineConfig.pipelineID,
               keySelector: eval(pipelineConfig.keySelector),
               dataSelector: eval(pipelineConfig.dataSelector),
               mapFn: eval(pipelineConfig.mapFn),
               reduceFn: eval(pipelineConfig.reduceFn),
            };

            const pipelineID = pipelineConfig.pipelineID;
            console.log(`[Pipeline Update] [${GROUP_ID}/${WORKER_ID}] New pipeline: ${JSON.stringify(pipelines[pipelineID])}`);
            newPipelines[pipelineID] = true;

            // When mappers have already started they automatically subscribe to new pipelines
            // when they reach the end of a stream
            if (MODE === '--map' && !mapStartedFlag) {
               await retryWrapper(subscribeToNewPipelines);
            }
            // Reducers instead subscribe to new pipelines as soon as they are available 
            if (MODE === '--reduce') {
               if (reduceStartedFlag) {
                  // if (pauseCounter === 4 || consumer.consumerGroup){
                     console.log(`Pausing ${pauseCounter}/${REDUCE_TOPIC}`);
                     await consumer.stop();
                     // pauseCounter = 3;
                  // }
                  if (pauseCounter >= 3){
                     console.log(`Disconnecting ${pauseCounter}/${REDUCE_TOPIC}`);
                     await consumer.disconnect();
                     pauseCounter = 2;
                  }
               }

               if(pauseCounter >= 2) {
                  console.log(`Connecting ${pauseCounter}/${REDUCE_TOPIC}`);
                  await consumer.connect();
                  pauseCounter = 1;
               }

               if (pauseCounter >= 1) {
                  console.log(`Subscribing ${pauseCounter}/${REDUCE_TOPIC}`);
                  await consumer.subscribe({ topic: `${REDUCE_TOPIC}---${pipelineID}`, fromBeginning: true });
                  console.log(`Correctly subscribed`);
                  pauseCounter = 4; // reset counter 
               }

               reduceMode();  // resume consuming messages
            }
         }
         catch (error) {
            if (error instanceof Error) {
               // console.error(`[Pipeline Update/ERROR] [${GROUP_ID}/${WORKER_ID}] ${error.message}`);
               if (error.stack) {
                  console.error(`[Pipeline Update/ERROR] [${GROUP_ID}/${WORKER_ID}]`);
                  console.error(error.stack);
               }
            } else {
               console.error(`[Pipeline Update/ERROR] [${GROUP_ID}/${WORKER_ID}] Unknown error`, error);
            }
         }
      },
   });
}

/**
 * Functions used by map to subscribe to topics of new pipelines
 */
async function subscribeToNewPipelines() {
   // Pause and subscribe to newPipelines
   if (Object.values(newPipelines).includes(true)) {
      // if (pauseCounter === 4 || consumer.consumerGroup) {
         console.log(`Pausing ${pauseCounter}/${MAP_TOPIC}`);
         await consumer.stop();
         // pauseCounter = 3;
      // }
      if (pauseCounter >= 3) {
         console.log(`Disconnecting ${pauseCounter}/${MAP_TOPIC}`);
         await consumer.disconnect();
         pauseCounter = 2;
      }
      if (pauseCounter >= 2) {
         console.log(`Connecting ${pauseCounter}/${MAP_TOPIC}`);
         await consumer.connect();
         pauseCounter = 1;
      }
      for (const id in newPipelines) {
         console.log(`Subscribing ${pauseCounter}/${MAP_TOPIC}---${id}`);
         await consumer.subscribe({ topic: `${MAP_TOPIC}---${id}`, fromBeginning: true });
         newPipelines[id] = false;
      }
      mapMode();  // resuming consuming messages
   }
}

async function retryWrapper (foo: Function) {
   let attempts = 0;
   const maxAttempts = 10;
   const delay = 300;

   while (attempts < maxAttempts) {
      try {
         return await foo(); // Exit the function if successful
      } catch (error: unknown) {
         attempts++;
         if (error instanceof Error) {
            console.error(`Attempt ${attempts} failed: ${error.message}`);
            if (attempts >= maxAttempts) {
               throw new Error(`Failed after ${maxAttempts} attempts: ${error.message}`);
            }
         } else {
            console.error(`Attempt ${attempts} failed: ${error}`);
            if (attempts >= maxAttempts) {
               throw new Error(`Failed after ${maxAttempts} attempts: ${error}`);
            }
         }
         await new Promise(resolve => setTimeout(resolve, delay));
      }
   }
}



async function main() {
   // We need redis for some shared state to synchronize on shuffling
   redis.on('error', (err: any) => console.error('Redis Client Error', err));

   if (MODE === '--map') {
      getBucketSizeWrapper()
      await consumer.connect();
      await producer.connect();
      await listenForPipelineUpdates();
   } else if (MODE === '--shuffle') {
      getBucketSizeWrapper()
      // This ain't necessary for shuffle
      // await listenForPipelineUpdates();
      await shuffleMode();
      await consumer.connect();
      await producer.connect();
   } else if (MODE === '--reduce') {
      await producer.connect();
      await listenForPipelineUpdates();
   } else {
      console.log('[ERROR] No valid mode provided. Use --map, --shuffle, or  --reduce.');
   }
}

// TODO implement disconnect policy
main().catch((error) => {
   console.error(`[ERROR] ${error.message}`);
   // If error is that kafka does not host the topic, wait and retry
   if (error.message.includes('This server does not host this topic-partition')) {
      setTimeout(() => {
         main();
      }, 5000);
   }
});