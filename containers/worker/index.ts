/**
 * //TODO implement manual offset commit
 *
 * */ 

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
const GROUP_ID = process.env.GROUP_ID || 'default-group';
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
   logLevel: logLevel.NOTHING
});
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: GROUP_ID });

let BUCKET_SIZE: number = 1;

const redis = createClient({
   url: 'redis://redis:6379' // Redis URL matches the service name in docker-compose
})

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


// Map mode: Applies the map function to incoming messages
async function mapMode() {
   // Connect and subscribe to Kafka topics
   await consumer.connect();
   await consumer.subscribe({ topic: MAP_TOPIC, fromBeginning: true });

   await producer.connect();

   const admin = await kafka.admin();
   await admin.connect();

   // We are already connected to redis 
   // redis is needed for synchronization purposes

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
   // We don't have to wait to propagate it as in shuffle.
   // We can propagate it immediately, and leave the synchronization to the shuffle container
   if (isStreamEnded(message)) {


      // Increment the counter for the number of ended messages
      await redis.incr(`${pipelineConfig.pipelineID}-MAP-ENDED-counter`);

      // Dispatcher sends one STREAM_ENDED message for each partition, i.e. BUCKET_SIZE times
      // We need to wait for all the STREAM_ENDED messages to arrive before starting to send to shuffle
      const counter = await redis.get(`${pipelineConfig.pipelineID}-MAP-ENDED-counter`);
      if (!counter || Number(counter) !== BUCKET_SIZE) {
         // In case we have not yet received all the STREAM_ENDED messages, we simply return,
         // as we are not ready to send to shuffle yet, and we have already incremented the counter
         console.log(`[MAP MODE] Received stream ended message. Got ${counter}/${BUCKET_SIZE} messages...`);
         return;
      }
      else {
         // One of the mappers will receive the last STREAM_ENDED message from the dispatcher
         // and enter this else branch. Here, we propagate the STREAM_ENDED message to the shuffle,
         // One for each partition, i.e. BUCKET_SIZE times
         console.log(`[MAP MODE] Received last STREAM_ENDED message. `);

         // Send to shuffle consumer special value to start feeding the reduce
         // Send onto all partitions
         for (let i = 0; i < BUCKET_SIZE; i++) {
            await producer.send({
               topic: SHUFFLE_TOPIC,
               messages: [{
                  key: `${pipelineConfig.pipelineID}__${STREAM_ENDED_KEY}`,
                  value: JSON.stringify(newStreamEndedMessage(val.pipelineID, null)),
                  partition: i,
               }],
            });
         }
         console.log(`[MAP MODE] Propagated stream ended message to shuffle...`);
         return;
      }
   }

   const mapResults = pipelineConfig.mapFn(val.data);
   for (const result of mapResults) {
      const hashed = bitwiseHash(pipelineConfig.keySelector(result)) % BUCKET_SIZE;
      await producer.send({
         topic: SHUFFLE_TOPIC,
         // items with the same key should go to the same partition, i.e. the same shuffler
         // consider flattening the keys to avoid too many partitions
         // TODO hash the key and use the hash and modulo BUCKET_SIZE to get partition
         messages: [{ key: pipelineConfig.keySelector(result), value: JSON.stringify(newMessageValue(result, pipelineConfig.pipelineID)) }],
      });
   }
}


/**
 * 
 * Function used for debug purposes to print the metadata of a topic
 * 
 * @param admin 
 * @param producer 
 * @param topic 
 * @param pipelineID 
 */
async function printTopicMetadata(admin: any, producer: any, topic: string, pipelineID: string) {

   const topicsMetadata = await admin.fetchTopicMetadata({ "topics": [topic] });
   console.log("[METADATA] printing topics metadata");

   const topics = await admin.listTopics();
   console.log(JSON.stringify(topics));
   await producer.send({
      topic: OUTPUT_TOPIC,
      messages: [{ key: "TOPIC_METADATA", value: JSON.stringify(newMessageValue(JSON.stringify(topicsMetadata), pipelineID)) }]
   })
}



/**
 * Shuffle mode: Groups messages by key for each stream/pipeline
 * When it receives a special message, it sends all stored values to the reduce topic
 */
async function shuffleMode() {
   // Connect and subscribe to Kafka topics
   
   await consumer.connect();
   await consumer.subscribe({ topic: SHUFFLE_TOPIC, fromBeginning: true });
   
   await producer.connect();
   
   // We are already connected to redis 
   // redis is needed for synchronization purposes
   
   // For each pipelineID, we store the key-value pairs
   const keyValueStore: { [pipelineID: string]: { [key: string]: string[] } } = {};

   let counter = 0;

   consumer.run({
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         console.log(`[${MODE}/${WORKER_ID}] -> ${!message.key || !message.value} | ${message.key?.toString()} ${message.value?.toString()}`)
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

            console.log(`[SHUFFLE MODE] Received stream ended message for pipeline: ${pipelineID}`);
            // TODO improve the logic of these ifs
            if (!await redis.get(`${pipelineID}-SHUFFLE-READY-flag`)) {
               await redis.incr(`${pipelineID}-SHUFFLE-ENDED-counter`);
               const streamEndedCounter = await redis.get(`${pipelineID}-SHUFFLE-ENDED-counter`);
               console.log(`[SHUFFLE MODE] Got ${streamEndedCounter} STREAM_ENDED messages...`);
            }
            // // TODO get bucket size from dispatcher?
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
                     topic: SHUFFLE_TOPIC,
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


               console.log(`[SHUFFLE MODE] [STREAM_ENDED] Sending ${Object.keys(keyValueStore[pipelineID]).length} keys to reduce... from ${counter} messages`);


               await Promise.all(Object.keys(keyValueStore[pipelineID]).map(async (key) => {
                  // Remove tmp
                  const tmp = JSON.stringify(newMessageValueShuffled(keyValueStore[pipelineID][key], pipelineID));
                  await producer.send({
                     topic: REDUCE_TOPIC,
                     messages: [{ "key": `${pipelineID}__shuffle-record__${key}`, "value": tmp }],
                  });
                  console.log(tmp);
                  console.log(`[SHUFFLE MODE] Sending: ${key} -> ${tmp}`);
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

         console.log(`[SHUFFLE MODE] Received: ${key} -> ${JSON.stringify(val.data)}`);

      }
   });
}

// Reduce mode: Applies the reduce function and forwards results
async function reduceMode() {
   // Connect and subscribe to Kafka topics
   await consumer.connect();
   await consumer.subscribe({ topic: REDUCE_TOPIC, fromBeginning: true });

   await producer.connect();

   consumer.run({
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         if (!message.key || !message.value) return;
         console.log(`[${MODE}/${WORKER_ID}] -> ${(message.key.toString())}`)


         const pipelineID = getPipelineID(message.key.toString());

         if (!pipelineID) return; // TODO Throw error?
         const pipelineConfig = pipelines[pipelineID];

         // If no pipelineConfig is found, enqueue the message for later processing
         if (!pipelineConfig) {
            enqueueUnprocessedMessage(message, pipelineID);
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
   console.log(`[REDUCE MODE] Reduced: ${word} -> ${reducedResult}`);

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
         console.error(error.message);
      });
}

async function main() {
   // We need redis for some shared state to synchronize on shuffling
   redis.on('error', (err: any) => console.error('Redis Client Error', err));

   if (MODE === '--map') {
      getBucketSizeWrapper()
      await listenForPipelineUpdates();
      await mapMode();
   } else if (MODE === '--shuffle') {
      getBucketSizeWrapper()
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