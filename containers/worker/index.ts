/**
 * We are suppressing some errors related to installed modules,
 * Since we will run everything in Docker, so no problem with the installed modules.
 *  */
// @ts-ignore
import { Kafka, logLevel, KafkaMessage } from 'kafkajs';

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

// counter for received messages
let counter = 0;
let messagesPerPipeline: { [key: string]: number } = {};
let outgoingMessages: { [key: string]: number } = {};

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
   await pipelinesConsumer.connect();
   await pipelinesConsumer.subscribe({ topic: PIPELINE_UPDATE_TOPIC, fromBeginning: true });

   pipelinesConsumer.run({
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         if (!message.value) return;
         const { kkey, val } = unboxKafkaMessage(message);
         const pipelineConfig = JSON.parse(val.data.toString());
         pipelines[pipelineConfig.pipelineID] = {
            pipelineID: pipelineConfig.pipelineID,
            keySelector: eval(pipelineConfig.keySelector),
            dataSelector: eval(pipelineConfig.dataSelector),
            mapFn: eval(pipelineConfig.mapFn),
            reduceFn: eval(pipelineConfig.reduceFn),
         };

         // Initialize counters for this pipeline
         messagesPerPipeline[pipelineConfig.pipelineID] = 0;
         outgoingMessages[pipelineConfig.pipelineID] = 0;

         await redis.set(`${pipelineConfig.pipelineID}-REDUCE-received-counter`, '0');

         console.log(`[Pipeline Update] [${GROUP_ID}/${WORKER_ID}] Updated pipeline: ${pipelineConfig.pipelineID}`);
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
   while (unprocessedMessages[pipelineID].length > 0) {
      const msg = unprocessedMessages[pipelineID].shift(); // consume the first item
      if (!msg) { // should not happen...
         console.log(`[ERROR] No message found in unprocessedMessages`);
         return;
      }
      console.log(`[MAP MODE] Processing unprocessed message: ${msg.timestamp}`);
      callback(msg, pipelines[pipelineID]);
   }
}

let unprocessedMessages: { [pipelineID: string]: KafkaMessage[] } = {}; // Queue for messages with missing pipelineConfig

function enqueueUnprocessedMessage(message: KafkaMessage, pipelineID: string) {
   console.log(`[${WORKER_ID}][ERROR] No pipeline found for ID: ${pipelineID}. Delaying message processing`);
   // Add entry to unprocessedMessages queue if missing
   if (!unprocessedMessages[pipelineID]) {
      unprocessedMessages[pipelineID] = [];
   }

   // Add message to queue
   unprocessedMessages[pipelineID].push(message);

   return; // Skip processing this message for now
}


// Map mode: Applies the map function to incoming messages
async function mapMode() {
   // Connect and subscribe to Kafka topics
   await consumer.connect();
   await consumer.subscribe({ topic: MAP_TOPIC, fromBeginning: true });

   await producer.connect();

   // We are already connected to redis 
   // redis is needed for synchronization purposes

   consumer.run({
      
      partitionsConsumedConcurrently: 3, // Default: 1
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         counter++;
         if (!message.value || !message.key) {
            return;
         }
         
         const pipelineID = getPipelineID(message.key.toString());
         if (!pipelineID) return; // Throw error?
         const pipelineConfig = pipelines[pipelineID];
         
         
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
   const { kkey, val } = unboxKafkaMessage(message);

   const pipelineID = pipelineConfig.pipelineID;

   // We don't have to wait to propagate it as in shuffle.
   // We can propagate it immediately, and leave the synchronization to the shuffle container
   if (isStreamEnded(message)) {

      // Increment the counter for the number of ended messages
      await redis.incr(`${pipelineID}-MAP-ENDED-counter`);

      // `if` statements to avoid unnecessary redis calls
      if (messagesPerPipeline[pipelineID] > 0)
         await redis.incrBy(`${pipelineID}-MAP-received-counter`, messagesPerPipeline[pipelineID]);
      if (outgoingMessages[pipelineID] > 0)
         await redis.incrBy(`${pipelineID}-MAP-outgoing-counter`, outgoingMessages[pipelineID]);

      // Reset the counters for this pipeline to avoid double counting in redis
      messagesPerPipeline[pipelineID] = 0;
      outgoingMessages[pipelineID] = 0;

      // Dispatcher sends one STREAM_ENDED message for each partition, i.e. BUCKET_SIZE times
      // We need to wait for all the STREAM_ENDED messages to arrive before starting to send to shuffle
      const streamEndedCounter = await redis.get(`${pipelineID}-MAP-ENDED-counter`);
      if (!streamEndedCounter || Number(streamEndedCounter) !== BUCKET_SIZE) {
         // In case we have not yet received all the STREAM_ENDED messages, we simply return,
         // as we are not ready to send to shuffle yet, and we have already incremented the streamEndedCounter
         console.log(`[MAP MODE] Got ${streamEndedCounter}/${BUCKET_SIZE} STREAM_ENDED messages... for ${pipelineID}`);
         return;
      }
      else {
         // One of the mappers will receive the last STREAM_ENDED message from the dispatcher
         // and enter this else branch. Here, we propagate the STREAM_ENDED message to the shuffle,
         // One for each partition, i.e. BUCKET_SIZE times
         
         const expectedMessages = val.data;
         const receivedMessages = await redis.get(`${pipelineID}-MAP-received-counter`);
         console.log(`[MAP MODE] Received all STREAM_ENDED messages. Got ${receivedMessages}/${expectedMessages} messages... for ${pipelineID}`);

         const outgoingMessagesCount = Number(await redis.get(`${pipelineID}-MAP-outgoing-counter`));
         console.log(`[MAP MODE] Sent ${outgoingMessagesCount} messages to shuffle...`);

         // Send to shuffle consumer special value to start feeding the reduce
         // Send onto all partitions
         for (let i = 0; i < BUCKET_SIZE; i++) {
            await producer.send({
               topic: SHUFFLE_TOPIC,
               messages: [{
                  key: `${pipelineID}__${STREAM_ENDED_KEY}`,
                  value: JSON.stringify(newStreamEndedMessage(val.pipelineID, outgoingMessagesCount)),
                  partition: i,
               }],
            });
         }
         console.log(`[MAP MODE] Propagated stream ended message to shuffle...`);
         return;
      }
   }

   messagesPerPipeline[pipelineID]++;
   const mapResults = pipelineConfig.mapFn(val.data);


   for (const arr of mapResults) {
      outgoingMessages[pipelineID] += arr.length;

      for (const result of arr) {
         const key = pipelineConfig.keySelector(result);
         const data = pipelineConfig.dataSelector(result);

         // every thousand messages, we log the progress
         if (counter % 1000 == 0) {
         console.log(`[MAP MODE] Processing messages...`);
         console.log(`[MAP MODE] Mapping data... Mapped: ${key} -> ${data}`);
         }

         await producer.send({
            topic: SHUFFLE_TOPIC,
            // items with the same key should go to the same partition, i.e. the same shuffler
            messages: [{ key: key, value: JSON.stringify(newMessageValue(data, pipelineID)) }],
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
   
   await consumer.connect();
   await consumer.subscribe({ topic: SHUFFLE_TOPIC, fromBeginning: true });
   
   await producer.connect();
   
   // We are already connected to redis 
   // redis is needed for synchronization purposes
   
   // For each pipelineID, we store the key-value pairs
   const keyValueStore: { [pipelineID: string]: { [key: string]: string[] } } = {};

   consumer.run({
      // This breaks the logic behind synchronizing
      // partitionsConsumedConcurrently: 3, // Default: 1
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         if (!message.key || !message.value) return;

         counter++;

         const { kkey, val } = unboxKafkaMessage(message);
         const pipelineID : string = val.pipelineID;
         const key = kkey;

         if (!pipelineID) return; // Throw error?

         // If not received this pipelineID before, add it 
         if (!keyValueStore[pipelineID]) {
            keyValueStore[pipelineID] = {};
            messagesPerPipeline[pipelineID] = 0;
            outgoingMessages[pipelineID] = 0;
         }

         if (isStreamEnded(message)) {
            
            // Increment redis counters
            if (messagesPerPipeline[pipelineID] > 0)
               await redis.incrBy(`${pipelineID}-SHUFFLE-received-counter`, messagesPerPipeline[pipelineID]);
            if (outgoingMessages[pipelineID] > 0)
               await redis.incrBy(`${pipelineID}-SHUFFLE-outgoing-counter`, outgoingMessages[pipelineID]);
            // Reset counter to avoid double counting in redis
            messagesPerPipeline[pipelineID] = 0;
            outgoingMessages[pipelineID] = 0;

            if (!await redis.get(`${pipelineID}-SHUFFLE-READY-flag`)) {
               await redis.incr(`${pipelineID}-SHUFFLE-ENDED-counter`);
               const streamEndedCounter = await redis.get(`${pipelineID}-SHUFFLE-ENDED-counter`);
               console.log(`[SHUFFLE MODE] Got ${streamEndedCounter}/${BUCKET_SIZE} STREAM_ENDED messages... for ${pipelineID}`);
            }
            const streamEndedCounter = await redis.get(`${pipelineID}-SHUFFLE-ENDED-counter`);
            if (!streamEndedCounter || Number(streamEndedCounter) < BUCKET_SIZE) {
               return;
            }
            // if reached the number of STREAM_ENDED messages, wake everyone with a stream Ended message 
            // having a flag set to make them recognize it as a dummy message
            if (!(await redis.get(`${pipelineID}-SHUFFLE-READY-flag`))) {
               await redis.set(`${pipelineID}-SHUFFLE-READY-flag`, `true`);

               const receivedMessages = await redis.get(`${pipelineID}-SHUFFLE-received-counter`);
               const expectedMessages = val.data;
               console.log(`[SHUFFLE MODE] Received all STREAM_ENDED messages. Got ${receivedMessages}/${expectedMessages} messages... for ${pipelineID}`);

               const outgoingMessagesCount = Number(await redis.get(`${pipelineID}-SHUFFLE-outgoing-counter`));

               for (let i = 0; i < BUCKET_SIZE; i++) {
                  await producer.send({
                     topic: SHUFFLE_TOPIC,
                     messages: [{
                        key: `${pipelineID}__${STREAM_ENDED_KEY}`,
                        value: JSON.stringify(newStreamEndedMessage(val.pipelineID, outgoingMessagesCount)),
                        partition: i
                     }],
                  });
               }
            }

            // Avoid 'else' so that we send stuff after setting the flag
            if (await redis.get(`${pipelineID}-SHUFFLE-READY-flag`)) {

               const numKeys = Object.keys(keyValueStore[pipelineID]).length;
               if (numKeys){
                  console.log(`[SHUFFLE MODE] [STREAM_ENDED] Sending ${numKeys} keys to reduce...`);
               }


               await Promise.all(Object.keys(keyValueStore[pipelineID]).map(async (key) => {
                  // Remove tmp
                  const tmp = JSON.stringify(newMessageValueShuffled(keyValueStore[pipelineID][key], pipelineID));
                  await producer.send({
                     topic: REDUCE_TOPIC,
                     messages: [{ "key": `${pipelineID}__shuffle-record__${key}`, "value": tmp }],
                  });
                  // Remove the key from keyValueStore
                  delete keyValueStore[pipelineID][key];
               }));

               await redis.incr(`${pipelineID}-SHUFFLE-ENDED-counter`);
            }

            if (Number(streamEndedCounter) === BUCKET_SIZE * 2) {
               // All shufflers have sent their records to reduce
               // Send stream ended to reduce
               const outgoingMessagesCount = Number(await redis.get(`${pipelineID}-SHUFFLE-outgoing-counter`));
               console.log(`[SHUFFLE MODE] Sent ${outgoingMessagesCount} messages to reduce... for ${pipelineID}`);
               for (let i = 0; i < BUCKET_SIZE; i++) {
                  await producer.send({
                     topic: REDUCE_TOPIC,
                     messages: [{
                        key: `${pipelineID}__${STREAM_ENDED_KEY}`,
                        value: JSON.stringify(newStreamEndedMessage(val.pipelineID, outgoingMessagesCount)),
                        partition: i
                     }],
                  });
               }
            }
            return;
         }
         if (!keyValueStore[pipelineID][key]) {
            keyValueStore[pipelineID][key] = [];
            // Each corresponds to a record to be sent to reduce
            outgoingMessages[pipelineID]++;
         }
         keyValueStore[pipelineID][key].push(val.data);

         // increment counters
         messagesPerPipeline[pipelineID]++;

         if (counter % 1000 == 0) {
            console.log(`[SHUFFLE MODE] Receiving messages... Just got: ${key} -> ${JSON.stringify(val.data)}`);
         }

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
      partitionsConsumedConcurrently: 1, // Default: 1
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
         counter++;
         if (!message.key || !message.value) return;


         const pipelineID = getPipelineID(message.key.toString());

         if (!pipelineID) return;
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
   const { kkey, val } = unboxKafkaMessage(message);
   const pipelineID = pipelineConfig.pipelineID;

   if (isStreamEnded(message)) {
      /**
       * This whole `if block is used only to check the number of
       * messages received vs expected, and to print it out.
       **/

      // get and reset counter
      const msg = messagesPerPipeline[pipelineID];
      const outgoing = outgoingMessages[pipelineID];
      messagesPerPipeline[pipelineID] = 0;
      outgoingMessages[pipelineID] = 0;

      if (msg > 0)
         await redis.incrBy(`${pipelineID}-REDUCE-received-counter`, msg);
      
      if (outgoing > 0)
         await redis.incrBy(`${pipelineID}-REDUCE-outgoing-counter`, outgoing);
   
      await redis.incr(`${pipelineID}-REDUCE-ENDED-counter`);
      const streamEndedCounter = Number(await redis.get(`${pipelineID}-REDUCE-ENDED-counter`));
      

      if (streamEndedCounter === BUCKET_SIZE) {
         const { kkey, val } = unboxKafkaMessage(message);

         const expectedMessages = val.data;
         const receivedMessages = await redis.get(`${pipelineID}-REDUCE-received-counter`);
         // Could still happen that here are printed less messages than the actual ones,
         // but everything looks fine in the output file
         console.log(`[REDUCE MODE] Received all ${pipelineID} STREAM_ENDED messages. Got ${receivedMessages}/${expectedMessages} messages`);
      }
      return;
   }
   
   if (kkey.toString().split('__').length !== 3) {
      console.error(`[REDUCE MODE] Invalid key format: ${kkey}`);
      return;
   }

   messagesPerPipeline[pipelineID]++;
   outgoingMessages[pipelineID]++;  
   // we are not actually using outgoingMessages, the sink does not count neither check
   // We can rely on kafka message retransmission for that

   // Trim to get from the second __ till the end, i.e. the word
   const word = kkey.split('__')[2];
   const reducedResult = pipelineConfig.reduceFn(word, val.data);
   
   if (counter % 300 == 0) {
      console.log(`[REDUCE MODE] Reducing data... Reduced: ${word} -> ${reducedResult}`);
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
      getBucketSizeWrapper()
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