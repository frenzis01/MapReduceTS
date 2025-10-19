enum MessageType {
   STREAM_ENDED = 'STREAM_ENDED',
   STREAM_DATA = 'STREAM_DATA',
   STREAM_SHUFFLED_DATA = 'STREAM_SHUFFLED_DATA',
}

const DISPATCHER_TOPIC = 'dispatcher-topic';
const MAP_TOPIC = 'map-topic';
const SHUFFLE_TOPIC = 'shuffle-topic';
const REDUCE_TOPIC = 'reduce-topic';
const OUTPUT_TOPIC = 'output-topic';
const PIPELINE_UPDATE_TOPIC = 'pipeline-updates';


const STREAM_ENDED_KEY = 'STREAM_ENDED';
const STREAM_ENDED_VALUE = null;

// @ts-ignore
import { KafkaMessage } from 'kafkajs';

// Define unified messages value field format
interface MessageValue {
   pipelineID: string;
   type: MessageType;
   data: any;
}

function unboxKafkaMessage(msg: KafkaMessage) : {kkey: string, val: any } {
   if (!msg || !msg.value) throw new Error(`[ERROR] Message value is null`);
   const kkey = !msg.key ? "" : "" + msg.key.toString();
   if (isStreamEnded(msg)) return { kkey: kkey, val: JSON.parse(msg.value.toString()) };
   const value = JSON.parse(msg.value.toString());
   const pipelineID = value.pipelineID;
   const data = value.data;
   const type = value.type;
   const val: MessageValue = { pipelineID, type, data }
   return { kkey, val }
}

// numberOfMessages is used only by source, it's not crucial
function newStreamEndedMessage(pipelineID: string, numberOfMessages: Number | null = STREAM_ENDED_VALUE): MessageValue {
   return {
      pipelineID: pipelineID,
      type: MessageType.STREAM_ENDED,
      data: numberOfMessages
   };
}

function isStreamEndedKey(str: string): boolean {
   const regex = new RegExp(`^[^_]*[^_]__${STREAM_ENDED_KEY}$`);
   // The string must match the regex and there must be only one __delimiter
   return regex.test(str) && (str.split("__").length - 1 === 1);
}

function isStreamEnded(message: KafkaMessage): boolean {
   try {
      if (!message.key) return false;
      const tmp = isStreamEndedKey(message.key.toString());
      return tmp;
   } catch (error) {
      return false;
   }
}

// we cannot parameterize for more robust type checking,
// it would imply forcing types on the map and reduce signatures
interface PipelineConfig {
   pipelineID: string;
   keySelector: (message: any) => string;
   dataSelector: (message: any) => any;
   mapFn: (key: string, value: any) => any[];
   reduceFn: (key: string, values: any[]) => any[];
}

function newMessageValue(data: any, pipelineID: string): MessageValue {
   return {
      pipelineID: pipelineID,
      type: MessageType.STREAM_DATA,
      data: data
   };
}

function newMessageValueShuffled(data: any, pipelineID: string): MessageValue {
   return {
      pipelineID: pipelineID,
      type: MessageType.STREAM_SHUFFLED_DATA,
      data: data
   };
}

function newSourceRecord(pipelineID: string, key: number | string, data: any) {
   // if key is string, hash it
   if (typeof key === 'string') key = bitwiseHash(key);
   return {
      pipelineID: pipelineID,
      type: MessageType.STREAM_DATA,
      data: { key, data }
   };
}

function getPipelineID(input: string): string | null {
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

function newSourceKey(pipelineID: string, key: number) {
   return `${pipelineID}__source-record__${key}`;
}

function parseSourceKey(key: string): { pipelineID: string, keyStr: string } {
   const [pipelineID, keyStr] = key.split('__source-record__');
   return { pipelineID, keyStr };
}

function stringifyPipeline(pipeline: PipelineConfig): string {
   const tmp = {
      pipelineID: pipeline.pipelineID,
      keySelector: pipeline.keySelector.toString(),
      dataSelector: pipeline.dataSelector.toString(),
      mapFn: pipeline.mapFn.toString(),
      reduceFn: pipeline.reduceFn.toString(),
   }
   return JSON.stringify(tmp);
}

/**
 * 
 * @param s 
 * @returns bitwise hash of the string
 */
function bitwiseHash(s: string) {
   let hash = 0;

   if (s.length === 0) return hash;

   for (const char of s) {
      hash ^= char.charCodeAt(0); // Bitwise XOR operation
   }

   return hash;
}

export {
   MessageType,
   MessageValue,
   unboxKafkaMessage,
   newStreamEndedMessage,
   isStreamEnded,
   newMessageValue,
   newMessageValueShuffled,
   PipelineConfig,
   stringifyPipeline,
   bitwiseHash,
   getPipelineID,
   newSourceRecord,
   newSourceKey,
   parseSourceKey,
   STREAM_ENDED_KEY,
   DISPATCHER_TOPIC,
   PIPELINE_UPDATE_TOPIC,
   MAP_TOPIC,
   SHUFFLE_TOPIC,
   REDUCE_TOPIC,
   OUTPUT_TOPIC,
}