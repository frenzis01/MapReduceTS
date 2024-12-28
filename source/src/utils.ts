// TODO clean this file from useless functions

enum MessageType {
   STREAM_ENDED = 'STREAM_ENDED',
   STREAM_DATA = 'STREAM_DATA',
   STREAM_SHUFFLED_DATA = 'STREAM_SHUFFLED_DATA',
}

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

function unboxKafkaMessage(msg: KafkaMessage) {
   // TODO kind of ugly
   // msg.value is not null when stream ends...
   // Perhaps the null I inject is inside msg.value.data, not msg.value
   if (!msg || !msg.value) throw new Error(`[ERROR] Message value is null`);
   // TODO is this default empty string necessary?
   const key = !msg.key ? "" : "" + msg.key.toString();
   if (isStreamEnded(msg)) return { key: key, val: JSON.parse(msg.value.toString()) };
   // TODO Why is toString() necessary? If i put raw data when sending i get an error.
   const value = JSON.parse(msg.value.toString());
   const pipelineID = value.pipelineID;
   // const pipelineConfig = pipelines[pipelineID];
   const data = value.data;   // JSON.parse(value.data) should not be necessary
   const type = value.type;
   const val: MessageValue = { pipelineID, type, data }
   return { key, val }
}


function newStreamEndedMessage(pipelineID: string, numberOfMessages: any = STREAM_ENDED_VALUE): MessageValue {
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
      if (tmp) console.log(`[STREAM_ENDED] Stream ${message.key.toString().split('__')[0]} ended`);
      return tmp;
   } catch (error) {
      return false;
   }
}

// TODO we would like to parameterize for more robust type checking
// Perhaps we cannot do it since this would break the "pipelines" definition
// i.e. having a collection of heterogeneous pipelines
interface PipelineConfig {
   pipelineID: string;
   keySelector: (message: any) => string;
   mapFn: (value: any) => any[];
   reduceFn: (key: string, values: any[]) => any;
}

function newMessageValue(data: any, pipelineID: string): MessageValue {
   return {
      pipelineID: pipelineID,
      type: MessageType.STREAM_DATA,
      data: data
   };
}

// TODO JSON stringify here or outside?
function newMessageValueShuffled(data: any, pipelineID: string): MessageValue {
   return {
      pipelineID: pipelineID,
      type: MessageType.STREAM_SHUFFLED_DATA,
      data: data
   };
}


function stringifyPipeline(pipeline: PipelineConfig): string {
   const tmp = {
      pipelineID: pipeline.pipelineID,
      keySelector: pipeline.keySelector.toString(),
      mapFn: pipeline.mapFn.toString(),
      reduceFn: pipeline.reduceFn.toString(),
   }
   return JSON.stringify(tmp);
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
   STREAM_ENDED_KEY
}