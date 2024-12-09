
enum MessageType {
   STREAM_ENDED = 'STREAM_ENDED',
   STREAM_DATA = 'STREAM_DATA',
   STREAM_SHUFFLED_DATA = 'STREAM_SHUFFLED_DATA',
}

const STREAM_ENDED_KEY = 'STREAM_ENDED';
const STREAM_ENDED_VALUE = null;

// @ts-ignore
import {KafkaMessage} from 'kafkajs';


// Define unified messages value field format
interface MessageValue {
   pipelineID: string;
   type: MessageType;
   data: any;
}

function unboxKafkaMessage(msg: KafkaMessage) {
   // TODO kind of ugly
   if (!msg || !msg.value) throw new Error(`[ERROR] Message value is null`);
   const key = !msg.key ? "" : msg.key.toString();
   if (isStreamEnded(msg)) return {key: key, val: JSON.parse(msg.value.toString())};
   // TODO is this default empty string necessary?
   const value = JSON.parse(msg.value.toString());
   const pipelineID = value.pipelineID;
   // const pipelineConfig = pipelines[pipelineID];
   const data = value.data;   // JSON.parse(value.data) should not be necessary
   const type = value.type;
   const val : MessageValue = {pipelineID,type,data}
   return {key, val}
}

function newStreamEndedMessage (pipelineID: string): MessageValue {
   return {
      pipelineID: pipelineID,
      type: MessageType.STREAM_ENDED,
      data: STREAM_ENDED_VALUE
   };
} 

function isStreamEnded (message: any): boolean {
   try {
      const value = JSON.parse(message.value);
      if (!value || !message.key) return false;
      const tmp = (message.key && message.key.toString() === STREAM_ENDED_KEY) && value.type === MessageType.STREAM_ENDED && value.data === STREAM_ENDED_VALUE;
      if (tmp) console.log(`[STREAM_ENDED] Stream ${value.pipelineID}`);
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


function stringifyPipeline (pipeline: PipelineConfig) :string {
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
   stringifyPipeline
}