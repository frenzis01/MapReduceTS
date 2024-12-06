// Define unified messages value field format


enum MessageType {
   STREAM_ENDED = 'STREAM_ENDED',
   STREAM_DATA = 'STREAM_DATA',
   STREAM_SHUFFLED_DATA = 'STREAM_SHUFFLED_DATA',
}

const STREAM_ENDED_KEY = 'STREAM_ENDED';
const STREAM_ENDED_VALUE = null;


interface MessageValue {
   pipelineID: string;
   type: MessageType;
   data: string | string[] | null;
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

function newMessageValue(data: string, pipelineID: string): MessageValue {
   return {
      pipelineID: pipelineID,
      type: MessageType.STREAM_DATA,
      data: data
   };
}

function newMessageValueShuffled(data: string[], pipelineID: string): MessageValue {
   return {
      pipelineID: pipelineID,
      type: MessageType.STREAM_SHUFFLED_DATA,
      data: data
   };
}

interface PipelineConfig {
   pipelineID: string;
   keySelector: (message: any) => string;
   mapFn: (value: any) => any[];
   reduceFn: (key: string, values: any[]) => any;
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
   newStreamEndedMessage,
   isStreamEnded,
   newMessageValue,
   newMessageValueShuffled,
   PipelineConfig,
   stringifyPipeline
}