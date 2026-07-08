export interface BaseEventEnvelope<TPayload> {
  eventId: string;
  eventType: string;
  correlationId: string;
  causationId: string;
  sagaId: string;
  requestId: string;
  timestamp: string;
  version: number;
  payload: TPayload;
}
