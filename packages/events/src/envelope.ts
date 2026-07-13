export interface BaseEventEnvelope<TPayload> {
  eventId: string;
  eventType: string;
  correlationId: string;
  causationId: string;
  sagaId: string;
  timestamp: string;
  version: number;
  payload: TPayload;
}
