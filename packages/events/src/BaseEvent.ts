export interface BaseEvent<T = unknown> {
  eventType: string;
  version: number;
  payload: T;
}
