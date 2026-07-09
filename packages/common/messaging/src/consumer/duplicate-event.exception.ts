export class DuplicateEventException extends Error {
  constructor(public readonly eventId: string, public readonly consumer: string) {
    super(`Duplicate event detected for eventId: ${eventId}, consumer: ${consumer}`);
    this.name = 'DuplicateEventException';
  }
}

export class EventCurrentlyProcessingException extends Error {
  constructor(public readonly eventId: string, public readonly consumer: string) {
    super(`Event is currently being processed by another worker: eventId: ${eventId}, consumer: ${consumer}`);
    this.name = 'EventCurrentlyProcessingException';
  }
}
