export const OUTBOX_METRICS = {
  PENDING: 'pending_events',
  PUBLISHED: 'published_events',
  FAILED: 'failed_events',
  BATCH_SIZE: 'outbox_relay_batch_size',
  PUBLISH_DURATION: 'outbox_relay_publish_duration_ms',
  IN_FLIGHT: 'outbox_relay_in_flight_messages',
};
