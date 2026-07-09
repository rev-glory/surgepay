export type EventVersion = number;

export type EventUpgrader<TFrom = any, TTo = any> = (payload: TFrom) => TTo;

export class EventVersionRegistry {
  private readonly upgraders = new Map<string, Map<number, EventUpgrader>>();

  /**
   * Registers a payload migration function from a specific version to the next version (v -> v+1).
   */
  registerUpgrader(
    eventType: string,
    fromVersion: number,
    upgrader: EventUpgrader,
  ): void {
    if (!this.upgraders.has(eventType)) {
      this.upgraders.set(eventType, new Map());
    }
    this.upgraders.get(eventType)!.set(fromVersion, upgrader);
  }

  /**
   * Resolves the upgrader function for a specific event type and version.
   */
  getUpgrader(eventType: string, fromVersion: number): EventUpgrader | undefined {
    return this.upgraders.get(eventType)?.get(fromVersion);
  }
}
