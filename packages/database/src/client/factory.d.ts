interface ClientLike {
    $disconnect(): Promise<void>;
}
export interface FactoryOptions {
    connectionLimit?: number;
    poolTimeout?: number;
    socketTimeout?: number;
    logQueries?: boolean;
}
/**
 * Factory to create or retrieve a singleton Prisma client instance.
 * Singleton is preserved in non-production environments to prevent connection leaks during hot-reloads.
 */
export declare function getOrCreatePrismaClient<T extends ClientLike>(name: string, ClientConstructor: new (options?: unknown) => T, options?: FactoryOptions): T;
/**
 * Gracefully disconnects all active Prisma clients in the registry.
 */
export declare function disconnectAll(): Promise<void>;
export {};
