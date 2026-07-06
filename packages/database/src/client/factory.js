"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getOrCreatePrismaClient = getOrCreatePrismaClient;
exports.disconnectAll = disconnectAll;
const config_1 = require("../config");
const globalForPrisma = global;
const activeClients = [];
/**
 * Factory to create or retrieve a singleton Prisma client instance.
 * Singleton is preserved in non-production environments to prevent connection leaks during hot-reloads.
 */
function getOrCreatePrismaClient(name, ClientConstructor, options = {}) {
    const databaseUrl = process.env.DATABASE_URL;
    if (!databaseUrl) {
        throw new Error('DATABASE_URL environment variable is not defined');
    }
    // Parse connection settings from environment variables with sensible defaults
    const connectionLimit = options.connectionLimit ?? (process.env.DATABASE_CONNECTION_LIMIT
        ? parseInt(process.env.DATABASE_CONNECTION_LIMIT, 10)
        : undefined);
    const poolTimeout = options.poolTimeout ?? (process.env.DATABASE_POOL_TIMEOUT
        ? parseInt(process.env.DATABASE_POOL_TIMEOUT, 10)
        : undefined);
    const socketTimeout = options.socketTimeout ?? (process.env.DATABASE_IDLE_TIMEOUT
        ? parseInt(process.env.DATABASE_IDLE_TIMEOUT, 10)
        : undefined);
    const finalUrl = (0, config_1.buildDatabaseUrl)(databaseUrl, {
        connectionLimit,
        poolTimeout,
        socketTimeout,
        schema: name,
    });
    const prismaOptions = {
        datasources: {
            db: {
                url: finalUrl,
            },
        },
        log: options.logQueries ?? process.env.NODE_ENV === 'development'
            ? ['query', 'info', 'warn', 'error']
            : ['error'],
    };
    if (process.env.NODE_ENV === 'production') {
        const client = new ClientConstructor(prismaOptions);
        activeClients.push(client);
        return client;
    }
    const globalKey = `__prisma_client_${name}__`;
    if (!globalForPrisma[globalKey]) {
        globalForPrisma[globalKey] = new ClientConstructor(prismaOptions);
        activeClients.push(globalForPrisma[globalKey]);
    }
    return globalForPrisma[globalKey];
}
/**
 * Gracefully disconnects all active Prisma clients in the registry.
 */
async function disconnectAll() {
    await Promise.all(activeClients.map(async (client) => {
        try {
            await client.$disconnect();
        }
        catch (_err) {
            // Suppress errors during shutdown to ensure other clients can disconnect
        }
    }));
    activeClients.length = 0;
}
//# sourceMappingURL=factory.js.map