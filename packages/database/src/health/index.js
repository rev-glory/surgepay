"use strict";
/**
 * Connectivity checks for PostgreSQL databases using a simple ping query.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.ping = ping;
exports.isConnected = isConnected;
/**
 * Pings the database by executing a simple SELECT 1 raw query.
 * Returns true if connection is successful, false otherwise.
 */
async function ping(client) {
    try {
        if (typeof client.$queryRawUnsafe === 'function') {
            await client.$queryRawUnsafe('SELECT 1');
        }
        else if (typeof client.$queryRaw === 'function') {
            await client.$queryRaw `SELECT 1`;
        }
        else {
            return false;
        }
        return true;
    }
    catch (_error) {
        return false;
    }
}
/**
 * Validates if the client has an active connection to the database.
 */
async function isConnected(client) {
    return ping(client);
}
//# sourceMappingURL=index.js.map