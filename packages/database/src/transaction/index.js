"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.runInTransaction = runInTransaction;
/**
 * Executes a function block within a standard Prisma interactive transaction.
 */
async function runInTransaction(client, fn, options) {
    return client.$transaction(fn, options);
}
//# sourceMappingURL=index.js.map