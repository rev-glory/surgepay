"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isPrismaError = isPrismaError;
/**
 * Checks if the thrown error is a Prisma-specific database error.
 */
function isPrismaError(error) {
    return (error !== null &&
        typeof error === 'object' &&
        'code' in error &&
        typeof error['code'] === 'string' &&
        'clientVersion' in error &&
        typeof error['clientVersion'] === 'string');
}
//# sourceMappingURL=index.js.map