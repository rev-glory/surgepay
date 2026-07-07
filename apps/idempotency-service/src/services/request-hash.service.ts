import * as crypto from 'crypto';

import { Injectable } from '@nestjs/common';

@Injectable()
export class RequestHashService {
  /**
   * Deterministically stringifies any object by sorting keys, then computes its SHA-256 checksum.
   */
  generate(body: Record<string, unknown>): string {
    const serialized = this.deterministicStringify(body);
    return crypto.createHash('sha256').update(serialized).digest('hex');
  }

  /**
   * Serializes an object with keys sorted alphabetically to guarantee consistent hashing
   * regardless of key ordering.
   */
  private deterministicStringify(obj: unknown): string {
    if (obj === null || obj === undefined) {
      return '';
    }
    if (typeof obj !== 'object') {
      return String(obj);
    }
    if (Array.isArray(obj)) {
      return '[' + obj.map((item) => this.deterministicStringify(item)).join(',') + ']';
    }
    const recordObj = obj as Record<string, unknown>;
    const keys = Object.keys(recordObj).sort();
    const properties = keys.map((key) => {
      return `"${key}":${this.deterministicStringify(recordObj[key])}`;
    });
    return '{' + properties.join(',') + '}';
  }
}
