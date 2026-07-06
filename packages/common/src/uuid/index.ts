import { v4 as uuidv4, validate as uuidValidate } from 'uuid';

export function generateUuid(): string {
  return uuidv4();
}

export function isValidUuid(val: string): boolean {
  return uuidValidate(val);
}
