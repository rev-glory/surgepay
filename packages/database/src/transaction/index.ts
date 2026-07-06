interface TransactionalClient {
  $transaction<T>(
    fn: (tx: Omit<this, '$connect' | '$disconnect' | '$on' | '$transaction' | '$use'>) => Promise<T>,
    options?: { maxWait?: number; timeout?: number; isolationLevel?: unknown }
  ): Promise<T>;
}

/**
 * Executes a function block within a standard Prisma interactive transaction.
 */
export async function runInTransaction<T, C extends TransactionalClient>(
  client: C,
  fn: (tx: Omit<C, '$connect' | '$disconnect' | '$on' | '$transaction' | '$use'>) => Promise<T>,
  options?: { maxWait?: number; timeout?: number; isolationLevel?: unknown }
): Promise<T> {
  return client.$transaction(fn, options);
}
