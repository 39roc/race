import type { ReserveResult } from '../domain/types.js';

export interface ConcurrencyStrategy {
  decrementStock(eventId: string, userId: string): Promise<ReserveResult>;
}
