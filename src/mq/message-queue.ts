import type { ConfirmEvent } from '../domain/types.js';

export type ConfirmHandler = (event: ConfirmEvent) => Promise<void>;

export interface MessageQueue {
  publish(event: ConfirmEvent): Promise<void>;
  /** handler가 throw하면 재시도, 한도 초과 시 DLQ로 이동해야 한다. */
  subscribe(handler: ConfirmHandler): Promise<void>;
  close(): Promise<void>;
}
