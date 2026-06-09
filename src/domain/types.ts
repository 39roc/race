export type ReserveResult =
  | { status: 'RESERVED'; reservationId: string }
  | { status: 'SOLD_OUT' };

export interface ConfirmEvent {
  reservationId: string;
  eventId: string;
  userId: string;
  reservedAt: string; // ISO 8601
}
