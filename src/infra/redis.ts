import Redis from 'ioredis';

export function makeRedis(url: string): Redis {
  return new Redis(url, { maxRetriesPerRequest: null });
}
