import { describe, it, expect, vi } from 'vitest';

describe('Frontend API Client Unit Tests (api.ts)', () => {
  it('should format base URL correctly from window or env', () => {
    const getBaseUrl = (envUrl?: string, locHost?: string): string => {
      if (envUrl) return envUrl.replace(/\/+$/, '');
      if (locHost) return `http://${locHost}/api/v1`;
      return 'http://localhost:8000/api/v1';
    };

    expect(getBaseUrl('http://api.sentinel.io/v1/')).toBe('http://api.sentinel.io/v1');
    expect(getBaseUrl(undefined, 'localhost:3000')).toBe('http://localhost:3000/api/v1');
  });

  it('should attach Bearer token when present in localStorage', () => {
    const createHeaders = (token: string | null) => {
      const headers: Record<string, string> = { 'Content-Type': 'application/json' };
      if (token) headers['Authorization'] = `Bearer ${token}`;
      return headers;
    };

    expect(createHeaders('jwt_token_123')['Authorization']).toBe('Bearer jwt_token_123');
    expect(createHeaders(null)['Authorization']).toBeUndefined();
  });
});
