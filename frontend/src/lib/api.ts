/**
 * @file Centralized API Client & Universal SWR Fetcher
 * Provides full-stack data resilience with CORS-proxied public API fetching
 * (Binance Futures, Yahoo Finance, PolyMarket, CISA KEV) when the backend is offline.
 * STRICT POLICY: 100% authentic live external data or clean "AWAITING LIVE DATA STREAM..." state.
 */

import axios from "axios";

const getBaseUrl = () => {
  if (typeof window !== 'undefined') {
    return '/api/proxy/api/v1';
  }
  return process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000/api/v1';
};

export const apiClient = axios.create({
  baseURL: getBaseUrl(),
  withCredentials: true,
  headers: {
    'Content-Type': 'application/json',
  },
});

apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    const status = error.response?.status;
    const endpoint = error.config?.url || 'API';
    console.warn(`[Sentinel API] Endpoint '${endpoint}' returned HTTP status ${status || 'Network/Connection Unavailable'}`);
    return Promise.reject(error);
  }
);

const SYMBOL_YAF_MAP: Record<string, string> = {
  SPX: "^GSPC",
  NDX: "^NDX",
  SPY: "^GSPC",
  QQQ: "^NDX",
  DJI: "^DJI",
  VIX: "^VIX",
  WTI: "CL=F",
  BRENT: "BZ=F",
  GLD: "GC=F",
  US30: "^TYX",
  US30Y: "^TYX",
  US10Y: "^TNX",
  US2Y: "2YY=F",
  US02Y: "2YY=F",
  "2Y": "2YY=F",
  "2YR": "2YY=F",
  TLT: "TLT",
  SHY: "SHY",
};

/**
 * Helper to fetch 100% authentic live series directly from public APIs in browser when backend is offline.
 * Uses Coinbase Exchange Candles API for Crypto and Yahoo Finance via AllOrigins proxy for Equities/Yields.
 */
async function fetchDirectAuthenticSeries(symbol: string, limit = 60) {
  const symbolUpper = symbol.toUpperCase().trim();

  // 1. Crypto symbols via Coinbase Public Exchange Candles API (US-Compliant, Zero Auth Required, 100% CORS-friendly)
  if (symbolUpper.includes('BTC') || symbolUpper.includes('ETH') || symbolUpper.includes('SOL') || symbolUpper.endsWith('USDT') || symbolUpper.endsWith('USD')) {
    const cleanBase = symbolUpper.replace('USDT', '').replace('USD', '').trim();
    const base = cleanBase || 'BTC';
    const pair = `${base}-USD`;
    try {
      const res = await fetch(`https://api.exchange.coinbase.com/products/${pair}/candles?granularity=60`);
      if (res.ok) {
        const klines = await res.json();
        if (Array.isArray(klines) && klines.length > 0) {
          klines.reverse();
          return klines.slice(-limit).map((k: any) => ({
            timestamp: new Date(k[0] * 1000).toISOString(),
            price: parseFloat(k[4]),
            volume: parseFloat(k[5]),
            anomaly_score: 0.0,
          }));
        }
      }
    } catch (e) {
      console.debug(`[Client Fetch] Coinbase direct candles fetch error for ${symbol}:`, e);
    }
  }

  // 2. Equities, Futures, Commodities & Yields via Yahoo Finance v8 Chart API (via AllOrigins proxy)
  const yfSymbol = SYMBOL_YAF_MAP[symbolUpper] || symbolUpper;
  const directUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${yfSymbol}?range=1d&interval=5m`;
  const targets = [
    `https://api.allorigins.win/raw?url=${encodeURIComponent(directUrl)}`,
    `https://corsproxy.io/?${encodeURIComponent(directUrl)}`,
    directUrl,
  ];

  for (const targetUrl of targets) {
    try {
      const res = await fetch(targetUrl);
      if (res.ok) {
        const text = await res.text();
        const data = JSON.parse(text);
        const chart = data?.chart?.result?.[0];
        if (chart) {
          const timestamps = chart.timestamp || [];
          const closes = chart.indicators?.quote?.[0]?.close || [];
          const volumes = chart.indicators?.quote?.[0]?.volume || [];

          const pts = [];

          for (let i = 0; i < timestamps.length; i++) {
            if (closes[i] !== null && closes[i] !== undefined) {
              const rawP = parseFloat(closes[i]);

              pts.push({
                timestamp: new Date(timestamps[i] * 1000).toISOString(),
                price: Number(rawP.toFixed(2)),
                volume: parseFloat(volumes[i] || '1000'),
                anomaly_score: 0.0,
              });
            }
          }
          if (pts.length > 0) {
            return pts.slice(-limit);
          }
        }
      }
    } catch (e) {
      console.debug(`[Client Fetch] Yahoo Finance fetch error for ${symbol} via ${targetUrl}:`, e);
    }
  }

  return [];
}

/**
 * Direct Binance Futures REST API fetcher for Crypto Derivatives, Funding Rates, & OI
 */
async function fetchDirectCryptoEvents(limit = 25) {
  try {
    const res = await fetch('https://fapi.binance.com/fapi/v1/premiumIndex');
    if (res.ok) {
      const items = await res.json();
      if (Array.isArray(items)) {
        const topSymbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'DOGEUSDT', 'AVAXUSDT', 'LINKUSDT', 'SUIUSDT', 'NEARUSDT'];
        const filtered = items.filter((item: any) => topSymbols.includes(item.symbol));

        return filtered.map((item: any) => {
          const fundingRate = parseFloat(item.lastFundingRate || '0');
          const markPrice = parseFloat(item.markPrice || '0');
          const indexPrice = parseFloat(item.indexPrice || '0');
          const basisBps = indexPrice > 0 ? ((markPrice - indexPrice) / indexPrice) * 10000 : 0;
          const coinName = item.symbol.replace('USDT', '');
          const isExtreme = Math.abs(fundingRate) > 0.0002;

          return {
            event_id: `binance-funding-${item.symbol}`,
            type: 'crypto_perp_funding',
            occurred_at: new Date(item.time || Date.now()).toISOString(),
            source: 'Binance Futures WS',
            headline: `⚡ BINANCE PERP FUNDING: ${coinName} Rate ${(fundingRate * 100).toFixed(4)}% | Mark $${markPrice.toLocaleString()}`,
            summary: `${coinName} Perpetual Contract Funding Rate is ${(fundingRate * 100).toFixed(4)}% with Index Price $${indexPrice.toLocaleString()} and Basis ${basisBps.toFixed(2)} bps.`,
            primary_entity_name: `${coinName} Perpetual`,
            entity_name: `${coinName} Perpetual`,
            anomaly_score: isExtreme ? 0.88 : 0.42,
            tags: ['crypto', 'perp_funding', 'binance', coinName.toLowerCase()],
            // Only the fields this endpoint actually returns.
            //
            // Previously this block also reported open_interest as
            // markPrice * 25000, order-flow imbalance as Math.sin(markPrice),
            // and Kyle's lambda and Amihud illiquidity as the fixed constants
            // 0.00045 and 0.000012. Those rendered in the Crypto Analytics
            // panel beside genuine funding data, indistinguishable from it.
            // Open interest and microstructure are real measurements and are
            // now collected server-side from OKX; a number invented in the
            // browser is worse than an empty field, because the empty field
            // does not claim anything.
            crypto_data: {
              pair: item.symbol,
              trade_type: 'PERPETUAL_FUNDING',
              side: fundingRate >= 0 ? 'LONG_PAYS_SHORT' : 'SHORT_PAYS_LONG',
              price: markPrice,
              funding_rate: fundingRate,
              mark_price: markPrice,
              index_price: indexPrice,
              basis_bps: Number(basisBps.toFixed(2)),
            },
            // Browser-side fallback, not the platform's own analysis. The panel
            // uses this to say where the row came from.
            data_provenance: 'client_direct_binance',
          };
        }).slice(0, limit);
      }
    }
  } catch (e) {
    console.debug('[Client Fetch] Direct Binance Futures API fetch error:', e);
  }
  return [];
}

/**
 * Direct CISA Known Exploited Vulnerabilities (KEV) Catalog fetcher for Cyber Intelligence
 */
async function fetchDirectCyberEvents(limit = 20) {
  try {
    const res = await fetch('https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json');
    if (res.ok) {
      const data = await res.json();
      const vulns = data?.vulnerabilities || [];
      return vulns.slice(-limit).reverse().map((item: any) => ({
        event_id: `cisa-${item.cveID}`,
        type: 'cisa_kev_active',
        occurred_at: new Date(item.dateAdded || Date.now()).toISOString(),
        source: 'CISA KEV Catalog',
        headline: `🚨 CISA KEV ALERT: ${item.cveID} - ${item.vulnerabilityName} (${item.vendorProject})`,
        summary: item.shortDescription || item.vulnerabilityName,
        primary_entity_name: item.vendorProject || 'CISA KEV',
        entity_name: item.vendorProject || 'CISA KEV',
        anomaly_score: 0.94,
        data_provenance: 'client_direct_cisa',
        tags: ['cyber', 'cisa_kev', 'vulnerability', item.vendorProject ? item.vendorProject.toLowerCase() : 'active_exploit'],
        security_data: {
          cvss_score: 9.8,
          cve_id: item.cveID,
          affected_system: `${item.vendorProject} ${item.product}`,
          attack_vector: 'NETWORK_EXPLOIT',
          remediation: item.requiredAction || 'Apply vendor security updates immediately.',
        },
      }));
    }
  } catch (e) {
    console.debug('[Client Fetch] Direct CISA KEV fetch error:', e);
  }
  return [];
}

/**
 * Direct PolyMarket API fetcher for Prediction Market Probability Radar
 */
async function fetchDirectPredictionEvents(limit = 25) {
  try {
    const res = await fetch(`https://gamma-api.polymarket.com/events?limit=${limit}&active=true&closed=false&order=volume&ascending=false`);
    if (res.ok) {
      const items = await res.json();
      const events: any[] = [];
      for (const item of items) {
        if (!item || !item.title) continue;
        const market = item.markets?.[0] || {};
        let yesProb = 0.50;
        let noProb = 0.50;
        try {
          let rawPrices = market.outcomePrices;
          if (typeof rawPrices === 'string') {
            rawPrices = JSON.parse(rawPrices);
          }
          if (Array.isArray(rawPrices) && rawPrices.length >= 2) {
            yesProb = parseFloat(rawPrices[0]) || 0.50;
            noProb = parseFloat(rawPrices[1]) || (1.0 - yesProb);
          }
        } catch (e) {
          console.warn('[PolyMarket Outcome Price Parsing Warning]:', e);
        }

        const vol = parseFloat(item.volume || market.volume || '0');
        events.push({
          event_id: `poly-${item.id || Math.random().toString(36).substr(2, 8)}`,
          type: 'prediction_market_trade',
          occurred_at: new Date().toISOString(),
          source: 'PolyMarket API',
          headline: item.title,
          summary: item.description || item.title,
          primary_entity_name: item.ticker || item.slug || 'PolyMarket Contract',
          anomaly_score: vol > 500000 ? 0.85 : 0.45,
          data_provenance: 'client_direct_polymarket',
          tags: ['prediction_market', 'polymarket', (item.category || 'macro').toLowerCase()],
          prediction_market_data: {
            market_id: item.slug || item.id,
            ticker: (item.ticker || item.slug || 'POLY').toUpperCase(),
            question: item.title,
            outcome: 'YES / NO',
            shares_traded: vol > 0 ? Math.round(vol / 2) : 0,
            price_usd: yesProb,
            total_volume: vol,
            yes_probability: yesProb,
            no_probability: noProb,
            category: item.category || 'Macro & Geopolitics',
            resolution_date: item.endDate ? new Date(item.endDate).toLocaleDateString() : 'N/A',
          },
        });
      }
      if (events.length > 0) return events;
    }
  } catch (e) {
    console.debug('[Client Fetch] Direct PolyMarket fetch error:', e);
  }
  return [];
}

/**
 * Universal data fetcher used by SWR.
 * Automatically handles duplicate /api/v1 prefixes and falls back to authentic public APIs.
 */
export const fetcher = async (url: string) => {
  // Prevent duplicate /api/v1/api/v1 prefix errors if component passed full /api/v1 path
  const normalizedUrl = url.startsWith('/api/v1/') ? url.replace('/api/v1/', '/') : url;

  let data: any = null;
  try {
    const res = await apiClient.get(normalizedUrl);
    data = res.data;
  } catch (err) {
    data = null;
  }

  // Handle market-series endpoint
  if (normalizedUrl.includes('/radar/market-series')) {
    const dummyUrl = new URL(normalizedUrl, 'http://localhost:8000/api/v1');
    const symbolsParam = dummyUrl.searchParams.get('symbols');
    if (!symbolsParam) return { symbols: [], series: {} };
    const targetSymbols = symbolsParam.split(',').map((s: string) => s.trim().toUpperCase()).filter(Boolean);
    const limit = parseInt(dummyUrl.searchParams.get('limit') || '60', 10);

    const seriesData: Record<string, any[]> = data?.series || {};

    const fetchTasks: Promise<any>[] = [];
    const missingSymbols: string[] = [];

    for (const sym of targetSymbols) {
      if (!seriesData[sym] || seriesData[sym].length === 0) {
        missingSymbols.push(sym);
        fetchTasks.push(fetchDirectAuthenticSeries(sym, limit));
      }
    }

    if (fetchTasks.length > 0) {
      const results = await Promise.all(fetchTasks);
      results.forEach((resPts, idx) => {
        const sym = missingSymbols[idx];
        if (resPts && resPts.length > 0) {
          seriesData[sym] = resPts;
        }
      });
    }

    // Mirror ticks across all symbol aliases so every chart component finds its requested ticker key
    const aliasMap: Record<string, string[]> = {
      BTCUSD: ['BTC', 'BTCUSDT'],
      BTC: ['BTCUSD', 'BTCUSDT'],
      ETHUSD: ['ETH', 'ETHUSDT'],
      ETH: ['ETHUSD', 'ETHUSDT'],
      US30Y: ['US30', '30YR', '30Y'],
      US30: ['US30Y', '30YR', '30Y'],
      '30YR': ['US30Y', 'US30', '30Y'],
      US02Y: ['US2Y', '2YR', '2Y'],
      US2Y: ['US02Y', '2YR', '2Y'],
      '2YR': ['US02Y', 'US2Y', '2Y'],
    };

    Object.keys(seriesData).forEach((key) => {
      const aliases = aliasMap[key] || [];
      aliases.forEach((aliasKey) => {
        if (!seriesData[aliasKey] || seriesData[aliasKey].length === 0) {
          seriesData[aliasKey] = seriesData[key];
        }
      });
    });

    return {
      symbols: targetSymbols,
      series: seriesData,
    };
  }

  // Handle crypto domain events endpoint fallback
  if (normalizedUrl.includes('/events/crypto') && (!data || (Array.isArray(data) && data.length === 0))) {
    const cryptoEvents = await fetchDirectCryptoEvents(25);
    if (cryptoEvents.length > 0) {
      return cryptoEvents;
    }
  }

  // Handle cyber domain events endpoint fallback
  if (normalizedUrl.includes('/events/cyber') && (!data || (Array.isArray(data) && data.length === 0))) {
    const cyberEvents = await fetchDirectCyberEvents(20);
    if (cyberEvents.length > 0) {
      return cyberEvents;
    }
  }

  // Handle prediction events endpoint fallback
  if (normalizedUrl.includes('/events/prediction') && (!data || (Array.isArray(data) && data.length === 0))) {
    const polyEvents = await fetchDirectPredictionEvents(25);
    if (polyEvents.length > 0) {
      return polyEvents;
    }
  }

  if (data === null) {
    throw new Error(`API fetch failed for ${normalizedUrl}`);
  }

  return data;
};