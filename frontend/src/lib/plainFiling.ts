/**
 * What an SEC form code means, in a few words. The code stays in the hover so the filing can still be found by it.
 */

const FORMS: { test: RegExp; name: string }[] = [
  { test: /^SCHEDULE 13D/i, name: 'Large stake, with plans' },
  { test: /^SCHEDULE 13G/i, name: 'Large stake, passive' },
  { test: /^8-K/i, name: 'Company news' },
  { test: /^10-K/i, name: 'Annual report' },
  { test: /^10-Q/i, name: 'Quarterly report' },
  { test: /^4(\/A)?$/i, name: 'Insider trade' },
  { test: /^3(\/A)?$/i, name: 'New insider' },
  { test: /^144/i, name: 'Insider plans to sell' },
  { test: /^13F/i, name: 'Fund holdings' },
  { test: /^S-1|^424B|^FWP/i, name: 'Share sale paperwork' },
  { test: /^DEF 14A|^PRE 14A/i, name: 'Shareholder vote' },
];

export function plainForm(form: string | null | undefined): string {
  const f = (form ?? '').trim();
  if (!f) return 'Filing';
  const hit = FORMS.find((x) => x.test.test(f));
  return hit ? `${hit.name}${/\/A$/i.test(f) ? ' (update)' : ''}` : f;
}
