/**
 * Plain-English meanings for the few technical terms the app cannot avoid. Each says what the number is
 * and how to read it, in a sentence or two, with no further jargon.
 */

export const GLOSSARY: Record<string, string> = {
  sharpe: 'Return for each unit of risk taken. Higher is better; below about 0.5 is weak, and a high number over a short stretch can be luck.',
  'deflated sharpe': 'The Sharpe ratio corrected for how many models were tried. Read it as the chance the result is real, not luck. The bar here is 0.95.',
  ic: 'How well the model’s ranking of stocks matched what they then did. 0 is no better than chance; higher is better.',
  't-statistic': 'How many times bigger a result is than its own noise. Above about 2 is unlikely to be luck.',
  sigma: 'How many typical day-to-day swings a reading sits from its own average. Above 2 is unusual; above 4 is rare.',
  'tracking error': 'How much the portfolio’s daily results differ from the S&P 500. A small number means it moves almost like the index.',
  'information ratio': 'Gain over the S&P 500 for each unit of difference from it. Above 0.5 is good.',
  turnover: 'How much of the portfolio is replaced each time it rebalances. High turnover means more trading costs.',
  beta: 'How much a stock or portfolio moves when the market moves. 1 moves with the market; 0.5 moves half as much.',
  drawdown: 'The biggest fall from a high point before it recovered.',
};

/** The definition for a term, case-insensitive, or null when there is none. */
export function defineTerm(term: string): string | null {
  return GLOSSARY[term.trim().toLowerCase()] ?? null;
}
