# Free sources for LLM calls (6 November 2026)

## Where we are

`shared/utils/hosted_llm.py` already speaks the OpenAI chat format to two providers, Groq and Gemini, and a new provider is one line in `PROVIDERS`,
one key in `.env`, and entries in `LLM_QUOTAS` and `LLM_ROUTES`. The live settings:

| Use | Model | Fallback |
|---|---|---|
| qwen2.5:3b calls | groq gpt-oss-20b, then gemini flash-lite | local 3B |
| News scoring (`NEWS_LLM_MODEL`) | groq gpt-oss-120b | none (hosted-only) |
| Scenarios (`SCENARIO_MODEL`) | groq gpt-oss-120b | none |
| Analyst shadow (`ANALYST_SHADOW_MODEL`) | groq gpt-oss-120b | none |

**The weak point is Groq.** In the factor-model service's log for 5 to 6 Nov: 149 rate-limit (429) refusals on gpt-oss-120b and 48 on gpt-oss-20b, and 171
"hosted model unavailable" failures. Three jobs depend on 120b alone, and a hosted-only model has no second provider to fall to (`route_for` returns only
itself). When Groq refuses, news scoring and the 13D reader run degraded and scenarios are skipped. Gemini has the quota and is not in their path.

## The candidates

Every figure below comes from third-party write-ups unless noted. Free tiers change often (Gemini cut its limits in December 2025; Cerebras changed in
mid-2026), so each number must be read from the provider's own console before it goes into `LLM_QUOTAS`.

| Provider | Free allowance reported | Fit for Sentinel | Catch |
|---|---|---|---|
| **Mistral** (La Plateforme, Experiment plan) | 1 request a second and 500,000 tokens a minute (Mistral's help centre); about 1 billion tokens a month (third parties only) | **Best.** Far more volume than Groq's 8,000 tokens a minute. JSON mode and an OpenAI-compatible endpoint | Needs phone verification, and the free plan requires opting in to training on your prompts |
| **OpenRouter** (`:free` models) | 20 requests a minute; 50 requests a day, or 1,000 after a lifetime top-up of $10 | Good second fallback; one key reaches many models and it can try a list in order | Without the top-up, 50 a day is small. A one-off $10 is spend, so it is your call |
| **Gemini** (already wired) | Flash-Lite reported at 15 a minute and 1,000 a day; Flash far lower; sources disagree | Keep as it is. We set 500 a day and 250,000 tokens a minute | Limits fell once already; resets at midnight Pacific |
| **GitHub Models** | 150 a day on low-tier models, 50 a day on high-tier; capped at about 8,000 input tokens a request | Small independent reserve for short prompts | The input cap rules out long filings and 13D text |
| **Cloudflare Workers AI** | 10,000 "neurons" a day, shared across all models (official) | Small models only, a few hundred calls | Needs an account ID; some models are paid-only |
| **NVIDIA build.nvidia.com** | 40 requests a minute, about 1,000 credits (forum posts) | Skip | Terms read as prototyping only, and moderators say limits cannot be raised |
| **Cerebras** | Reports now describe a $5 one-off trial with a card required, not a standing free tier | Skip until checked on their own page | Conflicting sources |
| **SambaNova** | 20 requests a day (third party) | Skip | Too small |
| **Hugging Face** | $0.10 of monthly credit for free accounts (official) | Skip | Negligible |

## Recommendation, in order

1. **Let hosted-only models fall back to another provider.** Add an ordered fallback list per hosted-only step (for example `LLM_FALLBACKS=groq/openai/gpt-oss-120b=mistral/<model>,openrouter/<model>`),
   tried only on a 429 or a full bucket, never retried on the same step. This changes no call volume; it moves calls that now fail onto spare quota. About 30 lines in `hosted_llm.py`, plus tests.
2. **Add Mistral as a provider** (one `PROVIDERS` entry, `MISTRAL_API_KEY`, a quota row from the console's Limits page). It is the only free tier whose size matches the load.
3. **Add OpenRouter as a second fallback** if you are willing to top up $10 once; without it, skip.
4. **Keep GitHub Models and Cloudflare for short prompts only,** and only if 1 to 3 still leave gaps.

## Cautions

- **Scores from different models are not the same series.** News scores, scenarios and the analyst shadow are stored and graded by model. A fallback must write the model that answered, and
  `analyst_shadow` should stay on one model, or its grading mixes two forecasters. News scores should be compared only within a model, or after a calibration check.
- **Free tiers may train on your prompts** (Mistral's Experiment plan requires it; some OpenRouter free models log). Send only public text (headlines, filings, scenario prompts built from public events). Never positions, orders or account data.
- **Quality varies.** Test any new model on the existing JSON tasks (headline scoring, Item 4 reading) against gpt-oss-120b on a fixed sample before routing to it, and count malformed output as a failure.
- **Inference stays the scarcest resource.** Fallbacks add no new calls, so no new trigger or cap is needed. Keep the per-agent hourly shares and the 0.3 reserve.

## Addendum: a "20,000 free calls a day" proposal checked against our own keys (6 November 2026)

A pasted proposal suggested stacking per-model quotas on Groq, Gemini and Cloudflare. I checked what our keys can reach with a handful of one-word test calls
(Groq reports its real limits in response headers; Google does not).

| Claim | What our key shows |
|---|---|
| Groq `llama-3.1-8b-instant` gives 14,400 requests a day | **Not available to us:** the model list has no llama models (404 on both `llama-3.1-8b-instant` and `llama-3.3-70b-versatile`) |
| Each Groq model has its own bucket | **True.** `gpt-oss-20b` 1,000 a day and 8,000 tokens a minute; `qwen/qwen3.8-27b` the same (the note said qwen-3.6); `allam-2-7b` 7,000 a day and 6,000 tokens a minute; `gpt-oss-safeguard-20b` 2,000 tokens a minute (a safety classifier, not useful here) |
| Gemini has several Flash families | **True, and open to us:** `gemini-3.5-flash-lite`, `3.6-flash`, `3.1-flash-lite` and `gemma-4-31b-it` each answered a first call. `3.8-flash` timed out at 40 s. Daily limits per model are **not visible**; the "1,500 a day each" figure is unverified and conflicts with reports of far lower Flash limits |
| Cloudflare gives 300 requests a minute on `llama-3.1-8b-instruct-fast` | **Untested** (no account here). One report says Llama 3.1 8B was retired in May 2026, so the model list must be checked first |

**What binds is tokens a minute, not requests a day.** The Groq 429s are the 8,000-tokens-a-minute cap (a gpt-oss-20b call reports 8,000 against a 1,000-a-day request limit), and they cluster when several jobs fire at
once (22:00, 22:15, 23:16 UTC). Daily call counts like 14,400 or 20,000 only hold for calls of about 35 tokens. Scenario, news and 13D prompts run to thousands of tokens, so a single Groq model carries only a few of them a minute.
Independent buckets help because each has its own tokens-a-minute allowance, which is the reason to spread across models.

**How it fits:**
1. Hosted-only models get an ordered chain across independent buckets: gpt-oss-120b, then gemini-3.6-flash, then qwen/qwen3.8-27b, then the local model. Gemini carries 250,000 tokens a minute, so it absorbs the bursts.
2. Start the new quotas low (Gemini 10 requests a minute and 250 a day per model) and widen them from evidence: log each 429's message, since Google's names the quota that was hit.
3. Keep the analyst shadow on one model so its grading is not mixed. Record the answering model on every news score and scenario.
4. Skip `allam-2-7b` (Arabic-focused 7B, untested on English finance) unless a quality test says otherwise. Cloudflare waits until its model list is checked.
5. Short, high-frequency calls stay on local Ollama, as the proposal says; the hosted chain is for long prompts the 7B handles slowly.

## What I did not do

No keys were created, no code changed, and nothing was called on any provider. The rate figures are unverified against the providers' own pages.

## Sources

- [Mistral help centre: free tier limits](https://help.mistral.ai/en/articles/225174-what-are-the-limits-of-the-free-tier) and [Experiment plan](https://help.mistral.ai/en/articles/450104-how-can-i-try-the-api-for-free-with-the-experiment-plan)
- [OpenRouter: free LLM APIs compared](https://openrouter.ai/blog/tutorials/free-llm-apis-compared/) and [TrueFoundry on OpenRouter limits](https://www.truefoundry.com/es/blog/openrouter-rate-limits-your-free-tier-depends-on-what-youve-already-paid)
- [Cloudflare Workers AI pricing](https://developers.cloudflare.com/workers-ai/platform/pricing/)
- [GitHub Models billing](https://docs.github.com/en/enterprise-cloud@latest/billing/concepts/product-billing/github-models) and [rate-limit summary](https://aireiter.com/blog/best-free-ai-api)
- [Gemini free tier 2026](https://www.cometapi.com/gemini-api-free-tier-2026/)
- [NVIDIA developer forum: is NIM free](https://forums.developer.nvidia.com/t/is-nim-free/369368)
- [Cerebras free plan reports](https://costbench.com/software/llm-api-providers/cerebras-inference/free-plan/)
- [SambaNova rate limits](https://docs.sambanova.ai/docs/en/models/rate-limits) and [Hugging Face inference pricing](https://huggingface.co/docs/inference-providers/en/pricing)
