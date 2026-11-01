# StudyApp V3 retrieval engine — review

Written after reading `src/cpp/*` and querying `data/studyapp.db` (read-only) with the
existing `prompts/fuel-cell-controllable-model.txt`. The C++ binary is Windows-only, so the
ranking was reproduced in Python as a faithful port of `prompt_ranking.cpp` +
`modules/word_freq.tokenize` and checked to agree with the engine's design before drawing
conclusions. Ground truth for "is this actually a PEMFC paper" came from an independent
text scan of the first three pages of all 1,807 PDFs in `../articles`.

## What's in the databases

**V3 `data/studyapp.db` — 443 MB, 1,800 articles.** The working index.

| table | rows | what it is |
|---|---|---|
| `file_info` | 1,800 | id, path, sha256, page count, ingest time, token-freq path |
| `article_text` | 1,800 | full extracted text |
| `article_tags` | 77,646 | `intrinsic` (paper's own keywords) + `expanded` (Datamuse) |
| `relation_distance_filtered` | 836,003 | per-doc token frequency + L2-normalized weight |
| `tf_idf` | 44,435 | corpus-level word, freq, doc_count, tf-idf |
| `comparison` | 2,780,081 | directed doc-pair scores |
| `comparison_seen` | 1,800 | incremental-build bookkeeping |

**V2 `data/pdf_text.db` — 7.8 GB, 5,281 files.** Superset corpus, and it has things V3 dropped:
`book_catalog` (174 books with domain, primary_topic, confidence), `book_user_meta`
(read_status, rating, notes), `file_token`, `item_matrix`. The book catalog and the
read-status tracking are worth keeping — V3 has no equivalent, and read-status is the thing
you wanted filename renaming to stand in for.

## How ranking currently works

`prompt_ranking.cpp::process_prompt`:

1. Tokenize the prompt the same way articles were tokenized — strip non-word chars,
   lowercase, NLTK stopwords + 21 extras, Porter stem.
2. Keep stems with length ≤ 18 that are all-lowercase alpha.
3. L2-normalize the query term frequencies: `qw[t] = freq[t] / sqrt(Σ freq²)`.
4. Add a rarity bonus to the **query** side only: `qw[t] += tf_idf[t] / freq[t]`.
5. `score(doc) = Σ_shared_tokens  doc_weight[t] × qw[t]`, where `doc_weight` is the
   L2-normalized term frequency from `relation_distance_filtered`.

So it is a dot product of two L2-normalized **term-frequency** vectors. There is no IDF on
the document side.

## Finding 1 — no document-side IDF, so precision decays with depth

Running `prompts/fuel-cell-controllable-model.txt` against the corpus:

| depth | genuinely PEMFC | off-topic |
|---|---|---|
| top-10 | 9 | 1 |
| top-25 | 21 | 4 |
| top-50 | **27** | 23 |
| top-100 | 41 | 59 |

The first page is good; past rank ~25 it fills with generic "control + temperature + model"
papers — aircraft engine thermal management, EV heat pumps, supercritical heat exchangers,
bidirectional converters. The highest-weighted query stems are `control, temperatur,
pressur, membran, model, voltag, current, densiti` — all of them low-IDF words that nearly
every engineering paper contains. The genuinely discriminating stems in the same prompt
(`osmot, tafel, extremum, dehydr, parameteris`) carry the least weight.

Step 4 doesn't fix this: the rarity bonus is added to the query vector, which reweights
*terms* but cannot distinguish *documents*, because every document is scored against the
same query vector.

**Fix.** Weight both sides by IDF and take the cosine. `doc_count` is already in `tf_idf`,
so nothing new has to be computed:

```
idf[t]   = log(N / doc_count[t])              // N = 1800
score    = Σ (doc_weight[t]·idf[t]) × (qw[t]·idf[t])  /  (‖doc·idf‖ · ‖q·idf‖)
```

Measured effect, same prompt:

| depth | current | with IDF cosine |
|---|---|---|
| top-10 | 9 | **10** |
| top-25 | 21 | **23** |
| top-50 | 27 | **43** |

Same corpus, same prompt, same tokenizer — 27 → 43 relevant in the top 50.

## Finding 2 — `persist_frequency_threshold = 3` throws away the discriminating vocabulary

`relational_distance.cpp` only persists tokens with frequency ≥ 3. Measured against the
pre-cut `data/token_freq/*.json`:

| article | filtered vocab | persisted | dropped |
|---|---|---|---|
| Two-stage MPC stack control | 1,943 | 649 | 1,294 (67%) |
| Humidity regulation (MPC) | 1,392 | 508 | 884 (64%) |
| Oxygen excess ratio control | 1,066 | 318 | 748 (70%) |

Two thirds of each document's vocabulary never reaches the index. That would be acceptable
if the lost terms were noise, but they are the opposite — these specific terms were dropped:

- `pukrushpan` — from **both** the humidity-regulation and oxygen-excess-ratio papers
- `stoichiometri`, `humidifi` — from the oxygen-excess-ratio paper
- `nernst`, `tafel`, `dehydr` — from the two-stage MPC paper

A term a paper uses twice is often the strongest evidence of what it is about; a term it
uses fifty times is usually "control". The threshold is backwards with respect to
information content, and it is the same root cause as Finding 1: **frequency is being used
as a proxy for importance.** With IDF in the scoring function, rare terms become the
valuable ones, and a frequency floor actively removes them.

**Fix.** Drop the floor to 1, or replace it with an IDF floor (discard terms whose
`doc_count` is above, say, 40% of the corpus). Index size grows roughly 3×, to ~2.5M rows —
still small next to the 2.78M-row `comparison` table.

## Finding 3 — the `comparison` table is misnamed and not usable as a neighbour index

Three separate problems:

1. **The column is called `distance` but it holds a similarity.** From
   `comparison.cpp`: `score += target_plain_weight × source_boosted_weight`, inserted into
   `distance`. Sorting ascending — which the name invites — returns the *least* similar
   documents first.
2. **It doesn't rank neighbours correctly in either direction.** For the Two-stage MPC
   paper, its seven obvious topical neighbours in the corpus land at ranks 318–1250 out of
   1,723 retained pairs. Not the top, not the bottom — the middle.
3. **Document length leaks into the score.** Spearman correlation between the stored value
   and the target's vocabulary size is **+0.42**. The 20 "nearest" documents average 309
   tokens; the 20 "farthest" average 614 (corpus mean 464). The cause is step 4 again: the
   `tf_idf/freq` bonus is added *after* L2 normalization, so the boosted source vector no
   longer has unit norm and the dot product stops being a cosine.

Also worth noting: `comparison_score_threshold = 0.15` still retains 1,723 of 1,799
possible targets for that seed. If almost everything passes, the threshold isn't
discriminating.

This is also the most expensive artifact in the build — 2.78M rows, both directions stored
because the score is asymmetric. An IDF-weighted cosine is symmetric, computes for
1,800 × 1,800 in seconds, and halves the storage.

## Finding 4 — `article_tags` is built and never used

77,646 tags are written by `tag_topics.py`, and no ranking path ever reads them —
`article_tags` appears only in schema, purge, and insert statements. This is the best unused
asset in the database. The `intrinsic` tags are precise; for the Two-stage MPC paper they are:

> `control-oriented modeling`, `experimental validation`,
> `nonlinear model predictive control`, `pem fuel cell`, `real-time implementation`

That is a better description of the paper than anything the term-frequency vector captures.
Using intrinsic tags as a filter or a score boost would likely beat the IDF fix on its own.

The `expanded` (Datamuse) tags are the opposite — for the same paper they include `balsa`,
`ceroplastic`, `clay`, `decal`, `audit`, because Datamuse expanded "model" into
modelling-clay vocabulary. Datamuse has no notion of domain, so on polysemous technical
terms it generates confident noise. I'd drop the expansion, or only expand tags that
survive an IDF check against the corpus.

## Finding 5 — small data-hygiene items

- **DOI extraction keeps stray brackets.** `10.1109/ECCE-Asia49820.2021.9479098⟩` — the
  trailing `⟩` comes from a HAL cover page formatting the DOI as `⟨doi⟩`. Strip `⟨⟩` and
  whitespace in the DOI regex.
- **101 of 1,800 ids fall back to `sha256:…`** because no DOI was found in the text. Fine as
  a fallback, but these are the records that will duplicate if the same paper is re-ingested
  from a differently-named file.
- **`file_info.token_freq_path` stores Windows absolute paths**, which makes the DB
  non-portable. A path relative to `DATA_DIR` would be enough.
- **Corpus drift.** The DB has 1,800 articles; `../articles` now holds 1,807. Seven are
  unindexed, and two of them are books worth having in the index:
  *Modeling and Control Strategies for a Fuel Cell System* (Xing, Springer Theses 2023) and
  *Advances in Energy Systems Engineering* (Kopanos, Liu & Georgiadis).

## Suggested order of work

1. IDF-weighted cosine in `prompt_ranking.cpp` (measured: 27 → 43 relevant in top 50).
2. Use `intrinsic` tags in scoring; stop writing `expanded` tags.
3. Lower or replace `persist_frequency_threshold`.
4. Rebuild `comparison` as a symmetric IDF cosine, and rename the column to `similarity`.
5. DOI regex cleanup, relative `token_freq_path`, re-ingest the 7 missing files.
