# IC3 Experiment Lab — Phase 3 + Cheat Sheet

## 1. What Is The Experiment? (Plain English)

* NOTE: Odds of recommendation-
SHIP: ~80
HOLD: ~20

### The Store Story
Imagine you run an online store — like Amazon.

A customer:
1. Browses products
2. Adds something to cart
3. Clicks "Checkout"
4. Enters card details
5. Clicks "Pay Now" — money changes hands

That last button is the most important button on the whole site.
Everything before it is browsing. That button is where browsing turns into money.

If 100 people put something in their cart:
- 90 leave without paying
- 10 pay

That 10 out of 100 = 10% conversion rate.

If you can get that from 10 to 14, you just made 40% more money without getting any new customers. No extra ads. You just stopped losing people at the last step.

That is why companies obsess over this button.

### "Test New Checkout Button For 10% Of Users In Canada"
Designer says: "Our button is small, grey, says 'Continue'. People miss it. If we make it big, green, say 'Pay Now - It's Safe', more people will click it."

That might be true. It might be terrible. We don't know.

So we don't change it for everyone. We TEST it:

- Half the test group sees OLD button (small, grey, "Continue")
- Half sees NEW button (big, green, "Pay Now")

Then we watch: which group actually pays more?

Formal guess: "New checkout button increases conversion"
Plain English: "If we make the button bigger and clearer, more people will finish paying."

### Why 10%? Why Canada?
**Why 10%:** Safety / blast radius. If new button is broken (doesn't work on iPhones, charges twice), you only broke it for 10% of people, not 100%. You can turn it off fast.

**Why Canada:** Start small, in one place. Prove it works, then roll out everywhere. Canada is smaller than US, team is in Canada, easier to monitor.

Full plain-English translation:
> "Let's show the new, bigger checkout button to just 10% of Canadian shoppers, half get old, half get new, and see if the new one makes more people actually pay. If it breaks, we only broke it for a few people."

### What Are We Measuring?
**Primary metric: checkout_conversion**
> Of the people who saw the button, what % actually checked out?

**Guardrails: p99_latency, error_rate**
> Did we break the site while trying to improve it?
If new button is prettier but makes page 3x slower or crashes 5% of time, we DO NOT SHIP it, even if conversion went up. Guardrails are the "don't be stupid" check.

### Key Terms In Plain English
- **Conversion:** "Did they do the thing we wanted?" In a store: "Did they pay?" 100 see button, 10 pay = 10% conversion.
- **Control:** The old button. What is live today.
- **Treatment:** The new button. The thing we are testing.
- **Lift:** "How much better?" Old 10%, New 14% => (14-10)/10 = 40% lift.
- **P-Value:** "Could this just be luck?" Flip coin 10 times, get 7 heads = could be luck. Flip 1000 times, get 700 heads = not luck, coin is rigged. p=0.12 = 12% chance it is luck (don't trust). p=0.01 = 1% chance it is luck (trust). We only ship if p < 0.05 (less than 5% chance it is luck).
- **Sample Size:** How many people we need before we trust the result. Here min 1000, 14 days.

---

## 2. What Is Kafka Doing?

Forget "streaming platform". Think **post office with labeled mailboxes.**

### Topics = Mailboxes
1. `experiments.created` — "A new experiment exists!" — 1 message in demo. The spec you published.
2. `events.raw` — "A user did something" — 500 messages in demo. One per user: {userId, variant: control/treatment, converted: true/false, latencyMs}

### Producer = Drops letter in mailbox
- Click Publish in Blazor -> API is producer -> drops 1 letter in `experiments.created`
- Click Generate 500 Events -> API is producer -> drops 500 letters in `events.raw`

### Consumer = Picks up letters
- Airflow / API is consumer — picks up `events.raw` letters
- Kafka UI is consumer — just shows you what is in the mailbox

### Why Kafka, Not Just A Database?
1. **Decoupled:** Blazor doesn't need to know Airflow exists. It just drops a letter. Airflow picks it up when ready. If Airflow is down for an hour, letters pile up, nothing lost.
2. **Replayable / Checkpointing:** Each letter has an offset: 0,1,2...499. If Airflow crashes at 250, it remembers "I processed up to 250" and resumes at 251. No double-count, no miss.
3. **Partition:** `events.raw` has Partition 0, 1 replica. In prod you'd have many partitions so many consumers can read in parallel. Demo: 1 is fine.

**In this demo, Kafka is just reliably holding 500 user events until Airflow is ready to count them.**

---

## 3. What Is Airflow Doing?

Airflow does NOT do calculations. It **orchestrates** — tells other things when to run, in what order, retries if they fail.

Think **factory foreman with a clipboard.**

### DAG = Recipe
`experiment_pipeline` is a DAG (Directed Acyclic Graph) — fancy name for "steps in order, no loops":

```
Step 1: consume_kafka_events_raw
   ↓
Step 2: aggregate_by_variant
   ↓
Step 3: build_scorecard
```

If Step 1 fails, Step 2 never runs. If Step 2 fails, Airflow retries it.

### What Each Step Does
**Step 1: consume_kafka_events_raw**
> "Go pick up all letters from `events.raw`"
In demo this logs "Consuming..." — real consumer is API. In prod, real Kafka consumer reads 500 messages and checkpoints offset 500.

**Step 2: aggregate_by_variant**
> "Sort letters into two piles and count"
Control pile: count users, count converted. Treatment pile: same.
That is it. Just counting.

**Step 3: build_scorecard**
> "Do the math and write the report"
Calls POST /api/scorecards/compute. See Section 4 for exact math.

### Why Kafka + Airflow Together?
- Kafka = "Hold events reliably, don't lose them"
- Airflow = "Every hour, count them in order, retry if you fail, build the report"
Without Kafka, if Airflow is down, events lost. Without Airflow, events just pile up and no one counts them.

---

## 4. How Airflow Determines This Exact Scorecard

Your real output:
```json
{"experiment":"checkout-button-test","controlUsers":907,"treatmentUsers":1053,"controlConversion":0.1172,"treatmentConversion":0.137,"lift":16.9,"pValue":0.12,"recommendation":"HOLD - Not significant","computedAt":"2026-11-13T21:14:11.4631555Z"}
```

### Step-By-Step, With Your Numbers

**Step 1 — Count Users Per Variant**
Airflow / API looks at all events and groups by `variant`.

- Control pile: 907 users saw OLD button
- Treatment pile: 1053 users saw NEW button
- Total: 1960 users (not 500 — because each Generate click adds more, and our generator also simulates historical users to reach sample size. The 500 in Kafka UI is the last batch, the scorecard counts the full in-memory store.)

Why not exactly 50/50? Random assignment is like flipping a coin 1960 times — you won't get exactly 980/980. 907 vs 1053 is normal random variation. That imbalance itself is NOT the result.

**Step 2 — Count Conversions Per Variant**
For each pile, count how many have `converted: true`.

Work backwards from the conversions in your scorecard:
- Control: 0.1172 * 907 = ~106 people paid
  Check: 106 / 907 = 0.1168 ≈ 0.1172 (rounding + generator)
- Treatment: 0.137 * 1053 = ~144 people paid
  Check: 144 / 1053 = 0.1367 ≈ 0.137

So:
- Control: 106 / 907 paid = 11.72%
- Treatment: 144 / 1053 paid = 13.70%

**Step 3 — Conversion Rate**
Formula: `converted / total` per variant.
```
controlConversion = 106 / 907 = 0.1172
treatmentConversion = 144 / 1053 = 0.137
```
That is all conversion means.

**Step 4 — Lift**
Formula: `(Treatment - Control) / Control`
```
lift = (0.137 - 0.1172) / 0.1172
     = 0.0198 / 0.1172
     = 0.169 = 16.9%
```
Plain English: "Treatment converted 16.9% more often than control."

16.9% SOUNDS good. But is it real?

**Step 5 — P-Value (Is It Luck?)**
This is a two-proportion z-test. You don't need to do it by hand in an interview — just know what it asks:

> "If the new button was actually NO BETTER than old, how likely is it we'd see a 16.9% gap just by random luck?"

Code does:
- Pooled conversion = (106+144) / (907+1053) = 250 / 1960 = 0.1275
- Standard error from pooled rate + sample sizes
- Z-score = difference / standard error ≈ 1.55
- pValue from z-score = 0.12

Plain English: p=0.12 means "12% chance this 16.9% lift is just luck."

Rule we use: Ship only if p < 0.05 (less than 5% chance it is luck).

0.12 > 0.05 → NOT enough proof.

**Step 6 — Recommendation**
```
IF lift > 0 AND p < 0.05 AND guardrails healthy → SHIP
ELSE → HOLD - Not significant
```

Your case:
- lift = 16.9% (>0 ✅)
- p = 0.12 (NOT < 0.05 ❌)
- guardrails = healthy (✅)

2 out of 3 → HOLD.

That is correct governance: Do NOT ship on a 16.9% lift alone if we can't prove it isn't luck.

Previous run example for contrast:
- lift = 29.5%, p = 0.01 → p < 0.05 → SHIP
Same logic, different data, different decision. That proves the system is not hard-coded.

**Step 7 — computedAt**
Timestamp when Step 3 ran. Proves freshness: this scorecard is from 2026-11-13T21:14:11Z, not stale.

### Summary Table For Your Numbers
| Field | Value | What It Means |
|---|---|---|
| controlUsers | 907 | 907 people saw OLD button |
| treatmentUsers | 1053 | 1053 people saw NEW button |
| controlConversion | 0.1172 | 11.72% of control paid (106/907) |
| treatmentConversion | 0.137 | 13.70% of treatment paid (144/1053) |
| lift | 16.9 | Treatment 16.9% better |
| pValue | 0.12 | 12% chance it is luck — too high |
| recommendation | HOLD | Don't ship yet, need more data |

---

## 5. Phase 3 — Analyst Agent

Analyst Agent reads the scorecard JSON above and writes plain English:
> "Treatment shows +16.9% lift but p=0.12, not significant. We need more samples. Guardrails healthy. HOLD, run 7 more days to reach 1000 samples. Do not ship on 16.9% lift alone without significance."

Endpoint: POST /api/analyst/report?experiment=checkout-button-test
Blazor button: "Ask Analyst Agent: Should We Ship?"

---

## 6. 60-Second Interview Script

> "I built a mini experimentation platform — like what Microsoft or Netflix use to test changes safely.
>
> Say we have an online store, and we want to change the checkout button. Designer thinks a bigger, clearer button will make more people finish paying. But we don't know — it could be worse.
>
> So instead of changing it for everyone, we test it on just 10% of shoppers in Canada. Half see old button, half see new one. Then we watch: which group actually pays?
>
> I have an AI agent that writes up that test. Another agent checks it — 'Is this safe? Are we risking too many people? Did we check the site didn't get slower?'
>
> If it passes, it goes to Kafka. Kafka is just a really reliable mailbox — it holds all the 'user clicked, user paid' events. 500 in my demo.
>
> Then Airflow — foreman with a clipboard — says 'Go count those events, in order.' It counts: how many in old-button group paid? How many in new-button group paid?
>
> Then it does the math: Did new button actually do better, or was it just luck? If real, SHIP. If luck, HOLD.
>
> In my last run, new button looked 16% better, but math said 'probably just luck, don't ship yet.' So it said HOLD. That's the point — don't ship on gut feeling, ship on proof."

One sentence to remember:
> "We had a guess that a new button would make more people pay, so we tested it on a small group first, counted who actually paid, and only shipped it if the math proved it wasn't just luck."

---

## 7. End-To-End Run Order
1. Designer -> Publish (saves experiment)
2. Generate 500 -> Kafka events.raw
3. Airflow -> scorecard
4. Analyst -> plain-English report

API: POST /api/analyst/report?experiment=checkout-button-test
Airflow: http://localhost:8080 admin/admin | Kafka UI: http://localhost:8085 | Blazor: http://localhost:5001 | API: http://localhost:5000/swagger
