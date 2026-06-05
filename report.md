# AIS Collision Detection

## Findings

The pipeline identified a collision between two vessels in the Baltic Sea on 13 December 2021.

| Field | Value |
|---|---|
| Vessel A | KARIN HOEJ (MMSI 219021240) |
| Vessel B | MV SCOT CARRIER (MMSI 232018267) |
| Timestamp | 2021-12-13 02:27:29 UTC |
| Latitude | 55.223079 N |
| Longitude | 14.243707 E |
| Distance at closest approach | 4.1 m |
| Collision score | 413.4 |

This matches the real-world incident in which the British cargo vessel Scot Carrier overtook the Danish hopper barge Karin Hoej in the Bornholmsgat traffic separation scheme southwest of Bornholm, Denmark. Karin Hoej capsized after the collision, resulting in two fatalities. (https://en.wikipedia.org/wiki/Karin_Høj)

---

## Dataset

Source: Danish Maritime Authority AIS data (http://web.ais.dk/aisdata/)

Timeframe: December 1-31 2021 (31 CSV files, approximately 60 GB total)

---

## Methodology

### 1. Data Loading

All 31 daily CSV files are loaded in a single Spark read operation. Spark distributes the read across all available CPU cores. The CSV header is used for column identification. Only the 9 columns needed by the pipeline are selected and cast to their target types, with the remaining columns discarded before any further processing.

### 2. Filtering

Filters are applied in order from cheapest to most expensive. All filters are lazy - they build a query plan but execute only when the persist step materializes the data.

**Time filter** - restricts to December 1-31 2021.

**Geographic filter** - keeps only pings within 50nm of 55.225N 14.245E. A rectangular bounding box eliminates most pings cheaply, then exact Haversine distance is computed on the surviving set.

**Class A filter** - retains only Class A transponder records.

**SAR transponder filter** - removes all records with MMSI prefix 111. These are Search and Rescue transponders carried by aircraft and specialized vessels.

### 3. Data Cleaning

**Duplicate and noise removal**

Duplicates arise when the same AIS broadcast is received by multiple base stations. For each (MMSI, timestamp) group, the ping closest to the vessel's previous known position is kept.

GPS noise pings are removed when the implied speed from the previous ping exceeds 50 knots.

**Stationary vessel filter** - vessels whose median SOG is below 0.5 knots are removed.

### 4. Collision Detection

**Grid bucketing** - each ping is assigned to a 0.005-degree grid cell (~0.3nm) and a 1-minute time bucket. The pipeline uses neighbor expansion: a 9-row broadcast table of offsets [-1, 0, 1] x [-1, 0, 1] expands each vessel's cell to all 9 neighbors, then an equality join matches against the other vessel's actual cell.

**Haversine distance** - exact distance is computed for all candidate pairs. Pairs within 0.1nm (~185m) are retained.

**Closing speed** - the rate of convergence is computed from previous ping positions. Pairs with positive closing speed are retained.

**Candidate reduction** - after distance and closing speed filtering, only the 3 closest approach pings per vessel pair are kept, ordered by distance ascending then closing speed descending. Without this step, the same two vessels could contribute hundreds of rows (one per close ping) to the scoring stage. Keeping 3 rows per pair reduces the scoring workload by over 95% while retaining enough candidates for the scorer to find the best event.

### 5. Post-Event Behavior Analysis

For each candidate pair, the 2 minutes before and after the close approach are analysed.

**SOG drop** - average speed before minus average speed after.

**COG change** - circular angular difference between mean heading before and after.

**Post-event divergence** - angular difference between the two vessels post-event headings. A real collision causes the vessels to move in different directions.

### 6. AIS Silence Detection

For each candidate pair, the pipeline checks whether either vessel stopped transmitting within 2 minutes of the close approach event.

### 7. Collision Scoring

Every candidate pair receives a single numeric score.

```
score = silence_score + behavior_score - penalties
```

**Silence score (dominant signal):**
1 vessel going silent = 320 points, 2 vessels = 350 points. The weight of 320 exceeds the maximum possible behavioral score (~313 points) for any non-silent vessel, guaranteeing any genuinely silent pair ranks above all non-silent pairs. Silence scoring is non linear because two vessels entering port together both routinely switch off their transponders.

A speed-of-silence bonus adds up to 20 points based on how quickly the vessel went silent (0 seconds = full bonus, 120 seconds = zero).

**Behavioral score:**
- SOG drop x 8 (capped at 20 knots)
- COG change x 0.4 (capped at 180 degrees)
- Post-event divergence x 0.3 (capped at 180 degrees)
- Closing speed x 3 (capped at 30 knots)

**Penalties:**
- No SOG reaction: up to -10 points (if SOG drop < 1 knot)
- Distance over 100m: -2 points per 100m above threshold
- Scene density: -3 points per vessel beyond 2 in the grid cell

---

## Top 5 Candidates

| Rank | Pair | Score | Distance | Silence | Timestamp |
|---|---|---|---|---|---|
| 1 | KARIN HOEJ x MV SCOT CARRIER | 413.4 | 4m | 1 | 2021-12-13 02:27:29 |
| 2 | 245039000 x SEA ENTERPRISE | 379.8 | 12m | 2 | 2021-12-10 12:05:09 |
| 3 | EXPRESS 1 x LILL | 160.2 | 125m | 0 | 2021-12-21 19:10:38 |
| 4 | KYST-FRB19 x RESCUE SJOMANSHUSET | 153.0 | 47m | 0 | 2021-12-13 06:53:59 |
| 5 | EXPRESS 1 x BALTIC EXPLORER | 151.5 | 133m | 0 | 2021-12-18 10:49:20 |

**Rank 1 - KARIN HOEJ x MV SCOT CARRIER**
Karin Hoej was a Danish hopper barge carrying sand southwest through the Bornholmsgat traffic separation scheme. Scot Carrier was a British cargo vessel travelling the same route northeast, overtaking the slower barge. At 02:27:29 UTC Scot Carrier struck Karin Hoej, causing her to capsize. Karin Hoej's last ping shows her SOG jumping and COG swinging sharply - the impact signature - after which she went completely silent with 18 days of dataset remaining. Scot Carrier's COG rotated 108 degrees and speed dropped from 12 to near zero within 2 minutes before she resumed her voyage. Two crew members of Karin Hoej were never found. The collision was later attributed to the watch officer on Scot Carrier being under the influence of alcohol.

**Rank 2 - Viking (tug) x SEA ENTERPRISE**
MMSI 245039000 is the Viking, a Dutch tug. Tugs routinely work in close proximity to other vessels - pushing, towing, positioning. A tug at 12m from another vessel with both going silent is plausible as a towing handover where both crews switch off AIS. The behavioral signals are weaker than Karin Hoej, suggesting neither vessel reacted violently. Most likely a routine operational close approach, not a collision.

**Ranks 3 and 5 - EXPRESS 1 x LILL and EXPRESS 1 x BALTIC EXPLORER**
Express 1 is a Danish high-speed ferry operating the Ronne route. Both encounters involve Express 1 at times consistent with ferry departures and arrivals in port. These are vessel traffic in and around Ronne harbour, not open-water collisions. Neither pair has an AIS silence signal (silence=0) and both score below 165. The pipeline does not filter port proximity, which is why ferry port operations appear in the lower ranks.

**Rank 4 - KYST-FRB19 x RESCUE SJOMANSHUSET**
KYST-FRB19 is a Danish coastal rescue boat. RESCUE SJOMANSHUSET is a Swedish sea rescue vessel. Both were responding to the Karin Hoej capsizing on December 13 - this is two rescue boats working the scene at 06:53 UTC, about four hours after the collision. They score silence=0 because both kept transmitting throughout. The close approach is two rescue vessels coordinating at the scene.

---

## Limitations

The pipeline finds two-vessel collisions. A multi-vessel pile-up would be partially detected - the closest pair would be identified, but the full extent of the incident would not be characterised.

The pipeline does not filter port proximity. Close approaches in port areas (vessels docking, ferries manoeuvring) appear in the lower ranks but score well below genuine open-water incidents due to the absence of silence and weak behavioral signals.

The scoring weights reflect subjective judgements about the relative importance of each signal.
