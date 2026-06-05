# AIS Collision Detection

---

## Requirements

- Docker Desktop (running)
- 8GB+ RAM recommended for the full December run
- December 2021 AIS CSV files from http://web.ais.dk/aisdata/

---

## Project structure

```
Dockerfile
docker-compose.yml
requirements.txt
report.md
data/
output/
src/
  main.py
  config.py
  noise_filter.py
  collision_detector.py
  visualizer.py
```

---

## Build

```bash
docker build -t ais-collision .
```

---

## Run

### Single day

```bash
docker run --rm -v ./data:/app/data -v ./output:/app/output -e SPARK_DRIVER_MEMORY=6g ais-collision python src/main.py --file data/aisdk-2021-12-13.csv
```

### Full December

```bash
docker compose up --build
```

---

## Output

All files are written to `./output/`:

| File | Description |
|---|---|
| `collision_trajectory.html` | Map for the winning pair (rank 1) |
| `collision_trajectory_top1.html` | Same as above |
| `collision_trajectory_top2.html` | Map for rank 2 pair |
| `collision_trajectory_top3.html` | Map for rank 3 pair |
| `collision_trajectory_top4.html` | Map for rank 4 pair |
| `collision_trajectory_top5.html` | Map for rank 5 pair |
| `collision_report.txt` | Text report with MMSI, names, timestamp, coordinates |

Each map shows both vessel trajectories over a 20-minute window centred on the close approach time. Solid lines = before, dashed = after. The collision marker is placed at the geometrically closest recorded ping.

---

## Configuration

All tunable parameters are in `src/config.py`:

| Parameter | Default | Description |
|---|---|---|
| `CENTER_LAT / CENTER_LON` | 55.225, 14.245 | Search area centre |
| `RADIUS_NM` | 50 | Search radius in nautical miles |
| `MAX_SPEED_KN` | 50 | Implied speed threshold for GPS noise |
| `MIN_SOG_KN` | 0.5 | Minimum median SOG to keep a vessel |
| `COLLISION_DIST_NM` | 0.1 | Collision distance threshold (~185m) |
| `GRID_CELL_DEG` | 0.005 | Spatial grid cell size (~0.3nm) |
| `TIME_BUCKET_MINUTES` | 1 | Time bucket width for join |

---

## Pipeline overview

1. Load all CSVs in parallel via Spark
2. Time filter (December 1-31 2021)
3. Geographic filter - bounding box then Haversine, 50nm radius
4. Class A vessels only
5. Remove SAR transponders (MMSI prefix 111 - aircraft)
6. Clean track points - duplicate removal and GPS noise
7. Remove stationary vessels
8. Persist clean data to memory and disk
9. Grid-bucketed spatial self-join - avoids O(N^2) product
10. Haversine distance filter (<0.1nm) and closing speed computation
11. Keep top 3 candidates per vessel pair by distance
12. Add post-event behavior signals - SOG drop, COG change, divergence
13. Add AIS silence detection - vessel going dark after close approach
14. Score every candidate pair with weighted collision score
15. Pick winner by score
16. Generate trajectory maps for top 5 distinct pairs and write report

---

## Scoring system

Every candidate pair receives a numeric collision score. Higher = more collision-like. The winner is the pair with the highest score.

**Positive signals:**

| Signal | Weight | Notes |
|---|---|---|
| AIS silence (1 vessel) | +320 | Exceeds max behavioral score |
| AIS silence (2 vessels) | +350 | Sublinear - two going quiet is more common |
| Silence speed bonus | up to +20 | 0 seconds = full bonus |
| SOG drop | x8 per knot | Capped at 20 knots |
| COG change | x0.4 per degree | Capped at 180 degrees |
| Post-event divergence | x0.3 per degree | Capped at 180 degrees |
| Closing speed | x3 per knot | Capped at 30 knots |

**Negative penalties:**

| Signal | Penalty | Notes |
|---|---|---|
| No SOG reaction | up to -10 | If SOG drop < 1 knot |
| Distance over 100m | -2 per 100m | Closer approaches rank higher |
| Scene density | -3 per extra vessel | Busy rescue scenes penalized |


## Docker Hub

```bash
docker pull voveriukas/ais-collision:latest
```
