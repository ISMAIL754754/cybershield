"""Producteur CyberShield : rejoue les flux réseau CICIDS2017 (CSV) vers un topic Kafka.

- Lecture par blocs (les CSV font plusieurs centaines de Mo).
- Noms de colonnes normalisés (snake_case) ; Infinity/NaN -> null.
- Chaque événement reçoit un event_id (UUID), un event_time (UTC, ISO 8601) et source_file.
- Clé Kafka = event_id (répartition uniforme sur les partitions).
- --drop-label retire la cible (simulation d'inférence réelle).
"""
import argparse, glob, json, os, re, time, uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from kafka import KafkaProducer


def norm(col: str) -> str:
    return re.sub(r"[^0-9a-z]+", "_", col.strip().lower()).strip("_")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9094"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "network-flows"))
    p.add_argument("--dir", default=os.getenv("DATASET_DIR", "./data"))
    p.add_argument("--rate", type=float, default=float(os.getenv("PRODUCER_RATE", 200)),
                   help="événements/seconde (0 = vitesse maximale)")
    p.add_argument("--limit", type=int, default=0, help="nombre max d'événements (0 = tout)")
    p.add_argument("--loop", action="store_true", help="rejouer en boucle")
    p.add_argument("--drop-label", action="store_true")
    a = p.parse_args()

    files = sorted(glob.glob(os.path.join(a.dir, "*.csv")))
    if not files:
        raise SystemExit(f"Aucun CSV trouvé dans {a.dir} (placer les fichiers CICIDS2017 dans ./data)")

    prod = KafkaProducer(
        bootstrap_servers=a.bootstrap,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
        acks="all", linger_ms=20, retries=5,
    )
    sent, t0 = 0, time.time()
    while True:
        for f in files:
            print(f"[producer] fichier {os.path.basename(f)}", flush=True)
            for chunk in pd.read_csv(f, chunksize=5000, encoding="latin1", low_memory=False):
                chunk.columns = [norm(c) for c in chunk.columns]
                chunk = chunk.replace([np.inf, -np.inf], np.nan)
                if a.drop_label:
                    chunk = chunk.drop(columns=["label"], errors="ignore")
                chunk = chunk.astype(object).where(chunk.notna(), None)
                for rec in chunk.to_dict("records"):
                    eid = str(uuid.uuid4())
                    rec["event_id"] = eid
                    rec["event_time"] = datetime.now(timezone.utc).isoformat()
                    rec["source_file"] = os.path.basename(f)
                    prod.send(a.topic, key=eid, value=rec)
                    sent += 1
                    if a.rate > 0 and sent % max(1, int(a.rate)) == 0:
                        time.sleep(max(0.0, sent / a.rate - (time.time() - t0)))
                    if a.limit and sent >= a.limit:
                        prod.flush(); print(f"[producer] {sent} événements envoyés"); return
                prod.flush()
                print(f"[producer] {sent} événements envoyés", flush=True)
        if not a.loop:
            break
    prod.flush()
    print(f"[producer] terminé : {sent} événements")


if __name__ == "__main__":
    main()
