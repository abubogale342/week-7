import os
import csv
from pathlib import Path
from ultralytics import YOLO
import pandas as pd

# Paths
IMAGES_DIR = Path("data/raw/telegram_media")

intermediate_dir = Path("data/intermediate")
intermediate_dir.mkdir(parents=True, exist_ok=True)  # ✅ Creates if missing


OUTPUT_CSV = Path("data/intermediate/image_detections.csv")

# Load YOLOv8 model (use yolov8n.pt or yolov8s.pt for speed)
model = YOLO("yolov8n.pt")

# Find image files
image_paths = list(IMAGES_DIR.rglob("*.jpg"))

results = []

for img_path in image_paths:
    detections = model(img_path)
    
    for det in detections[0].boxes:
        cls_id = int(det.cls[0])
        conf = float(det.conf[0])
        cls_name = model.names[cls_id]

        relative_path = str(Path("data/raw/telegram_media") / img_path.relative_to(IMAGES_DIR))

        results.append({
            "file_path": relative_path,
            "detected_object_class": cls_name,
            "confidence_score": conf,
        })

# Save results to CSV
pd.DataFrame(results).to_csv(OUTPUT_CSV, index=False)
print(f"Saved {len(results)} detections to {OUTPUT_CSV}")
