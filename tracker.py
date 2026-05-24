import cv2
import numpy as np
import sys
import argparse
import threading
import base64
import json
import os
from ultralytics import YOLO


# --- Configuration ---
OUTPUT_PATH = 'presence.mp4'
HIGHLIGHTS_PATH = 'highlights.mp4'
TIMESTAMPS_PATH = 'ball_timestamps.csv'

GRID_ROWS = 6
GRID_COLS = 8

# --- Tuning ---
EXPANSION_FRAME_LIMIT = 150 
BASE_MATCH_THRESHOLD = 0.6
DEFAULT_WEIGHT_COLOR = 0.6     
DEFAULT_WEIGHT_DISTANCE = 0.4  
EDGE_TOLERANCE = 150 

# Snap Back: Frame count after ReID to force a YOLO resize
SNAP_BACK_FRAME = 50

# Highlight settings
HIGHLIGHT_DURATION_SEC = 10

# Minimum frames between logged ball interactions (debounce)
INTERACTION_DEBOUNCE_FRAMES = 30  # ~1 second at 30fps

# --- LLM ReID Configuration ---
# Only invoke LLM after the player has been lost this many frames (~2s at 30fps)
LLM_FRAMES_LOST_TRIGGER = 60
# Minimum low-level score for a candidate to be worth sending to LLM
LLM_MIN_CANDIDATE_SCORE = 0.35
# LLM confidence must exceed this to confirm the re-identification
LLM_CONFIRM_THRESHOLD = 0.55
# Update the stored reference crop every N tracked frames
LLM_REFERENCE_UPDATE_INTERVAL = 90
# Give up waiting for LLM response after this many frames (~10s at 30fps)
VERIFYING_TIMEOUT_FRAMES = 300
# Frames to wait before calling LLM again after a rejection
LLM_COOLDOWN_FRAMES = 90


class PlayerIdentity:
    def __init__(self):
        self.hist = None
        self.last_grid_pos = None
        self.last_center = None 
        self.avg_w = 0
        self.avg_h = 0
        self.reference_crop = None  # BGR image used for LLM verification

    def initialize(self, frame, bbox):
        self.hist = self.extract_hist(frame, bbox)
        self.avg_w, self.avg_h = bbox[2], bbox[3]
        self.reference_crop = self._extract_crop(frame, bbox)

    def update_look(self, frame, bbox):
        new_hist = self.extract_hist(frame, bbox)
        if new_hist is not None and self.hist is not None:
            cv2.accumulateWeighted(new_hist, self.hist, 0.05)
            cv2.normalize(self.hist, self.hist, 0, 255, cv2.NORM_MINMAX)
        
        w, h = bbox[2], bbox[3]
        if w > 0 and h > 0:
            self.avg_w = 0.9 * self.avg_w + 0.1 * w
            self.avg_h = 0.9 * self.avg_h + 0.1 * h

    def update_reference_crop(self, frame, bbox):
        """Refresh the reference image used for LLM verification."""
        crop = self._extract_crop(frame, bbox)
        if crop is not None:
            self.reference_crop = crop

    def _extract_crop(self, frame, bbox):
        x, y, w, h = [int(v) for v in bbox]
        h_img, w_img = frame.shape[:2]
        x1, y1 = max(0, x), max(0, y)
        x2, y2 = min(w_img, x + w), min(h_img, y + h)
        if x2 > x1 and y2 > y1:
            crop = frame[y1:y2, x1:x2]
            return crop.copy() if crop.size > 0 else None
        return None

    def extract_hist(self, frame, bbox):
        x, y, w, h = [int(v) for v in bbox]
        h_img, w_img = frame.shape[:2]
        x = max(0, x); y = max(0, y)
        w = min(w, w_img - x); h = min(h, h_img - y)
        if w <= 0 or h <= 0: return None

        cx, cy = x + int(w*0.25), y + int(h*0.25)
        cw, ch = int(w*0.5), int(h*0.5)
        roi = frame[cy:cy+ch, cx:cx+cw]
        if roi.size > 0:
            hsv = cv2.cvtColor(roi, cv2.COLOR_BGR2HSV)
            mask = cv2.inRange(hsv, np.array((0., 60., 32.)), np.array((180., 255., 255.)))
            hist = cv2.calcHist([hsv], [0, 1], mask, [180, 256], [0, 180, 0, 256])
            cv2.normalize(hist, hist, 0, 255, cv2.NORM_MINMAX)
            return hist
        return None


class VisionReIDVerifier:
    """LLM-based player re-identification verifier using GPT-4o-mini vision."""

    def __init__(self, api_key=None):
        self.client = None
        if api_key:
            try:
                from openai import OpenAI
                self.client = OpenAI(api_key=api_key)
                print("✓ LLM ReID verifier ready (gpt-4o-mini)")
            except ImportError:
                print("⚠  openai package not found. Run: pip install openai")
        else:
            print("⚠  No API key — LLM ReID disabled. Pass --api-key or set OPENAI_API_KEY.")

    @property
    def available(self):
        return self.client is not None

    def _encode(self, img_bgr, max_dim=256):
        h, w = img_bgr.shape[:2]
        if max(h, w) > max_dim:
            scale = max_dim / max(h, w)
            img_bgr = cv2.resize(img_bgr, (int(w * scale), int(h * scale)))
        _, buf = cv2.imencode('.jpg', img_bgr, [cv2.IMWRITE_JPEG_QUALITY, 80])
        return base64.b64encode(buf).decode('utf-8')

    def verify(self, reference_crop, candidate_crop):
        """
        Blocking. Returns (is_match: bool | None, confidence: float | None, reasoning: str).
        Returns (None, None, error) on failure so callers can distinguish API errors.
        """
        if not self.available or reference_crop is None or candidate_crop is None:
            return None, None, "LLM unavailable or missing crops"

        prompt = (
            "You are a sports player re-identification system.\n"
            "Decide if the two images show the SAME player.\n\n"
            "Consider: jersey colour & number, player build, hair, skin tone, "
            "any other distinguishing feature.\n\n"
            "Reply with ONLY valid JSON (no markdown):\n"
            '{"is_same_player": true/false, "confidence": 0.0-1.0, "reasoning": "brief"}'
        )

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "text",  "text": "Reference — player last seen clearly:"},
                        {"type": "image_url", "image_url": {
                            "url": f"data:image/jpeg;base64,{self._encode(reference_crop)}",
                            "detail": "low"
                        }},
                        {"type": "text",  "text": "Candidate — is this the same player?"},
                        {"type": "image_url", "image_url": {
                            "url": f"data:image/jpeg;base64,{self._encode(candidate_crop)}",
                            "detail": "low"
                        }},
                    ]
                }],
                max_tokens=120,
                temperature=0,
            )
            text = response.choices[0].message.content.strip()
            result = json.loads(text)
            return (
                bool(result.get("is_same_player", False)),
                float(result.get("confidence", 0.0)),
                result.get("reasoning", ""),
            )
        except json.JSONDecodeError as e:
            print(f"LLM JSON parse error: {e}")
            return None, None, f"JSON error: {e}"
        except Exception as e:
            print(f"LLM API error: {e}")
            return None, None, str(e)


def _llm_verify_worker(verifier, ref_crop, cand_crop, cand_box, result):
    """Background thread target: fills `result` dict when the API call completes."""
    is_match, confidence, reasoning = verifier.verify(ref_crop, cand_crop)
    result.update({
        "done": True,
        "is_match": is_match,
        "confidence": confidence,
        "candidate_box": cand_box,
        "reasoning": reasoning,
    })
    label = "MATCH" if is_match else "NO MATCH"
    print(f"LLM ReID → {label} | conf={confidence} | {reasoning}")


def frame_to_timestamp(frame_num, fps):
    """Converts a frame number to a human-readable MM:SS.mmm timestamp string."""
    total_seconds = frame_num / fps
    minutes = int(total_seconds // 60)
    seconds = total_seconds % 60
    return f"{minutes:02d}:{seconds:06.3f}"


def save_timestamps(interactions, output_path, fps):
    """Writes ball interaction timestamps to a CSV file."""
    import csv
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['#', 'Timestamp', 'Frame', 'Grid Row', 'Grid Col'])
        for i, (frame_num, timestamp, grid_pos) in enumerate(interactions, 1):
            writer.writerow([i, timestamp, frame_num, grid_pos[0], grid_pos[1]])
    print(f"✓ Timestamps saved to: {output_path}")


def calculate_iou(box1, box2):
    x1, y1, w1, h1 = box1
    x2, y2, w2, h2 = box2
    
    xA = max(x1, x2); yA = max(y1, y2)
    xB = min(x1 + w1, x2 + w2); yB = min(y1 + h1, y2 + h2)
    
    interArea = max(0, xB - xA) * max(0, yB - yA)
    box1Area = w1 * h1
    box2Area = w2 * h2
    
    if box1Area + box2Area - interArea == 0: return 0
    return interArea / float(box1Area + box2Area - interArea)

def get_grid_coords(x, y, w, h, fw, fh):
    cx, cy = x + w//2, y + h//2
    c = int(cx / (fw / GRID_COLS))
    r = int(cy / (fh / GRID_ROWS))
    return max(0, min(GRID_ROWS-1, r)), max(0, min(GRID_COLS-1, c))

def get_search_area(row, col, fw, fh, radius):
    min_r, max_r = max(0, row-radius), min(GRID_ROWS, row+radius+1)
    min_c, max_c = max(0, col-radius), min(GRID_COLS, col+radius+1)
    return min_c*(fw//GRID_COLS), min_r*(fh//GRID_ROWS), max_c*(fw//GRID_COLS), max_r*(fh//GRID_ROWS)

def calculate_proximity_score(cand_box, last_center, max_dist_pixels):
    bx, by, bw, bh = cand_box
    cand_cx, cand_cy = bx + bw//2, by + bh//2
    last_cx, last_cy = last_center
    dist = np.sqrt((cand_cx - last_cx)**2 + (cand_cy - last_cy)**2)
    score = 1.0 - (dist / max_dist_pixels)
    return max(0.0, score)

def determine_exit_edge(grid_r, grid_c):
    if grid_c == 0: return 'left'
    if grid_c == GRID_COLS - 1: return 'right'
    if grid_r == 0: return 'top'
    if grid_r == GRID_ROWS - 1: return 'bottom'
    return None

def is_touching_edge(box, w_frame, h_frame, edge_side):
    x, y, w, h = box
    if edge_side == 'left': return x < EDGE_TOLERANCE
    elif edge_side == 'right': return (x + w) > (w_frame - EDGE_TOLERANCE)
    elif edge_side == 'top': return y < EDGE_TOLERANCE
    elif edge_side == 'bottom': return (y + h) > (h_frame - EDGE_TOLERANCE)
    return False

def is_watermark(x, y, w_img, h_img):
    if y < 100 and (x < 200 or x > w_img - 200): return True
    if y > h_img - 80 and x > w_img - 200: return True
    return False

def merge_intervals(intervals):
    if not intervals: return []
    intervals.sort(key=lambda x: x[0])
    merged = [intervals[0]]
    for current in intervals[1:]:
        previous = merged[-1]
        if current[0] <= previous[1]: 
            merged[-1] = (previous[0], max(previous[1], current[1]))
        else:
            merged.append(current)
    return merged

def extract_and_concatenate_highlights(video_path, segments, output_path, fps):
    if not segments:
        print("No highlight segments to extract.")
        return
    merged_segments = merge_intervals(segments)
    print(f"\nProcessing {len(merged_segments)} merged highlight clips...")
    cap = cv2.VideoCapture(video_path)
    w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    out = cv2.VideoWriter(output_path, cv2.VideoWriter_fourcc(*'mp4v'), fps, (w, h))
    for idx, (start_frame, end_frame) in enumerate(merged_segments):
        print(f"  Clip {idx+1}: Frame {start_frame} -> {end_frame}")
        cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
        for _ in range(start_frame, end_frame):
            ret, frame = cap.read()
            if not ret: break
            out.write(frame)
    cap.release()
    out.release()
    print(f"✓ Highlights saved to: {output_path}")


def _parse_bbox(s):
    parts = [int(x.strip()) for x in s.split(',')]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError('bbox must be four integers: x,y,width,height')
    x, y, w, h = parts
    if w <= 0 or h <= 0:
        raise argparse.ArgumentTypeError('bbox width and height must be positive')
    return tuple(parts)


_TRACKER_FACTORY = None


def _resolve_tracker_factory(module, name: str):
    """Return a zero-arg callable that creates a tracker, or None."""
    if module is None:
        return None
    create_fn = getattr(module, f'{name}_create', None)
    if create_fn is not None:
        return create_fn
    cls = getattr(module, name, None)
    if cls is None:
        return None
    create = getattr(cls, 'create', None)
    if create is not None:
        return create
    return cls if callable(cls) else None


def _create_csrt_tracker():
    """
    Create an OpenCV long-term tracker. Wheels differ:
    - opencv-contrib-python: cv2.legacy.TrackerCSRT_create or cv2.TrackerCSRT_create
    - opencv-python-headless alone: often has no trackers at all
  - OpenCV 4.13+ on some Linux wheels: legacy namespace exists but trackers missing
    """
    global _TRACKER_FACTORY
    if _TRACKER_FACTORY is not None:
        return _TRACKER_FACTORY()

    legacy = getattr(cv2, 'legacy', None)
    tracker_names = ('TrackerCSRT', 'TrackerKCF', 'TrackerMIL', 'TrackerMOSSE', 'TrackerMedianFlow')
    candidates: list[tuple[str, object]] = []

    for name in tracker_names:
        factory = _resolve_tracker_factory(legacy, name)
        if factory is not None:
            candidates.append((f'{name} (legacy)', factory))
    for name in tracker_names:
        factory = _resolve_tracker_factory(cv2, name)
        if factory is not None:
            candidates.append((name, factory))

    for label, factory in candidates:
        try:
            tracker = factory()
            if tracker is not None:
                if 'CSRT' not in label:
                    print(f"⚠  Using {label} (CSRT not available on this OpenCV build).")
                else:
                    print(f"✓ Tracker: {label}")
                _TRACKER_FACTORY = factory
                return tracker
        except Exception:
            continue

    legacy_trackers = [x for x in dir(legacy or ()) if 'Tracker' in x]
    sys.exit(
        "No OpenCV object tracker available.\n"
        f"  OpenCV {getattr(cv2, '__version__', '?')}\n"
        f"  cv2.legacy tracker symbols: {legacy_trackers or 'none'}\n\n"
        "On RunPod/Jupyter run:\n"
        "  pip uninstall opencv-python opencv-python-headless opencv-contrib-python opencv-contrib-python-headless -y\n"
        "  pip install opencv-contrib-python-headless==4.10.0.84\n"
        "Then re-run tracker.py."
    )


def _setup_player_tracker(frame, bbox, w_frame, h_frame):
    """Initialize player identity and CSRT tracker from (x, y, w, h)."""
    player = PlayerIdentity()
    player.initialize(frame, bbox)
    r, c = get_grid_coords(bbox[0], bbox[1], bbox[2], bbox[3], w_frame, h_frame)
    player.last_grid_pos = (r, c)
    player.last_center = (bbox[0] + bbox[2] // 2, bbox[1] + bbox[3] // 2)
    tracker = _create_csrt_tracker()
    tracker.init(frame, bbox)
    return player, tracker


def main():
    parser = argparse.ArgumentParser(
        description='Track a soccer player and log ball interactions.',
        epilog=(
            'RunPod / headless: use --dump-frame 0 to save preview_frame.jpg, '
            'then run with --no-gui --bbox x,y,w,h'
        ),
    )
    parser.add_argument('--video', type=str, required=True, help='Path to video file')
    parser.add_argument('--api-key', type=str, default=None,
                        help='OpenAI API key for LLM ReID (falls back to OPENAI_API_KEY env var)')
    parser.add_argument('--no-gui', action='store_true',
                        help='Headless mode (no Qt window; for Jupyter/RunPod servers)')
    parser.add_argument('--bbox', type=_parse_bbox, default=None,
                        help='Initial player box as x,y,width,height (required with --no-gui)')
    parser.add_argument('--start-frame', type=int, default=0,
                        help='Frame index to read before starting (default: 0)')
    parser.add_argument('--dump-frame', type=int, default=None, metavar='N',
                        help='Save preview_frame.jpg at frame N and exit (for picking --bbox)')
    args = parser.parse_args()
    video_path = args.video
    use_gui = not args.no_gui

    if args.no_gui and args.bbox is None:
        parser.error('--bbox is required when using --no-gui')
    if args.dump_frame is not None and args.no_gui:
        parser.error('use --dump-frame without --no-gui')

    if args.dump_frame is not None:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            sys.exit(f"Error opening {video_path}")
        cap.set(cv2.CAP_PROP_POS_FRAMES, max(0, args.dump_frame))
        ret, frame = cap.read()
        cap.release()
        if not ret:
            sys.exit(f"Could not read frame {args.dump_frame}")
        preview_path = 'preview_frame.jpg'
        cv2.imwrite(preview_path, frame)
        print(f"Wrote {preview_path} ({frame.shape[1]}x{frame.shape[0]}) at frame {args.dump_frame}")
        print("Open it in Jupyter, note the player box, then run:")
        print(f"  python tracker.py --video {video_path} --no-gui --bbox x,y,w,h")
        return

    api_key = args.api_key or os.getenv('OPENAI_API_KEY')
    verifier = VisionReIDVerifier(api_key=api_key)

    print("Loading YOLO11m...")
    model = YOLO('yolo11m.pt') 

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened(): sys.exit(f"Error opening {video_path}")

    w_frame = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    h_frame = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = int(cap.get(cv2.CAP_PROP_FPS)) or 30
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    out = cv2.VideoWriter(OUTPUT_PATH, cv2.VideoWriter_fourcc(*'mp4v'), fps, (w_frame, h_frame))
    
    window_name = "Tracker + Ball Highlights"
    if use_gui:
        cv2.namedWindow(window_name, cv2.WINDOW_NORMAL)
        cv2.createTrackbar("Seek", window_name, 0, total_frames, lambda x: cap.set(cv2.CAP_PROP_POS_FRAMES, x))

    tracker = None
    player = PlayerIdentity()
    tracking_state = "IDLE"
    paused = not use_gui
    frames_lost = 0 
    frames_tracked = 0
    exit_edge = None

    # LLM ReID state
    llm_result = {"done": False}
    llm_frames_waiting = 0
    llm_cooldown = 0        # frames to wait before next LLM call
    
    highlight_segments = []
    is_recording = False
    segment_start_frame = 0
    recording_until_frame = 0
    highlight_duration_frames = HIGHLIGHT_DURATION_SEC * fps

    # --- Timestamp tracking ---
    ball_interactions = []          # List of (frame_num, timestamp_str, grid_pos)
    last_interaction_frame = -INTERACTION_DEBOUNCE_FRAMES  # Ensures first hit is always logged
    ball_in_contact = False         # True while ball is currently in the player's grid cell

    if use_gui:
        print("--- SYSTEM READY --- (press 's' to select player, Space pause, 'q' quit)")
    else:
        start = max(0, args.start_frame)
        if start > 0:
            cap.set(cv2.CAP_PROP_POS_FRAMES, start)
        ret, frame = cap.read()
        if not ret:
            sys.exit(f"Could not read frame {start}")
        player, tracker = _setup_player_tracker(frame, args.bbox, w_frame, h_frame)
        tracking_state = "TRACKING"
        frames_tracked = 0
        frames_lost = 0
        paused = False
        cap.set(cv2.CAP_PROP_POS_FRAMES, start)
        print(f"--- HEADLESS: tracking from frame {start} with bbox {args.bbox} ---")

    progress_interval = max(1, total_frames // 20) if total_frames > 0 else 300

    while True:
        curr_pos = int(cap.get(cv2.CAP_PROP_POS_FRAMES))
        if use_gui:
            cv2.setTrackbarPos("Seek", window_name, curr_pos)

        if not paused:
            ret, frame = cap.read()
            if not ret: break
        else:
            cap.set(cv2.CAP_PROP_POS_FRAMES, curr_pos)
            ret, frame = cap.read()
            if not ret: break
        
        display = frame.copy()
        
        # Grid
        cw, rh = w_frame // GRID_COLS, h_frame // GRID_ROWS
        for c in range(1, GRID_COLS): cv2.line(display, (c*cw, 0), (c*cw, h_frame), (50, 50, 50), 1)
        for r in range(1, GRID_ROWS): cv2.line(display, (0, r*rh), (w_frame, r*rh), (50, 50, 50), 1)

        if tracking_state == "TRACKING":
            success, box = tracker.update(frame)
            
            if success:
                x, y, w, h = [int(v) for v in box]
                
                ix1 = max(0, x); iy1 = max(0, y)
                ix2 = min(w_frame, x + w); iy2 = min(h_frame, y + h)
                visible_area = max(0, ix2 - ix1) * max(0, iy2 - iy1)
                total_area = w * h
                
                if total_area > 0 and (visible_area / total_area < 0.2):
                    print("Lost: Player exited frame (Box out of bounds).")
                    success = False

                if success:
                    frames_tracked += 1
                    if llm_cooldown > 0:
                        llm_cooldown -= 1

                    # Refresh the LLM reference crop periodically during stable tracking
                    if frames_tracked % LLM_REFERENCE_UPDATE_INTERVAL == 0:
                        player.update_reference_crop(frame, box)

                    if frames_tracked % SNAP_BACK_FRAME == 0:

                        # 1. Expand the margin dynamically to give YOLO enough context
                        margin_x = max(100, int(w * 0.5))
                        margin_y = max(100, int(h * 0.5))
                        rx1, ry1 = max(0, x - margin_x), max(0, y - margin_y)
                        rx2, ry2 = min(w_frame, x + w + margin_x), min(h_frame, y + h + margin_y)
                        roi_refine = frame[ry1:ry2, rx1:rx2]
                        
                        if roi_refine.size > 0:
                            # 2. Lower conf slightly to match your SEARCHING state
                            results = model.predict(roi_refine, classes=0, conf=0.25, verbose=False)
                            
                            best_iou = 0
                            best_rect = None
                            
                            for res in results:
                                for rbox in res.boxes:
                                    bx1, by1, bx2, by2 = rbox.xyxy[0].cpu().numpy().astype(int)
                                    gx, gy = rx1 + bx1, ry1 + by1
                                    gw, gh = bx2 - bx1, by2 - by1
                                    
                                    iou = calculate_iou((x,y,w,h), (gx,gy,gw,gh))
                                    if iou > best_iou:
                                        best_iou = iou
                                        best_rect = (gx, gy, gw, gh)
                            
                            # 3. Lower the IOU threshold so heavily drifted boxes still snap back
                            if best_rect and best_iou > 0.05:
                                tracker = _create_csrt_tracker()
                                tracker.init(frame, best_rect)
                                box = best_rect
                                
                                # 4. Re-unpack variables so the drawn rectangle updates on THIS frame
                                x, y, w, h = [int(v) for v in box] 
                                print(f"SNAP BACK: Corrected Box at Frame {frames_tracked}")
            if success:
                x, y, w, h = [int(v) for v in box]
                r, c = get_grid_coords(x, y, w, h, w_frame, h_frame)
                player.update_look(frame, box)
                player.last_grid_pos = (r, c)
                player.last_center = (x + w//2, y + h//2)
                frames_lost = 0
                exit_edge = determine_exit_edge(r, c)

                # --- BALL DETECTION ---
                col_w = w_frame // GRID_COLS
                row_h = h_frame // GRID_ROWS
                gb_x1 = c * col_w
                gb_y1 = r * row_h
                gb_x2 = min(w_frame, gb_x1 + col_w)
                gb_y2 = min(h_frame, gb_y1 + row_h)
                ball_roi = frame[gb_y1:gb_y2, gb_x1:gb_x2]
                found_ball_in_grid = False

                if ball_roi.size > 0:
                    ball_results = model.predict(ball_roi, classes=32, conf=0.15, verbose=False)
                    for res in ball_results:
                        for bbox in res.boxes:
                            b_x1, b_y1, b_x2, b_y2 = bbox.xyxy[0].cpu().numpy().astype(int)
                            g_bx1, g_by1 = gb_x1 + b_x1, gb_y1 + b_y1
                            g_bx2, g_by2 = gb_x1 + b_x2, gb_y1 + b_y2
                            b_cx, b_cy = g_bx1 + (g_bx2 - g_bx1) // 2, g_by1 + (g_by2 - g_by1) // 2

                            if is_watermark(b_cx, b_cy, w_frame, h_frame): continue

                            br, bc = get_grid_coords(g_bx1, g_by1, g_bx2 - g_bx1, g_by2 - g_by1, w_frame, h_frame)
                            if (br, bc) == (r, c):
                                found_ball_in_grid = True
                                cv2.rectangle(display, (g_bx1, g_by1), (g_bx2, g_by2), (0, 255, 0), 2)

                # --- TIMESTAMP LOGGING ---
                # Log on the RISING EDGE of contact (ball just appeared in grid cell)
                # and enforce a debounce gap to avoid duplicate entries
                if found_ball_in_grid and not ball_in_contact:
                    frames_since_last = curr_pos - last_interaction_frame
                    if frames_since_last >= INTERACTION_DEBOUNCE_FRAMES:
                        timestamp_str = frame_to_timestamp(curr_pos, fps)
                        ball_interactions.append((curr_pos, timestamp_str, (r, c)))
                        last_interaction_frame = curr_pos
                        print(f"⚽ Ball interaction at {timestamp_str} (Frame {curr_pos}, Grid R{r} C{c})")
                
                ball_in_contact = found_ball_in_grid

                # --- HIGHLIGHT RECORDING ---
                if found_ball_in_grid:
                    if not is_recording:
                        is_recording = True
                        segment_start_frame = max(0, curr_pos - 15) 
                    recording_until_frame = curr_pos + highlight_duration_frames
                
                if is_recording and curr_pos >= recording_until_frame:
                    is_recording = False
                    highlight_segments.append((segment_start_frame, recording_until_frame))

                if is_recording:
                    cv2.circle(display, (w_frame-30, 30), 10, (0,0,255), -1)
                    cv2.putText(display, "REC", (w_frame-70, 35), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0,0,255), 2)

                # --- Interaction counter overlay ---
                cv2.putText(display, f"Interactions: {len(ball_interactions)}", (20, 60),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 255), 2)

                sx1, sy1, sx2, sy2 = get_search_area(r, c, w_frame, h_frame, radius=1)
                cv2.rectangle(display, (sx1, sy1), (sx2, sy2), (0, 50, 0), 2)
                cv2.rectangle(display, (x, y), (x+w, y+h), (0, 255, 0), 2)
                if not paused: out.write(frame)
            else:
                ball_in_contact = False  # Reset contact state when tracking is lost
                tracking_state = "SEARCHING"
                frames_lost = 0
                if not paused: out.write(frame)

        elif tracking_state == "SEARCHING":
            ball_in_contact = False
            frames_lost += 1
            if llm_cooldown > 0:
                llm_cooldown -= 1
            if frames_lost > 30 * fps:
                tracking_state = "IDLE"
                tracker = None
                if use_gui:
                    paused = True
                else:
                    print("Player lost for 30s — continuing without tracker (headless).")
                continue

            lr, lc = player.last_grid_pos
            if frames_lost < 30:
                cur_color, cur_dist = 0.4, 0.6
                thresh, radius = BASE_MATCH_THRESHOLD, 1
            elif frames_lost < EXPANSION_FRAME_LIMIT:
                cur_color, cur_dist = DEFAULT_WEIGHT_COLOR, DEFAULT_WEIGHT_DISTANCE
                thresh, radius = BASE_MATCH_THRESHOLD-0.05, 1   #0.6 - 0.05
            else:
                cur_color, cur_dist = 0.5, 0.5
                thresh, radius = BASE_MATCH_THRESHOLD-0.13, 2

            if exit_edge and frames_lost < 200: radius = 1

            sx1, sy1, sx2, sy2 = get_search_area(lr, lc, w_frame, h_frame, radius)
            max_dist = np.sqrt((sx2-sx1)**2 + (sy2-sy1)**2)
            cv2.rectangle(display, (sx1, sy1), (sx2, sy2), (0, 0, 255), 3)
            
            if exit_edge:
                cv2.putText(display, f"EDGE: {exit_edge}", (sx1, sy1-20), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0,0,255), 2)

            roi = frame[sy1:sy2, sx1:sx2]
            
            if roi.size > 0:
                results = model.predict(roi, classes=0, conf=0.25, verbose=False)
                best_score = 0
                best_box = None
                
                for result in results:
                    for box in result.boxes:
                        bx1, by1, bx2, by2 = box.xyxy[0].cpu().numpy().astype(int)
                        bw, bh = bx2 - bx1, by2 - by1
                        
                        aspect_ratio = bh / float(bw)
                        expected_area = player.avg_w * player.avg_h
                        area_ratio = (bw * bh) / expected_area if expected_area > 0 else 0

                        if exit_edge and frames_lost < 200:
                            gx, gy = sx1+bx1, sy1+by1
                            if not is_touching_edge((gx,gy,bw,bh), w_frame, h_frame, exit_edge):
                                continue
                        elif not exit_edge:
                            if aspect_ratio < 0.8: continue 
                            if area_ratio < 0.4 or area_ratio > 3.0: continue
                        
                        cand_roi = roi[by1:by2, bx1:bx2]
                        if cand_roi.size > 0:
                            hsv_cand = cv2.cvtColor(cand_roi, cv2.COLOR_BGR2HSV)
                            mask = cv2.inRange(hsv_cand, np.array((0., 60., 32.)), np.array((180., 255., 255.)))
                            cand_hist = cv2.calcHist([hsv_cand], [0, 1], mask, [180, 256], [0, 180, 0, 256])
                            cv2.normalize(cand_hist, cand_hist, 0, 255, cv2.NORM_MINMAX)
                            
                            s_col = max(0.0, cv2.compareHist(player.hist, cand_hist, cv2.HISTCMP_CORREL))
                            gx, gy = sx1+bx1, sy1+by1
                            s_dist = calculate_proximity_score((gx, gy, bw, bh), player.last_center, max_dist)
                            
                            total = (s_col * cur_color) + (s_dist * cur_dist)
                            cv2.putText(display, f"{total:.2f}", (gx, gy-5), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (0,255,255), 1)
                            cv2.rectangle(display, (gx, gy), (gx+bw, gy+bh), (0, 100, 255), 1)

                            if total > best_score:
                                best_score = total
                                best_box = (gx, gy, bw, bh)

                use_llm = (
                    verifier.available
                    and frames_lost >= LLM_FRAMES_LOST_TRIGGER
                    and llm_cooldown == 0
                    and player.reference_crop is not None
                )

                if best_box and best_score > thresh:
                    if use_llm:
                        # Long absence — verify with LLM before committing to tracker
                        bx, by, bw, bh = best_box
                        cand_crop = frame[
                            max(0, by):min(frame.shape[0], by + bh),
                            max(0, bx):min(frame.shape[1], bx + bw)
                        ].copy()
                        llm_result = {"done": False}
                        llm_frames_waiting = 0
                        threading.Thread(
                            target=_llm_verify_worker,
                            args=(verifier, player.reference_crop, cand_crop, best_box, llm_result),
                            daemon=True,
                        ).start()
                        tracking_state = "VERIFYING"
                        print(f"LLM verify triggered — low-level score={best_score:.2f}, frames_lost={frames_lost}")
                    else:
                        # Short absence or no LLM — direct re-init (original behaviour)
                        tracker = _create_csrt_tracker()
                        tracker.init(frame, best_box)
                        tracking_state = "TRACKING"
                        frames_tracked = 0
                        print(f"Recovered! Score: {best_score:.2f}")
                        bx, by, bw, bh = best_box
                        cv2.rectangle(display, (bx, by), (bx+bw, by+bh), (0, 255, 255), 2)

                elif best_box and best_score > LLM_MIN_CANDIDATE_SCORE and use_llm:
                    # Score too low for direct ReID but player was lost long — let LLM decide
                    bx, by, bw, bh = best_box
                    cand_crop = frame[
                        max(0, by):min(frame.shape[0], by + bh),
                        max(0, bx):min(frame.shape[1], bx + bw)
                    ].copy()
                    llm_result = {"done": False}
                    llm_frames_waiting = 0
                    threading.Thread(
                        target=_llm_verify_worker,
                        args=(verifier, player.reference_crop, cand_crop, best_box, llm_result),
                        daemon=True,
                    ).start()
                    tracking_state = "VERIFYING"
                    print(f"LLM verify triggered — marginal score={best_score:.2f}, frames_lost={frames_lost}")

            if not paused: out.write(frame)

        elif tracking_state == "VERIFYING":
            ball_in_contact = False
            frames_lost += 1
            llm_frames_waiting += 1

            # Show the candidate box being evaluated
            cand_box = llm_result.get("candidate_box")
            if cand_box:
                bx, by, bw, bh = cand_box
                cv2.rectangle(display, (bx, by), (bx + bw, by + bh), (0, 165, 255), 2)
                cv2.putText(display, "AI Verifying...", (bx, by - 10),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 165, 255), 2)

            if llm_result.get("done"):
                is_match = llm_result.get("is_match")
                confidence = llm_result.get("confidence") or 0.0

                if is_match and confidence >= LLM_CONFIRM_THRESHOLD:
                    best_box = llm_result["candidate_box"]
                    tracker = _create_csrt_tracker()
                    tracker.init(frame, best_box)
                    tracking_state = "TRACKING"
                    frames_tracked = 0
                    llm_frames_waiting = 0
                    llm_result = {"done": False}
                    print(f"✓ LLM confirmed ReID (conf={confidence:.2f}). Tracker re-initialized.")
                else:
                    tracking_state = "SEARCHING"
                    llm_cooldown = LLM_COOLDOWN_FRAMES
                    llm_frames_waiting = 0
                    llm_result = {"done": False}
                    print(f"✗ LLM rejected candidate (match={is_match}, conf={confidence:.2f}). Resuming search.")

            elif llm_frames_waiting > VERIFYING_TIMEOUT_FRAMES:
                tracking_state = "SEARCHING"
                llm_cooldown = LLM_COOLDOWN_FRAMES
                llm_frames_waiting = 0
                llm_result = {"done": False}
                print("LLM verification timed out. Resuming search.")

            # Honour the global lost-too-long timeout
            if frames_lost > 30 * fps:
                tracking_state = "IDLE"
                tracker = None
                if use_gui:
                    paused = True
                else:
                    print("Player lost for 30s — continuing without tracker (headless).")

            if not paused: out.write(frame)

        status = f"Mode: {tracking_state}"
        cv2.putText(display, status, (20, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2)
        if use_gui:
            cv2.imshow(window_name, display)
            key = cv2.waitKey(20) & 0xFF
            if key == ord('q'): break
            elif key == 32: paused = not paused
            elif key == ord('s'):
                paused = True
                bbox = cv2.selectROI(window_name, frame, fromCenter=False, showCrosshair=True)
                if bbox != (0, 0, 0, 0):
                    player, tracker = _setup_player_tracker(frame, bbox, w_frame, h_frame)
                    tracking_state = "TRACKING"
                    frames_tracked = 0
                    frames_lost = 0
                    ball_in_contact = False
                    llm_result = {"done": False}
                    llm_frames_waiting = 0
                    llm_cooldown = 0
                    paused = False
        elif not use_gui and curr_pos > 0 and curr_pos % progress_interval == 0:
            pct = 100 * curr_pos / total_frames if total_frames else 0
            print(f"  frame {curr_pos}/{total_frames} ({pct:.0f}%) — {tracking_state}")

    cap.release()
    out.release()
    if use_gui:
        cv2.destroyAllWindows()
    
    if is_recording: 
        highlight_segments.append((segment_start_frame, recording_until_frame))
    if highlight_segments:
        extract_and_concatenate_highlights(video_path, highlight_segments, HIGHLIGHTS_PATH, fps)
    
    # --- Save timestamps ---
    save_timestamps(ball_interactions, TIMESTAMPS_PATH, fps)

if __name__ == "__main__":
    main()