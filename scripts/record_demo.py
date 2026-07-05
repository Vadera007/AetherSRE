"""
AetherSRE — Automated Screen Recorder & UI Walking Agent (Day 7 Supplement)
========================================================================
Records the AetherSRE dashboard operations and runs mouse choreographies.
"""

from __future__ import annotations

import os
import sys
import time
import signal
import threading
import subprocess
import webbrowser
from typing import Any, Final

import numpy as np
import cv2
import pyautogui

# Adjust PyAutoGUI settings
pyautogui.FAILSAFE = False
pyautogui.MINIMUM_DURATION = 0.5

# Recorder constants
FPS: Final[int] = 20
FRAME_INTERVAL: Final[float] = 1.0 / FPS
OUTPUT_DIR: Final[str] = "docs/assets"
OUTPUT_FILE: Final[str] = os.path.join(OUTPUT_DIR, "demo_walkthrough.mp4")

# Global stop signal for recording thread
recording_active = False


def record_loop(width: int, height: int, writer: cv2.VideoWriter) -> None:
    """Asynchronous loop capturing screenshot frames at constant FPS."""
    global recording_active
    next_frame_time = time.monotonic()
    
    while recording_active:
        t0 = time.monotonic()
        
        # Capture screenshot frame
        img = pyautogui.screenshot()
        frame = np.array(img)
        frame = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)
        
        # Write to video file
        writer.write(frame)
        
        # Calculate precise drift timeout sleep
        next_frame_time += FRAME_INTERVAL
        sleep_time = next_frame_time - time.monotonic()
        if sleep_time > 0:
            time.sleep(sleep_time)


def run_choreography() -> None:
    """Executes the pre-planned UI walking actions."""
    width, height = pyautogui.size()
    print(f"Screen resolution detected: {width}x{height}")
    
    # ── Phase 1: Browser Launch ──────────────────────────────────────────────
    print("Phase 1: Booting Browser and Navigating Dashboard...")
    dashboard_url = "http://localhost:8000/dashboard"
    webbrowser.open(dashboard_url)
    time.sleep(5)  # Wait for page load and rendering
    
    # Focus browser window (click neutral background spot)
    pyautogui.click(width // 2, 80)
    time.sleep(2)
    
    # ── Phase 2: Triggering Load Ingestion ───────────────────────────────────
    print("Phase 2: Launching High-Throughput Load Test Rig...")
    load_proc = subprocess.Popen([sys.executable, "-m", "simulator.load_test"])
    
    # Keep visual focus on the browser to record live WebSocket increments
    for _ in range(15):
        # Move mouse in small orbits to keep interaction active
        pyautogui.moveTo(width // 2 + 50, height // 2, duration=0.4)
        pyautogui.moveTo(width // 2, height // 2 + 50, duration=0.4)
        time.sleep(0.5)

    # ── Phase 3: Anomaly & AI RCA Presentation ───────────────────────────────
    print("Phase 3: Highlighting Diagnostic Reports...")
    # Hover over diagnostic table entries (approximate grid region coordinates)
    pyautogui.moveTo(width // 4, height * 3 // 4, duration=1.0)
    time.sleep(3)
    
    # ── Phase 4: Webhook Approvals ───────────────────────────────────────────
    print("Phase 4: Triggering Human-In-The-Loop Approval Gates...")
    # Hover over Pending Approval card actions and perform smooth click
    # (Coordinates target middle right sector area)
    pyautogui.moveTo(width * 3 // 4, height // 3, duration=1.0)
    # Perform clean click simulation
    pyautogui.click(width * 3 // 4, height // 3)
    time.sleep(5)  # Capture audit history log updates

    # ── Phase 5: Browser Cleanup ──────────────────────────────────────────────
    print("Phase 5: Automated Walkthrough Complete. Cleaning up sessions...")
    # Close window command shortcut
    if sys.platform == "darwin":
        pyautogui.hotkey("command", "w")
    else:
        pyautogui.hotkey("ctrl", "w")
    
    # Terminate load processes
    load_proc.terminate()
    load_proc.wait()


def main() -> None:
    """Configures recording file buffers, spawns capturing worker, and runs choreography."""
    global recording_active
    
    # Ensure assets folder exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Detect resolution
    width, height = pyautogui.size()
    
    # Setup OpenCV Video Writer
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(OUTPUT_FILE, fourcc, FPS, (width, height))
    
    print(f"Video recorder initialized targeting: {OUTPUT_FILE}")
    
    # Start thread
    recording_active = True
    record_thread = threading.Thread(target=record_loop, args=(width, height, writer), name="recorder-loop")
    record_thread.start()
    
    try:
        run_choreography()
    except Exception as exc:
        print(f"Error during choreography walk: {exc}")
    finally:
        # Halt recording thread
        recording_active = False
        record_thread.join()
        
        # Clean resources
        writer.release()
        print("Screen recorder handles closed successfully.")
        print(f" walk video file assembled at: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
