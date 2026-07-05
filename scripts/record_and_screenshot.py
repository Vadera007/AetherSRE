"""
AetherSRE — Automated Screen Recorder & Screenshot Capture Agent
========================================================================
Records the AetherSRE dashboard operations, captures high-res screenshots
at key milestone phases, and saves them to docs/assets and the artifacts folder.
"""

from __future__ import annotations

import os
import sys
import time
import shutil
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

# Paths and configuration
FPS: Final[int] = 20
FRAME_INTERVAL: Final[float] = 1.0 / FPS
OUTPUT_DIR: Final[str] = "docs/assets"
VIDEO_FILE: Final[str] = os.path.join(OUTPUT_DIR, "demo_walkthrough.mp4")
ARTIFACTS_DIR: Final[str] = "/Users/akshatvadera/.gemini/antigravity/brain/cc05040b-d986-42a3-b409-ea50f22dcf3a"

# Global stop signal for recording thread
recording_active = False


def record_loop(width: int, height: int, writer: cv2.VideoWriter) -> None:
    """Asynchronous loop capturing screenshot frames at constant FPS."""
    global recording_active
    next_frame_time = time.monotonic()
    
    while recording_active:
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


def take_screenshot(name: str) -> None:
    """Helper to capture a screenshot and save it to both docs/assets and the artifacts folder."""
    filename = f"{name}.png"
    assets_path = os.path.join(OUTPUT_DIR, filename)
    artifacts_path = os.path.join(ARTIFACTS_DIR, filename)
    
    print(f"📷 Capturing screenshot: {filename}...")
    img = pyautogui.screenshot()
    img.save(assets_path)
    
    # Copy to artifacts directory
    try:
        shutil.copy(assets_path, artifacts_path)
        print(f"   Saved to {assets_path} and copied to artifacts.")
    except Exception as exc:
        print(f"   Warning: Failed to copy to artifacts: {exc}")


def run_choreography() -> None:
    """Executes the pre-planned UI walking actions and takes screenshots."""
    width, height = pyautogui.size()
    print(f"Screen resolution detected: {width}x{height}")
    
    # ── Phase 1: Browser Launch ──────────────────────────────────────────────
    print("Phase 1: Booting Browser and Navigating Dashboard...")
    dashboard_url = "http://localhost:8000/dashboard"
    webbrowser.open(dashboard_url)
    time.sleep(6)  # Wait for page load and rendering
    
    # Focus browser window (click neutral background spot)
    pyautogui.click(width // 2, 80)
    time.sleep(2)
    
    # Take initial state screenshot
    take_screenshot("dashboard_initial")
    
    # ── Phase 2: Triggering Load Ingestion ───────────────────────────────────
    print("Phase 2: Launching High-Throughput Load Test Rig...")
    load_proc = subprocess.Popen([sys.executable, "-m", "simulator.load_test"])
    
    # Keep visual focus on the browser to record live WebSocket increments
    for _ in range(12):
        # Move mouse in small orbits to keep interaction active
        pyautogui.moveTo(width // 2 + 50, height // 2, duration=0.4)
        pyautogui.moveTo(width // 2, height // 2 + 50, duration=0.4)
        time.sleep(0.5)

    take_screenshot("dashboard_active_traffic")

    # ── Phase 3: Anomaly & AI RCA Presentation ───────────────────────────────
    print("Phase 3: Highlighting Diagnostic Reports...")
    # Hover over diagnostic table entries (approximate grid region coordinates)
    pyautogui.moveTo(width // 4, height * 3 // 4, duration=1.0)
    time.sleep(3)
    
    take_screenshot("dashboard_rca_diagnostic")
    
    # ── Phase 4: Webhook Approvals ───────────────────────────────────────────
    print("Phase 4: Triggering Human-In-The-Loop Approval Gates...")
    # Hover over Pending Approval card actions and perform smooth click
    pyautogui.moveTo(width * 3 // 4, height // 3, duration=1.0)
    pyautogui.click(width * 3 // 4, height // 3)
    time.sleep(4)  # Capture audit history log updates
    
    take_screenshot("dashboard_remediation_success")
    time.sleep(1)

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
    writer = cv2.VideoWriter(VIDEO_FILE, fourcc, FPS, (width, height))
    
    print(f"Video recorder initialized targeting: {VIDEO_FILE}")
    
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
        print(f"Walk video file assembled at: {VIDEO_FILE}")


if __name__ == "__main__":
    main()
