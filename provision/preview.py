#!/usr/bin/env python3
"""Preview OOBE screens on the device framebuffer.

Usage:
    sudo python3 provision/preview.py welcome
    sudo python3 provision/preview.py connect_phone
    sudo python3 provision/preview.py phone_connected
    sudo python3 provision/preview.py wifi_connecting
    sudo python3 provision/preview.py wifi_connected
    sudo python3 provision/preview.py cms_connecting
    sudo python3 provision/preview.py cms_pending
    sudo python3 provision/preview.py adopted
    sudo python3 provision/preview.py list          # show all screen names
"""

import sys
import time
import threading

sys.path.insert(0, "/opt/agora/src")
from provision.display import ProvisionDisplay


SCREENS = {
    "welcome": "Welcome screen with spinner",
    "connect_phone": "Step 1: Connect your phone to AP",
    "phone_connected": "Step 2: Phone connected",
    "wifi_connecting": "Step 3: Connecting to Wi-Fi",
    "wifi_connected": "Wi-Fi connected",
    "wifi_failed": "Wi-Fi failed",
    "cms_connecting": "Step 4: Contacting server",
    "cms_pending": "CMS connected, pending adoption",
    "adopted": "Step 5: Adopted",
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] == "list":
        print("Available screens:")
        for name, desc in SCREENS.items():
            print(f"  {name:20s} — {desc}")
        return

    screen = sys.argv[1]
    if screen not in SCREENS:
        print(f"Unknown screen: {screen}")
        print(f"Available: {', '.join(SCREENS.keys())}")
        sys.exit(1)

    d = ProvisionDisplay()
    if not d.available:
        print("Framebuffer not available")
        sys.exit(1)

    ssid = "Agora-TEST"

    if screen == "welcome":
        d.show_welcome(frame=5)
    elif screen == "welcome_anim":
        stop = threading.Event()
        t = threading.Thread(target=d.animate_welcome, kwargs={"stop_event": stop})
        t.start()
        try:
            time.sleep(300)
        except KeyboardInterrupt:
            pass
        stop.set()
        t.join()
    elif screen == "connect_phone":
        d.show_connect_phone(ssid, frame=5)
    elif screen == "connect_phone_anim":
        stop = threading.Event()
        t = threading.Thread(target=d.animate_connect_phone, args=(ssid,), kwargs={"stop_event": stop})
        t.start()
        try:
            time.sleep(300)
        except KeyboardInterrupt:
            pass
        stop.set()
        t.join()
    elif screen == "phone_connected":
        d.show_phone_connected()
    elif screen == "wifi_connecting":
        d.animate_spinner(
            step="Step 3 of 5", title="Connecting to Wi-Fi",
            detail="MyNetwork", subtitle="Attempt 1 of 3...\nPlease wait.",
            progress=3, stop_event=threading.Event(), duration=300,
        )
    elif screen == "wifi_connected":
        d.show_wifi_connected("MyNetwork")
    elif screen == "wifi_failed":
        d.show_wifi_failed("MyNetwork", "Connection timed out")
    elif screen == "cms_connecting":
        d.animate_spinner(
            step="Step 4 of 5", title="Contacting Server",
            detail="agora-cms.local", detail_font="Monospace 32",
            subtitle="Verifying connection...", progress=4,
            stop_event=threading.Event(), duration=300, y_offset=100,
        )
    elif screen == "cms_pending":
        d.show_cms_connected_pending("agora-cms.local")
    elif screen == "adopted":
        d.show_adopted()

    print(f"Showing: {screen} — press Ctrl+C to exit")
    try:
        time.sleep(60)
    except KeyboardInterrupt:
        pass
    d.close()


if __name__ == "__main__":
    main()
