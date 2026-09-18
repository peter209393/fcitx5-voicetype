#!/usr/bin/env python3
"""Exercise the addon without a GUI: creates an input context through fcitx5's
DBus frontend, holds the hotkey for N seconds (speak into the microphone!),
releases it, and prints the preedit/commit signals fcitx5 sends back.

    python3 scripts/ictest.py [seconds=4] [keysym=Alt_R]
"""
import sys, time
import gi
gi.require_version("Gio", "2.0")
from gi.repository import Gio, GLib

hold = float(sys.argv[1]) if len(sys.argv) > 1 else 4.0
keysym = sys.argv[2] if len(sys.argv) > 2 else "Alt_R"
KEYSYMS = {"Alt_R": (0xFFEA, 108), "Alt_L": (0xFFE9, 64), "F13": (0xFFC0, 191)}
keyval, keycode = KEYSYMS[keysym]

bus = Gio.bus_get_sync(Gio.BusType.SESSION, None)
IM, IC = "org.fcitx.Fcitx.InputMethod1", "org.fcitx.Fcitx.InputContext1"

def call(path, iface, method, params=None):
    return bus.call_sync("org.fcitx.Fcitx5", path, iface, method, params, None,
                         Gio.DBusCallFlags.NONE, 5000, None).unpack()

# A private display name gives the test its own focus group, so the real
# focused window does not steal focus back.
ic = call("/org/freedesktop/portal/inputmethod", IM, "CreateInputContext",
          GLib.Variant("(a(ss))", [[("program", "ictest"), ("display", "ictest:0")]]))[0]
bus.signal_subscribe("org.fcitx.Fcitx5", IC, None, ic, None, Gio.DBusSignalFlags.NONE,
                     lambda *a: print(time.strftime("%H:%M:%S"), a[4], a[5].unpack()))
call(ic, IC, "SetCapability", GLib.Variant("(t)", [2]))  # CapabilityFlag::Preedit
call(ic, IC, "FocusIn")

loop = GLib.MainLoop()
def key(release):
    handled = call(ic, IC, "ProcessKeyEvent",
                   GLib.Variant("(uuubu)", [keyval, keycode, 1 << 3 if release else 0, release, 0]))[0]
    print(time.strftime("%H:%M:%S"), "release" if release else "press", "handled =", handled)
    return handled

def press():
    if not key(False):
        print("hotkey not handled: check ~/.config/fcitx5/conf/voicetype.conf and fcitx5's log")
        loop.quit()
        return
    print(f"recording for {hold:.0f}s, speak now…")
    GLib.timeout_add(int(hold * 1000), lambda: (key(True), GLib.timeout_add(10000, loop.quit)) and False)

GLib.timeout_add(300, press)
loop.run()
call(ic, IC, "DestroyIC")
