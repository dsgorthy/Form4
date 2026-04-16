#!/bin/bash
# Run after catchup plists fire to unload them. Schedule at 9:40 ET.
launchctl unload ~/Library/LaunchAgents/com.openclaw.catchup-qm.plist 2>/dev/null
launchctl unload ~/Library/LaunchAgents/com.openclaw.catchup-10b5.plist 2>/dev/null
rm -f ~/Library/LaunchAgents/com.openclaw.catchup-qm.plist
rm -f ~/Library/LaunchAgents/com.openclaw.catchup-10b5.plist
launchctl unload ~/Library/LaunchAgents/com.openclaw.catchup-cleanup.plist 2>/dev/null
rm -f ~/Library/LaunchAgents/com.openclaw.catchup-cleanup.plist
echo "$(date): Catchup plists cleaned up" >> /Users/openclaw/trading-framework/logs/catchup-cleanup.log
