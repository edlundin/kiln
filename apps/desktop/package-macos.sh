#!/bin/sh
set -eu

cd "$(dirname "$0")/../.."
cargo build -p kiln-desktop --bin kiln-desktop
bundle="target/debug/Kiln.app"
mkdir -p "$bundle/Contents/MacOS"
cp apps/desktop/Info.plist "$bundle/Contents/Info.plist"
cp target/debug/kiln-desktop "$bundle/Contents/MacOS/kiln-desktop"
printf '%s\n' "Built $bundle"
