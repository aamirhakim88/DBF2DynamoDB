#!/usr/bin/env bash
DISK_ID=$(hdiutil attach -nomount ram://2097152)
diskutil erasevolume HFS+ "ramdisk" ${DISK_ID}
cp SEBANET.DBF /Volumes/ramdisk/.
time python3 dbfdynamo.py
hdiutil detach ${DISK_ID}
