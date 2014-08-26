#!/bin/sh
# sample script to start and stop create_compressed_fs daemons on a set of
# compressing nodes
echo "Testing hosts..."
case "$1" in
    start)
    cmd="create_compressed_fs -l </dev/null >/dev/null 2>/dev/null &"
    ;;
    stop)
    cmd="killall create_compressed_fs"
    ;;
    *)
    echo "Syntax: $0 start|stop host host host ..."
    exit 1;
    ;;
esac

shift

for node in "$@"; do
    echo Running: ssh -x -q -t $node "$cmd"
    ssh -x -q -t $node "$cmd"
done

