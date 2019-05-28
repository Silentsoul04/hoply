while true; do
    ps -C $1 -o pid=,%mem=,vsz=
    sleep 0.5
done
