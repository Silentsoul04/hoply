set term png small size 1280,1024
set output "hoply-memory-graph.png"

set y2label "%MEM"
set ylabel "VSZ"


set ytics nomirror
set y2tics nomirror in

set yrange [0:*]
set y2range [0:*]

plot "log" using 2 with lines axes x1y2 title "%MEM", \
     "log" using 3 with lines axes x1y1 title "VSZ"
