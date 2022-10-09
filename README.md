# Latency

Records produced: 3000 per sec, 500 keys
Latency introduced by producer: 1ms at 99p
Latency introduced by consumer: 3ms at 99p (buffering with hopping window, taking latest 10 by key)
