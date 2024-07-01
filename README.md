# cdf-kafka-consumer

Use local port forwarding:

```bash
ssh -A <username>@<ip address> -L 9092:localhost:9092 -NT
```

Install the requirements:

```bash
pip install -r requirements.txt
```

Run:

```bash
python consumer.py
```
