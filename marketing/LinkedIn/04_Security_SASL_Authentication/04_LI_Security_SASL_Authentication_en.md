### "It's our internal network, nobody can reach :9092." — said every team before the demo-day surprise.

SASL/SCRAM is the difference between "Kafka is a service" and "Kafka is a hallway with no door." Today's WKafka Day 04 treats authentication as a typed, one-time declaration — not a ritual you copy between services.

**The typed contract that replaces the auth snippet:**

```python
from wkafka import WKafka

kafka = WKafka(
    bootstrap_servers="localhost:30092",
    security_protocol="SASL_PLAINTEXT",      # or SASL_SSL
    sasl_mechanism="SCRAM-SHA-256",          # PLAIN / SCRAM-SHA-512 also
    sasl_plain_username="external-user",
    sasl_plain_password=os.getenv("KAFKA_PASSWORD"),   # credentials live in env, never in code
)
```

**Why this deletes a whole class of incidents:**
- Password never lands in a committed file — it is injected via env and resolved once at construction.
- A typo in the mechanism (`SCRAM-SHA-25` → validation error) fails at build time, not at 2 am after a rebatch.
- The consumer decorator stays 100% Day-01 — auth is transport, not a leak of handler logic.

**Verified (from the real repo, `04_security_sasl`):**
- SASL_PLAINTEXT & SASL_SSL; mechanisms PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
- `04_security_sasl` is a runnable end-to-end SASL auth cycle — reproducible
- Python 3.9 → 3.14, mypy strict, loguru, tox multi-version, MIT
- 14+ examples, LTS API, no invented metrics — only the reproducible code

**Resources:**
- PyPI: https://pypi.org/project/wkafka
- GitHub with `04_security_sasl` runnable: https://github.com/wisrovi/wkafka
- Docs: https://wkafka.readthedocs.io

*If you've ever shipped a Kafka password in a committed config, this is the sixth-safe space to admit it. We don't judge — we fix the wrapper.*

#Kafka #SASL #Security #SCRAM #Python #DataEngineering #OpenSource #Wisrovi