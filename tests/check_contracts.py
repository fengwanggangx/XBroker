"""Read-only check of XBroker/XEngine wire schemas and generated messages.

Internal session interfaces, trade models and request helpers have different
responsibilities and are intentionally not compared byte for byte.
"""
from pathlib import Path
import sys


def main():
    root = Path(__file__).resolve().parents[2]
    broker_root = root / "XBroker" / "XBroker" / "request"
    engine_root = root / "XEngine" / "XEngine" / "request"
    names = (
        "request.proto", "request.pb.h", "request.pb.cc",
        "v1/market.proto", "v1/market.pb.h", "v1/market.pb.cc",
    )
    errors = []
    for name in names:
        broker = broker_root / name
        engine = engine_root / name
        missing = [str(path) for path in (broker, engine) if not path.is_file()]
        if missing:
            errors.append("Missing contract file: " + ", ".join(missing))
        elif broker.read_bytes() != engine.read_bytes():
            errors.append(f"Wire contract differs: {broker} <> {engine}")
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"XBroker/XEngine wire contracts match ({len(names)} files)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
