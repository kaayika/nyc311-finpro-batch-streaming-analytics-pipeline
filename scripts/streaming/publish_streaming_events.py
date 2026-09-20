import argparse
import json
import time
from pathlib import Path

from google.cloud import pubsub_v1


PROJECT_ID = "jcdeah-009"
TOPIC_ID = "nyc311-streaming-topic"

DATA_DIR = Path("data/stream/sample")


def main():

    # ========================================
    # ARGUMENTS
    # ========================================

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--delay",
        type=float,
        default=0.05,
        help="Delay between messages in seconds",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of messages to publish",
    )

    args = parser.parse_args()


    # ========================================
    # PUB/SUB CLIENT
    # ========================================

    publisher = pubsub_v1.PublisherClient()

    topic_path = publisher.topic_path(
        PROJECT_ID,
        TOPIC_ID,
    )


    # ========================================
    # GET STREAMING SAMPLE FILES
    # ========================================

    files = sorted(
        DATA_DIR.glob("nyc311_2026_04_*.jsonl")
    )

    if not files:
        raise FileNotFoundError(
            "Streaming sample files not found."
        )


    # ========================================
    # START PUBLISHER
    # ========================================

    print("=" * 80)
    print("NYC 311 STREAMING PUBLISHER")
    print("=" * 80)

    print(f"Project : {PROJECT_ID}")
    print(f"Topic   : {TOPIC_ID}")
    print(f"Files   : {len(files)}")
    print(f"Delay   : {args.delay} seconds")

    if args.limit is not None:
        print(f"Limit   : {args.limit} messages")
    else:
        print("Limit   : ALL messages")


    published_count = 0


    # ========================================
    # READ FILES
    # ========================================

    for file_path in files:

        print(f"\nReading : {file_path}")

        with file_path.open(
            "r",
            encoding="utf-8",
        ) as file:

            for line in file:

                # ========================================
                # CHECK MESSAGE LIMIT
                # ========================================

                if (
                    args.limit is not None
                    and published_count >= args.limit
                ):
                    break


                # ========================================
                # PREPARE JSON MESSAGE
                # ========================================

                message = json.loads(line)

                message_data = json.dumps(
                    message,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")


                # ========================================
                # PUBLISH MESSAGE
                # ========================================

                future = publisher.publish(
                    topic_path,
                    data=message_data,
                )

                message_id = future.result()

                published_count += 1

                print(
                    f"Published {published_count:,} "
                    f"| Message ID : {message_id}"
                )


                # ========================================
                # SIMULATED REAL-TIME DELAY
                # ========================================

                if args.delay > 0:
                    time.sleep(args.delay)


        # Stop reading other files if limit reached
        if (
            args.limit is not None
            and published_count >= args.limit
        ):
            break


    # ========================================
    # FINAL STATUS
    # ========================================

    print("\n" + "=" * 80)
    print("STREAMING PUBLISH COMPLETED")
    print("=" * 80)

    print(
        f"Total published : "
        f"{published_count:,}"
    )


if __name__ == "__main__":
    main()
