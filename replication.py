import asyncio
from nats.aio.client import Client as NATS
from nats.js import api
from nats.errors import TimeoutError

NATS_URL1 = "nats://nats-1:4222"
NATS_URL2 = "nats://nats-2:4222"
STREAM_NAME = "my_stream"
DURABLE_CONSUMER_NAME = "my_durable_consumer"
DELIVER_SUBJECT = "deliver_subject"

async def main():
    # Connect to NATS servers
    nc1 = NATS()
    await nc1.connect(servers=[NATS_URL1])
    print(f"Connected to NATS server: {NATS_URL1}")

    nc2 = NATS()
    await nc2.connect(servers=[NATS_URL2])
    print(f"Connected to NATS server: {NATS_URL2}")

    # Initialize JetStream context
    js1 = nc1.jetstream()
    js2 = nc2.jetstream()

    # Remove consumer on NATS-1 if it exists
    async def remove_consumer():
        try:
            await js1.consumer_info(STREAM_NAME, DURABLE_CONSUMER_NAME)
            await js1.delete_consumer(STREAM_NAME, DURABLE_CONSUMER_NAME)
            print(f"Removed existing consumer: {DURABLE_CONSUMER_NAME}")
        except TimeoutError as e:
            print(f"Timeout when checking consumer: {e}")
        except Exception as e:
            print(f"Consumer not found or already removed: {e}")

    # Ensure NATS-2 stream is empty or reset
    async def reset_nats2_stream():
        try:
            stream_info = await js2.stream_info(STREAM_NAME)
            print(f"NATS-2 stream {STREAM_NAME} exists. Purging messages...")
            await js2.purge_stream(STREAM_NAME)
        except TimeoutError as e:
            print(f"Timeout when checking NATS-2 stream: {e}")
        except Exception as e:
            print(f"Stream not found on NATS-2, creating new one: {e}")
            await js2.add_stream(name=STREAM_NAME, subjects=["my_stream.>"])

    async def create_or_update_consumer():
        try:
            # Check if consumer exists
            consumer_info = await js1.consumer_info(STREAM_NAME, DURABLE_CONSUMER_NAME)
            deliver_subject = consumer_info.config.deliver_subject or DELIVER_SUBJECT
            print(f"Consumer {DURABLE_CONSUMER_NAME} exists with config: {consumer_info.config}")
            return deliver_subject
        except TimeoutError as e:
            print(f"Timeout when checking consumer: {e}")
        except Exception as e:
            # If consumer doesn't exist, create a new one
            print(f"Consumer not found, creating new one: {e}")
            deliver_subject = DELIVER_SUBJECT
            await js1.add_consumer(STREAM_NAME, config=api.ConsumerConfig(
                durable_name=DURABLE_CONSUMER_NAME,
                ack_policy=api.AckPolicy.EXPLICIT,
                deliver_policy=api.DeliverPolicy.ALL,
                replay_policy=api.ReplayPolicy.INSTANT,
                deliver_subject=deliver_subject,
            ))
            return deliver_subject

    async def message_handler(msg):
        print(f"Received message from {NATS_URL1}: {msg.data.decode()}")
        # Publish the message to the target subject on NATS-2
        await nc2.publish("my_stream.>", msg.data)
        # Acknowledge the message
        await msg.ack()

    # Remove consumer on NATS-1
    await remove_consumer()

    # Reset NATS-2 stream to ensure no duplicates
    await reset_nats2_stream()

    deliver_subject = await create_or_update_consumer()

    # Subscribe to messages on NATS-1
    await nc1.subscribe(subject=deliver_subject, cb=message_handler)

    # Keep the script running
    while True:
        await asyncio.sleep(1)

if __name__ == '__main__':
    asyncio.run(main())
