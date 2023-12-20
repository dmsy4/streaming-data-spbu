import asyncio
import pandas as pd
from telethon.sync import TelegramClient
from telethon.tl.types import MessageMediaDocument
from datetime import datetime, timezone
import pyarrow.parquet as pq
import pyarrow as pa
import argparse
import os
from dotenv import load_dotenv


load_dotenv()
api_id = os.environ.get('api_id')
api_hash = os.environ.get('api_hash')
phone_number = os.environ.get('phone_number')

channel_usernames = [
    'https://t.me/rtvimain',
    'https://t.me/lentachold',
    'https://t.me/ru2ch',
    'https://t.me/Wylsared',
    'https://t.me/rian_ru',
    'https://t.me/readovkanews',
    'https://t.me/exploitex'
]

async def collect_messages(api_id, api_hash, phone_number, channel_usernames, run_time, sleep_time):
    client = TelegramClient('session_name', api_id, api_hash)

    await client.connect()

    if not await client.is_user_authorized():
        await client.send_code_request(phone_number)
        await client.sign_in(phone_number, input('Enter the code: '))

    last_message_ids = {channel_username: 0 for channel_username in channel_usernames}

    start_time = datetime.now(timezone.utc)
    # set up starting date for collecting messages
    start_collect_date_utc = datetime(2023, 12, 20, 12, 0, 0)

    try:
        last_collecting_time = start_time
        while (datetime.now(timezone.utc) - start_time).total_seconds() < run_time:
            channels_data = []
            for channel_username in channel_usernames:
                channel = await client.get_entity(channel_username)

                print(f"Channel {channel_username}, min_id={last_message_ids[channel_username]}")
                messages = client.iter_messages(
                    channel,
                    min_id=last_message_ids[channel_username],
                    limit=20,
                    offset_date=start_collect_date_utc,
                    reverse=True
                )

                data, last_message_id = await process_messages(messages, channel_username)
                channels_data += data
                if len(data) > 0:
                    last_message_ids[channel_username] = last_message_id

            if len(channels_data) > 0:
                now_time = datetime.now(timezone.utc)
                df = pd.DataFrame(channels_data)
                df['Time'] = df['Time'].astype('datetime64[s]')  # because spark error otherwise
                first_timestamp = f'{str(last_collecting_time).replace(" ", "_").replace(".", "_").replace(":", "_")}'
                last_timestamp = f'{str(now_time).replace(" ", "_").replace(".", "_").replace(":", "_")}'
                filename = f'parquets/{first_timestamp}--{last_timestamp}'
                save_to_parquet(df, filename)
                last_collecting_time = now_time

            await asyncio.sleep(sleep_time)

    except Exception as e:
        print(f"Error: {e}")

    finally:
        await client.disconnect()

async def process_messages(messages, channel_username):
    data = []
    last_message_id = 0
    async for message in messages:
        time = message.date.replace(tzinfo=None)
        print(time)
        text = message.text
        has_media = True if message.media else False
        print(f"Channel {channel_username}, id={message.id}")
        data.append({'Time': time, 'Source': channel_username, 'Text': text, 'Has_Media': has_media, 'Message_ID': message.id})
        last_message_id = max(last_message_id, message.id)

    return data, last_message_id

def save_to_parquet(df, filename):
    filename = filename + ".parquet"

    pq.write_table(pa.Table.from_pandas(df), filename)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--run-time',
        type=float,
        default=float("inf"),
        required=False,
        help="The amount of time to run the script in seconds"
    )
    parser.add_argument(
        '--sleep-time',
        type=float,
        default=30,
        required=False,
        help="Period in seconds between collecting messages"
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        collect_messages(
            api_id, api_hash, phone_number, channel_usernames, args.run_time, args.sleep_time
            ))
