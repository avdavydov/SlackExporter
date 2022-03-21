import logging
import ssl
import time
import warnings
from datetime import datetime

import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


def duration(finction):
    def wrapped(*args, **kwargs):
        start = time.time()
        result = finction(*args, **kwargs)
        print(f'Общая длительность выполнения функции, секунд: {int(time.time() - start)}')
        return result

    return wrapped


def loggingConfig(level):
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s',
                        datefmt='%d-%m-%Y %H:%M:%S',
                        level=logging.INFO)


def get_user_by_id(client, user_id):
    loggingConfig(logging.INFO)
    user_name = None
    try:
        result = client.users_info(user=user_id)
        user = result['user']
        real_name = user.get('real_name')
        profile = user.get('profile')
        display_name = profile.get('display_name')

        if display_name != '':
            user_name = display_name
        else:
            user_name = real_name

    except SlackApiError as e:
        logging.error("Error fetching user: {}".format(e))

    return user_name


def get_channel_name_by_id(client, channel):
    loggingConfig(logging.INFO)
    result = None
    try:
        conversations_info = client.conversations_info(channel=channel)

        channel = conversations_info['channel']
        user = channel.get('user')
        name = channel.get('name')
        name_normalized = channel.get('name_normalized')

        is_im = channel.get('is_im')
        is_group = channel.get('is_group')
        is_channel = channel.get('is_channel')

        if is_im is True:
            result = get_user_by_id(client=client, user_id=user)
        elif is_group is True:
            result = name_normalized
        elif is_channel is True:
            result = name

    except SlackApiError as e:
        logging.error("Error fetching conversations: {}".format(e))

    return result


def fetch_users(client, limit=1000, cursor=None):
    loggingConfig(logging.INFO)
    res = []
    try:
        logging.info(f'Fetch users cursor: {cursor}')
        result = client.users_list(limit=limit, cursor=cursor)
        next_cursor = result['response_metadata'].get('next_cursor')
        for user in result['members']:
            id = user.get('id')
            name = user.get('name')
            real_name = user.get('real_name')
            profile = user.get('profile')
            display_name = profile.get('display_name')
            phone = profile.get('phone')
            title = profile.get('title')
            res.append([id, name, real_name, display_name, phone, title])

    except SlackApiError as e:
        logging.error("Error fetching users: {}".format(e))

    return [next_cursor, res]


def get_all_users(client, limit=None):
    loggingConfig(logging.INFO)
    result = []
    next_cursor = None
    while next_cursor != '' or next_cursor is None:
        channels = fetch_users(client=client, cursor=next_cursor, limit=limit)
        next_cursor = channels[0]
        for res in channels[1]:
            result.append(res)
    df = pd.DataFrame(result, columns=['id', 'name', 'real_name', 'display_name', 'phone', 'title'])
    return df


def fetch_conversations(client, limit=1000, cursor=None, types=None):
    loggingConfig(logging.INFO)
    res = []
    try:
        logging.info(f'Fetch conversations cursor: {cursor}')
        result = client.conversations_list(limit=limit, cursor=cursor, types=types)
        next_cursor = result['response_metadata'].get('next_cursor')
        for channel in result['channels']:
            id = channel.get('id')
            name = channel.get('name')
            private = channel.get('is_private')

            is_im = channel.get('is_im')
            is_mpim = channel.get('is_mpim')
            is_group = channel.get('is_group')
            is_channel = channel.get('is_channel')
            user = channel.get('user')

            if is_mpim is True:
                type = 'mpim'
            elif is_channel is True:
                type = 'channel'
            elif is_group is True:
                type = 'group'
            elif is_im is True:
                type = 'im'
            else:
                type = 'other'
            res.append([id, name, type, private, user])

    except SlackApiError as e:
        logging.error("Error fetching conversations: {}".format(e))

    return [next_cursor, res]


def get_all_channels(client, types, limit=None):
    loggingConfig(logging.INFO)
    result = []
    next_cursor = None
    while next_cursor != '' or next_cursor is None:
        channels = fetch_conversations(client=client, cursor=next_cursor, types=types, limit=limit)
        next_cursor = channels[0]
        for res in channels[1]:
            result.append(res)
    df = pd.DataFrame(result, columns=['id', 'name', 'type', 'private', 'user'])
    return df


def fetch_conversations_replies(client, channel, ts, cursor=None, limit=1000):
    loggingConfig(logging.INFO)
    res = []
    try:
        logging.info(f'Fetch conversations replies cursor: {cursor}')
        result = client.conversations_replies(channel=channel, ts=ts, cursor=cursor, limit=limit)
        try:
            next_cursor = result['response_metadata'].get('next_cursor')
        except:
            next_cursor = ''
        for messages in result['messages']:
            type = messages.get('type')
            user = messages.get('user')
            user = get_user_by_id(client=client, user_id=user)
            ts = messages.get('ts')
            ts_float = float(ts)
            ts = datetime.utcfromtimestamp(ts_float)
            ts = ts.strftime("%d:%m:%Y %H:%M:%S")
            text = messages.get('text')
            res.append([type, user, ts, text, ts_float])
    except SlackApiError as e:
        logging.error("Error fetching conversations replies: {}".format(e))
    return [next_cursor, res]


def get_all_replies(client, channel, ts, limit=1000):
    loggingConfig(logging.INFO)
    result = []
    next_cursor = None

    while next_cursor != '' or next_cursor is None:
        replies = fetch_conversations_replies(client=client, channel=channel, ts=ts, cursor=next_cursor, limit=limit)
        next_cursor = replies[0]
        for res in replies[1]:
            result.append(res)
    result = sorted(result, key=lambda x: x[4])
    return result


def fetch_conversations_history(client, channel, cursor=None, limit=1000):
    loggingConfig(logging.INFO)
    res = []

    try:
        logging.info(f'Fetch conversations cursor: {cursor}')
        result = client.conversations_history(channel=channel, cursor=cursor, limit=limit)

        try:
            next_cursor = result['response_metadata'].get('next_cursor')
        except:
            next_cursor = ''

        for messages in result['messages']:
            type = messages.get('type')
            user = messages.get('user')
            ts = messages.get('ts')
            ts_float = float(ts)
            ts = datetime.utcfromtimestamp(ts_float)
            ts = ts.strftime("%d:%m:%Y %H:%M:%S")
            text = messages.get('text')
            thread_ts = messages.get('thread_ts')

            if not thread_ts:
                res.append([ts_float, channel, user, ts, type, text, None, None, None, None])
            else:
                res.append([ts_float, channel, user, ts, type, text, None, None, None, None])
                replies = get_all_replies(client=client, channel=channel, ts=thread_ts, limit=limit)
                for reply in replies:
                    r_type, r_user, r_ts, r_text, _ = reply
                    res.append([ts_float, channel, user, ts, type, None, r_user, r_ts, r_text, r_type])

    except SlackApiError as e:
        logging.error("Error fetching conversations history: {}".format(e))

    return [next_cursor, res]


def get_all_conversations_history(client, channel, limit=1000):
    loggingConfig(logging.INFO)
    result = []
    next_cursor = None

    try:
        channel_name = get_channel_name_by_id(client=client, channel=channel)
    except SlackApiError as e:
        logging.error("Error creating conversation: {}".format(e))

    while next_cursor != '' or next_cursor is None:
        messages = fetch_conversations_history(client=client, channel=channel, cursor=next_cursor, limit=limit)
        next_cursor = messages[0]
        for res in messages[1]:
            res.insert(0, channel_name)
            result.append(res)

    result = sorted(result, key=lambda x: x[1])
    df = pd.DataFrame(result,
                      columns=['channel_name', 'ts_float', 'channel', 'user', 'ts', 'type', 'text', 'r_user', 'r_ts',
                               'reply', 'r_type'])
    return df


def get_all_conversations_history_for_all_users(client, types, limit=None):
    loggingConfig(logging.INFO)
    cannels = get_all_channels(client=client, types=types)
    result = []

    for index, row in cannels.iterrows():

        try:
            channel_name = get_channel_name_by_id(client=client, channel=row['id'])
        except SlackApiError as e:
            logging.error("Error creating conversation: {}".format(e))

        logging.info(f'''Get conversations history for cannel ID {row['id']}: {channel_name}''')
        next_cursor = None
        while next_cursor != '' or next_cursor is None:
            messages = fetch_conversations_history(client=client, channel=row['id'], cursor=next_cursor, limit=limit)
            next_cursor = messages[0]
            for res in messages[1]:
                res.insert(0, channel_name)
                result.append(res)
    result = sorted(result, key=lambda x: x[1])
    df = pd.DataFrame(result,
                      columns=['channel_name', 'ts_float', 'channel', 'user', 'ts', 'type', 'text', 'r_user', 'r_ts',
                               'reply', 'r_type'])
    return df


def saveToExcel(df, file_name) -> None:
    loggingConfig(logging.INFO)
    logging.info('Выгрузка отчёта')
    writer = pd.ExcelWriter(file_name, engine='xlsxwriter', options={'strings_to_urls': False})
    df.to_excel(writer, index=False)
    writer.save()
    logging.info(f'Отчёт сформирован. Размер отчёта: {df.shape}')
    logging.info(f'Результат выгружен в файл: {file_name}')


@duration
def main():
    token = '********'

    warnings.filterwarnings("ignore")
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    client = WebClient(token=token, ssl=ssl_context)

    # Экспорт данных по всем каналам
    users = get_all_users(client=client)
    types = 'public_channel, private_channel, mpim, im'
    channels = get_all_channels(client=client, types=types)
    channels_result = pd.merge(channels, users, left_on='user', right_on='id', how='left')
    channels_result = channels_result.drop('id_y', 1)
    file_name = '~/Downloads/Slack_channels.xlsx'
    saveToExcel(df=channels_result, file_name=file_name)

    # Экспорт всей истории переписок со всеми пользователями (каналы и их историю не экспортируем)
    types = 'im, mpim'
    limit = 200
    messages = get_all_conversations_history_for_all_users(client=client, types=types, limit=limit)
    messages_result = pd.merge(messages, users, left_on='user', right_on='id', how='left')
    messages_result = messages_result.drop('id', 1)
    messages_result = messages_result.drop('phone', 1)
    messages_result = messages_result.drop('title', 1)
    messages_result = messages_result.drop('ts_float', 1)
    messages_result = messages_result.reindex(
        columns=['channel_name', 'ts', 'display_name', 'text', 'r_user', 'r_ts', 'reply', 'type', 'r_type', 'real_name',
                 'name', 'user', 'channel'])
    file_name = '~/Downloads/Slack_messages.xlsx'
    saveToExcel(df=messages_result, file_name=file_name)


if __name__ == '__main__':
    main()
