"""
Simple echo Telegram Bot example on Aiogram framework using
Yandex.Cloud functions.
"""
import json
import logging
import os

from create_bot import dp
from aiogram import Bot, Dispatcher, types
from handler import commands_start, start_test, answer_menager, explanation_menager


# Logger initialization and logging level setting
log = logging.getLogger(__name__)
log.setLevel(os.environ.get('LOGGING_LEVEL').upper())

# Functions for Yandex.Cloud
async def register_handlers(dp: Dispatcher):
    """Registration all handlers before processing update."""
    dp.register_message_handler(commands_start, commands=['start'])
    dp.register_callback_query_handler(start_test, text_startswith=['Начать тест','Далее','Назад','Билет', 'Избранное'])
    dp.register_callback_query_handler(answer_menager,text_startswith=['right_answer', 'false_answer_', 'Сердце'])
    dp.register_callback_query_handler(explanation_menager, text_startswith='Объяснение')
    log.debug('Handlers are registered.')

async def process_event(event, dp: Dispatcher):
    """
    Converting an Yandex.Cloud functions event to an update and
    handling tha update.
    """
    update = json.loads(event['body'])
    log.debug('Update: ' + str(update))

    Bot.set_current(dp.bot)
    update = types.Update.to_object(update)
    await dp.process_update(update)


async def handler(event, context):
    """Yandex.Cloud functions handler."""

    if event['httpMethod'] == 'POST':
        # Bot and dispatcher initialization
        await register_handlers(dp)
        await process_event(event, dp)

        return {'statusCode': 200, 'body': 'ok'}
    return {'statusCode': 405}
