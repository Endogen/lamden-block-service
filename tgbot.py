import html
import json
import traceback

from telegram import ParseMode, Update
from telegram.error import Unauthorized, InvalidToken
from telegram.ext import Updater, CallbackContext
from config import Config
from loguru import logger


class TelegramBot:
    cfg = None

    def __init__(self, config: Config):
        self.cfg = config

        read_timeout = self.cfg.get("tg_read_timeout")
        connect_timeout = self.cfg.get("tg_connect_timeout")
        con_pool_size = self.cfg.get("tg_con_pool_size")

        self.tgb_kwargs = dict()

        if read_timeout:
            self.tgb_kwargs["read_timeout"] = read_timeout
        if connect_timeout:
            self.tgb_kwargs["connect_timeout"] = connect_timeout
        if con_pool_size:
            self.tgb_kwargs["con_pool_size"] = con_pool_size

        try:
            logger.info("Connecting bot...")
            self.updater = Updater(self.cfg.get("tg_token"), request_kwargs=self.tgb_kwargs)
            logger.info("Checking bot token...")
            self.updater.bot.get_me()
        except (InvalidToken, Unauthorized) as e:
            logger.exception(f"Bot token not valid: {e}")
            self.send('Telegram bot token not valid')
            exit()

        self.job_queue = self.updater.job_queue
        self.dispatcher = self.updater.dispatcher

        self.dispatcher.add_error_handler(self._handle_tg_errors)

    def _handle_tg_errors(self, update: Update, context: CallbackContext):
        logger.error(msg="Exception while handling an update:", exc_info=context.error)
        tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
        tb_string = ''.join(tb_list)

        if not update:
            logger.error(f'<pre>{html.escape(tb_string)}</pre>')
            return

        message = (
            f'An exception was raised while handling an update\n'
            f'<pre>update = {html.escape(json.dumps(update.to_dict(), indent=2, ensure_ascii=False))}'
            f'</pre>\n\n'
            f'<pre>context.chat_data = {html.escape(str(context.chat_data))}</pre>\n\n'
            f'<pre>context.user_data = {html.escape(str(context.user_data))}</pre>\n\n'
            f'<pre>{html.escape(tb_string)}</pre>'
        )

        context.bot.send_message(
            chat_id=self.cfg.get("admin_id"),
            text=message,
            parse_mode=ParseMode.HTML)

        if update.message:
            update.message.reply_text(
                text=f"{context.error}",
                parse_mode=ParseMode.MARKDOWN)
        elif update.callback_query:
            update.callback_query.message.reply_text(
                text=f"{context.error}",
                parse_mode=ParseMode.MARKDOWN)

    def send(self, msg: str, to: int = None, mode: ParseMode = ParseMode.HTML):
        to = to if to else self.cfg.get('telegram_notify')
        if not to: return

        try:
            self.updater.bot.send_message(to, msg, parse_mode=mode)
        except Exception as e:
            logger.exception(f"Could not send Telegram message: {e}")
