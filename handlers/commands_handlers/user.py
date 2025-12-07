import random
import uuid

from pyrogram import Client
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup

import core.utils.messages as config_messages
from core.utils.helpers import format_file_size
from core.cache.config import CacheTTLConfig
from core.utils.logger import get_logger
from core.utils.validators import private_only
from handlers.commands_handlers.base import BaseCommandHandler
from handlers.decorators import require_subscription, check_ban

logger = get_logger(__name__)


class UserCommandHandler(BaseCommandHandler):
    """Handler for user commands"""

    @check_ban()
    async def start_command(self, client: Client, message: Message):
        """Handle /start command with subscription check for deeplinks"""
        user_id = message.from_user.id if message.from_user else None
        if not user_id:
            return

        # Ensure user exists in database
        if not await self.bot.user_repo.is_user_exist(user_id):
            await self.bot.user_repo.create_user(
                user_id,
                message.from_user.first_name or "User"
            )

            # Log new user
            if self.bot.config.LOG_CHANNEL:
                try:
                    await client.send_message(
                        self.bot.config.LOG_CHANNEL,
                        f"#NewUser\n"
                        f"ID: <code>{user_id}</code>\n"
                        f"Name: {message.from_user.mention}"
                    )
                except Exception as e:
                    logger.error(f"Failed to log new user: {e}")

        # Handle deep link
        if len(message.command) > 1:
            # Check subscription for deeplinks (except for admins and auth users)
            skip_sub_check = (
                    user_id in self.bot.config.ADMINS or
                    user_id in getattr(self.bot.config, 'AUTH_USERS', [])
            )

            # Check if auth channel/groups are configured and user needs to subscribe
            if not skip_sub_check and (self.bot.config.AUTH_CHANNEL or getattr(self.bot.config, 'AUTH_GROUPS', [])):
                is_subscribed = await self.bot.subscription_manager.is_subscribed(
                    client, user_id
                )

                if not is_subscribed:
                    # Send subscription message with the deeplink parameter
                    await self._send_subscription_message_for_deeplink(
                        client, message, message.command[1]
                    )
                    return

            # User is subscribed or doesn't need subscription, handle deeplink
            from handlers.deeplink import DeepLinkHandler
            deeplink_handler = DeepLinkHandler(self.bot)
            # Remove the decorator from handle_deep_link since we're checking here
            await deeplink_handler.handle_deep_link_internal(client, message, message.command[1])
            return

        # Send welcome message with simplified buttons
        buttons = [
            [
                InlineKeyboardButton(
                    "➕ Add me to Group",
                    url=f"https://t.me/{self.bot.bot_username}?startgroup=true"
                )
            ],
            [
                InlineKeyboardButton("📚 Help", callback_data="help"),
                InlineKeyboardButton("ℹ️ About", callback_data="about")
            ]
        ]

        if self.bot.config.SUPPORT_GROUP_URL and self.bot.config.SUPPORT_GROUP_NAME:
            buttons.append([
                InlineKeyboardButton(
                    f"💬 {self.bot.config.SUPPORT_GROUP_NAME}",
                    url=self.bot.config.SUPPORT_GROUP_URL
                )
            ])

        custom_start_message = None
        if self.bot.config.START_MESSAGE:
            custom_start_message = self.bot.config.START_MESSAGE

        if custom_start_message:
            # Format with available placeholders
            welcome_text = custom_start_message.format(
                mention=message.from_user.mention,
                user_id=user_id,
                first_name=message.from_user.first_name or "User",
                bot_name=self.bot.bot_name,
                bot_username=self.bot.bot_username
            )
        else:
            # Use default message
            mention = message.from_user.mention
            welcome_text = config_messages.START_MSG.format(mention=mention)

        if self.bot.config.PICS:
            await message.reply_photo(
                photo=random.choice(self.bot.config.PICS),
                caption=welcome_text,
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        else:
            await message.reply_text(
                welcome_text,
                reply_markup=InlineKeyboardMarkup(buttons)
            )

    async def _send_subscription_message_for_deeplink(
            self, client: Client, message: Message, deeplink_param: str
    ):
        """Send subscription message for deeplink access"""
        from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

        user_id = message.from_user.id

        # Store the deeplink parameter in cache with a short key
        session_key = f"deeplink_{user_id}_{uuid.uuid4().hex[:8]}"
        await self.bot.cache.set(
            session_key,
            {'deeplink': deeplink_param, 'user_id': user_id},
            expire=CacheTTLConfig.USER_DATA
        )

        # Build buttons for required subscriptions
        buttons = []

        # AUTH_CHANNEL button
        if self.bot.config.AUTH_CHANNEL:
            try:
                chat_link = await self.bot.subscription_manager.get_chat_link(
                    client, self.bot.config.AUTH_CHANNEL
                )
                chat = await client.get_chat(self.bot.config.AUTH_CHANNEL)
                channel_name = chat.title or "Updates Channel"

                buttons.append([
                    InlineKeyboardButton(
                        f"📢 Join {channel_name}",
                        url=chat_link
                    )
                ])
            except Exception as e:
                logger.error(f"Error creating AUTH_CHANNEL button: {e}")

        # AUTH_GROUPS buttons
        if hasattr(self.bot.config, 'AUTH_GROUPS') and self.bot.config.AUTH_GROUPS:
            for group_id in self.bot.config.AUTH_GROUPS:
                try:
                    chat_link = await self.bot.subscription_manager.get_chat_link(
                        client, group_id
                    )
                    chat = await client.get_chat(group_id)
                    group_name = chat.title or "Required Group"

                    buttons.append([
                        InlineKeyboardButton(
                            f"👥 Join {group_name}",
                            url=chat_link
                        )
                    ])
                except Exception as e:
                    logger.error(f"Error creating AUTH_GROUP button for {group_id}: {e}")

        # Add "Try Again" button with the short session key
        buttons.append([
            InlineKeyboardButton(
                "🔄 Try Again",
                callback_data=f"checksub#dl#{session_key}"  # Use short key instead
            )
        ])

        message_text = (
            "🔒 <b>Subscription Required</b>\n\n"
            "You need to join our channel(s) to access this content.\n"
            "Please join the required channel(s) and try again."
        )

        await message.reply_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            disable_web_page_preview=True
        )

    @check_ban()
    @require_subscription()
    async def help_command(self, client: Client, message: Message):
        """Handle /help command"""
        help_text =config_messages.HELP_MSG.format(bot_username=self.bot.bot_username)

        if message.from_user and message.from_user.id in self.bot.config.ADMINS:
            help_text += (
                "\n<b>Admin Commands:</b>\n"
                "• /users - Total users count\n"
                "• /broadcast - Broadcast message\n"
                "• /ban <user_id> - Ban user\n"
                "• /unban <user_id> - Unban user\n"
                "• /addpremium <user_id> - Add premium\n"
                "• /removepremium <user_id> - Remove premium\n"
                "• /setskip <number> - Set indexing skip\n"
                "• /performance - View bot performance metrics\n"
                "\n<b>Channel Management:</b>\n"
                "• /add_channel <id> - Add channel for indexing\n"
                "• /remove_channel <id> - Remove channel\n"
                "• /list_channels - List all channels\n"
                "• /toggle_channel <id> - Enable/disable channel\n"
            )

        await message.reply_text(help_text)

    @check_ban()
    @require_subscription()
    async def about_command(self, client: Client, message: Message):
        """Handle /about command"""
        about_text = config_messages.ABOUT_MSG.format(bot_username=self.bot.bot_username,bot_name=self.bot.bot_name)
        await message.reply_text(about_text)

    @check_ban()
    @require_subscription()
    async def stats_command(self, client: Client, message: Message):
        """Handle stats command"""
        # Get comprehensive stats
        try:
            stats = await self.bot.maintenance_service.get_system_stats()
        except Exception as e:
            logger.error(f"Error getting system stats: {e}")
            await message.reply_text("❌ Error retrieving statistics. Please try again later.")
            return

        # Format stats message
        text = (
            f"📊 <b>Bot Statistics</b>\n\n"
            f"<b>👥 Users:</b>\n"
            f"├ Total: {stats['users']['total']:,}\n"
            f"├ Premium: {stats['users']['premium']:,}\n"
            f"├ Banned: {stats['users']['banned']:,}\n"
            f"└ Active Today: {stats['users']['active_today']:,}\n\n"
            f"<b>📁 Files:</b>\n"
            f"├ Total: {stats['files']['total_files']:,}\n"
            f"└ Size: {format_file_size(stats['files']['total_size'])}\n\n"
            f"<b>💾 Database Storage:</b>\n"
            f"├ Total: {format_file_size(stats.get('storage', {}).get('total_size', 0))}\n"
            f"├ Data: {format_file_size(stats.get('storage', {}).get('database_size', 0))}\n"
            f"├ Indexes: {format_file_size(stats.get('storage', {}).get('index_size', 0))}\n"
            f"└ Objects: {stats.get('storage', {}).get('objects_count', 0):,}\n"
        )

        # Add file type breakdown
        if stats['files']['by_type']:
            text += "\n<b>📊 By Type:</b>\n"
            for file_type, data in stats['files']['by_type'].items():
                text += f"├ {file_type.title()}: {data['count']:,} ({format_file_size(data['size'])})\n"

        # Add collection breakdown (top 3 by size)
        if stats.get('storage', {}).get('collections'):
            collections = stats['storage']['collections']
            # Sort by storage size and show top 3
            sorted_collections = sorted(
                collections.items(), 
                key=lambda x: x[1]['storage_size'], 
                reverse=True
            )[:3]
            
            if sorted_collections:
                text += "\n<b>🗂 Top Collections:</b>\n"
                for i, (coll_name, coll_data) in enumerate(sorted_collections):
                    display_name = coll_name.replace('_', ' ').title()
                    symbol = "└" if i == len(sorted_collections) - 1 else "├"
                    text += f"{symbol} {display_name}: {format_file_size(coll_data['storage_size'])}\n"

        await message.reply_text(text)

    @check_ban()
    @private_only
    @require_subscription()
    async def plans_command(self, client: Client, message: Message):
        """Handle plans command"""
        if self.bot.config.DISABLE_PREMIUM:
            await message.reply_text("✅ Premium features are disabled. Enjoy unlimited access!")
            return

        user_id = message.from_user.id
        user = await self.bot.user_repo.get_user(user_id)

        # Build plans message
        text = (
            "💎 <b>Premium Plans</b>\n\n"
            f"🎯 <b>Free Plan:</b>\n"
            f"├ {self.bot.config.NON_PREMIUM_DAILY_LIMIT} files per day\n"
            f"├ Basic search features\n"
            f"└ Standard support\n\n"
            f"⭐ <b>Premium Plan:</b> <b>{self.bot.config.PREMIUM_PRICE}</b>\n"
            f"├ Unlimited file access\n"
            f"├ Priority support\n"
            f"├ Advanced features\n"
            f"└ Duration: {self.bot.config.PREMIUM_DURATION_DAYS} days\n\n"
        )

        # Add current status
        if user:
            if user.is_premium:
                is_active, status_msg = await self.bot.user_repo.check_and_update_premium_status(user)
                text += f"✅ <b>Your Status:</b> {status_msg}\n"
            else:
                remaining = self.bot.config.NON_PREMIUM_DAILY_LIMIT - user.daily_retrieval_count
                text += f"📊 <b>Your Status:</b> Free Plan\n"
                text += f"📁 Today's Usage: {user.daily_retrieval_count}/{self.bot.config.NON_PREMIUM_DAILY_LIMIT}\n"
                text += f"📁 Remaining: {remaining}\n"

        buttons = [[
            InlineKeyboardButton("💳 Get Premium", url=self.bot.config.PAYMENT_LINK)
        ]]

        await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))

    @check_ban()
    @require_subscription()
    async def request_stats_command(self, client: Client, message: Message):
        """Show user's request statistics"""
        user_id = message.from_user.id
        stats = await self.bot.user_repo.get_request_stats(user_id)

        if not stats['exists']:
            await message.reply_text(
                "❌ No request data found. Make your first request using #request in the support group!")
            return

        # Build stats message
        text = (
            "📊 <b>Your Request Statistics</b>\n\n"
            f"📅 <b>Today's Requests:</b> {stats['daily_requests']}/{stats['daily_limit']}\n"
            f"📁 <b>Remaining Today:</b> {stats['daily_remaining']}\n"
            f"⚠️ <b>Warnings:</b> {stats['warning_count']}/{stats['warning_limit']}\n"
            f"📈 <b>Total Requests:</b> {stats['total_requests']}\n"
        )

        if stats['is_at_limit']:
            text += "\n⚠️ <b>Status:</b> Daily limit reached! Further requests will result in warnings."
        elif stats['is_warned']:
            text += f"\n⚠️ <b>Status:</b> You have {stats['warnings_remaining']} warnings remaining before ban."
        else:
            text += "\n✅ <b>Status:</b> You can make requests normally."

        if stats['warning_reset_in_days'] is not None:
            text += f"\n\n⏱ <b>Warning Reset:</b> {stats['warning_reset_in_days']} days"

        if stats['last_request_date']:
            text += f"\n📅 <b>Last Request:</b> {stats['last_request_date']}"

        await message.reply_text(text)
