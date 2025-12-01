import asyncio
import json
import logging
import os
import re
import smtplib
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import aiofiles
import dns.resolver
from aiogram import Bot, Dispatcher, types
from aiogram.types import ContentType, ParseMode

# --- Конфигурация ---
COMMON_EMAIL_DOMAINS = ["gmail.com"]
MAX_CONCURRENT_REQUESTS = 10
VALID_EMAILS_FILE = "valid_emails.txt"
SMTP_TIMEOUT_SECONDS = 5

# --- Логирование ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("carousell_bot")


# --- Проверка Email ---
class EmailVerifier:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.valid_emails = self.load_valid_emails()
        self.new_valid_emails = set()
        logger.info(f"✅ EmailVerifier initialized. {len(self.valid_emails)} cached emails loaded.")

    def load_valid_emails(self) -> set:
        """Загрузка кэшированных адресов."""
        path = Path(VALID_EMAILS_FILE)
        if path.exists():
            with path.open("r", encoding="utf-8") as file:
                return {line.strip() for line in file if line.strip()}
        return set()

    async def save_valid_emails(self):
        """Сохраняет новые валидные email после завершения обработки."""
        if not self.new_valid_emails:
            return
        async with aiofiles.open(VALID_EMAILS_FILE, 'a') as f:
            for email in self.new_valid_emails:
                await f.write(email + '\n')
        logger.info(f"💾 Saved {len(self.new_valid_emails)} new valid emails to file.")
        self.new_valid_emails.clear()

    async def verify(self, email: str) -> bool:
        """Асинхронная верификация email с кэшированием."""
        normalized = email.strip().lower()
        if normalized in self.valid_emails:
            return True
        async with self.semaphore:
            result = await asyncio.to_thread(self.check_mailbox_exists, normalized)
            if result:
                self.valid_emails.add(normalized)
                self.new_valid_emails.add(normalized)
            return result

    def check_mailbox_exists(self, email: str) -> bool:
        """Проверяет deliverability через SMTP для поддерживаемых доменов."""
        if not self.is_supported_domain(email):
            return False
        try:
            _, domain = email.split('@', 1)
            records = dns.resolver.resolve(domain, 'MX')
            mx_record = str(records[0].exchange)

            with smtplib.SMTP(mx_record, timeout=SMTP_TIMEOUT_SECONDS) as server:
                server.helo()
                server.mail('me@example.com')
                code, _ = server.rcpt(email)

            return code == 250
        except Exception as exc:  # noqa: BLE001 - логируем и возвращаем False
            logger.debug(f"SMTP check failed for {email}: {exc}")
            return False

    @staticmethod
    def is_supported_domain(email: str) -> bool:
        return any(email.endswith(f"@{domain}") for domain in COMMON_EMAIL_DOMAINS)


# --- Парсинг Carousell ---
@dataclass(frozen=True)
class Listing:
    photo: str
    title: str
    price: str
    link: str
    seller: str

    @property
    def email(self) -> str:
        return f"{self.seller.lower()}@{COMMON_EMAIL_DOMAINS[0]}"


class FileParser:
    @staticmethod
    async def read_async(path: Path) -> str:
        async with aiofiles.open(path, 'r', encoding='utf-8', errors='ignore') as f:
            return await f.read()

    @staticmethod
    def parse(file_content: str) -> Tuple[List[Listing], str]:
        """Определяет формат файла и парсит данные."""
        file_content = file_content.strip().replace("\ufeff", "")
        try:
            data = json.loads(file_content)
            listings = FileParser.parse_carousell_json(data)
            if listings:
                return FileParser.deduplicate_listings(listings), "Atom Parser"
        except json.JSONDecodeError:
            pass

        listings = FileParser.parse_carousell_blocks(file_content)
        return FileParser.deduplicate_listings(listings), "G6 Parser"

    @staticmethod
    def parse_carousell_blocks(file_content: str) -> List[Listing]:
        """Парсит старый текстовый формат Carousell."""
        product_blocks = re.split(r"🔸CAROUSELL", file_content)
        listings: List[Listing] = []

        for block in product_blocks:
            sanitized = block.strip()
            if not sanitized:
                continue

            listing = FileParser._build_listing_from_block(sanitized)
            if listing:
                listings.append(listing)
        return listings

    @staticmethod
    def parse_carousell_json(data: dict) -> List[Listing]:
        """Парсит JSON-формат (Atom Parser)."""
        listings: List[Listing] = []
        for item in data.values():
            seller = item.get("seller")
            if not FileParser.is_valid_seller(seller):
                continue
            photo = item.get("img_url")
            link = item.get("adLink")
            title = item.get("title")
            price = item.get("price")
            if not all([photo, link, title, price]):
                continue
            listings.append(
                Listing(
                    photo=photo.strip(),
                    title=title.strip(),
                    price=str(price).strip(),
                    link=link.strip(),
                    seller=seller.strip(),
                )
            )
        return listings

    @staticmethod
    def deduplicate_listings(listings: Iterable[Listing]) -> List[Listing]:
        """Убирает дубликаты по продавцу и ссылке."""
        seen = set()
        unique: List[Listing] = []
        for listing in listings:
            key = (listing.seller.lower(), listing.link)
            if key in seen:
                continue
            seen.add(key)
            unique.append(listing)
        return unique

    @staticmethod
    def _build_listing_from_block(block: str) -> Optional[Listing]:
        photo = FileParser.extract_value(r"(https://media\.karousell\.com[^\s]+)", block)
        title = FileParser.extract_value(r"🗂 Товар:\s*(.+)", block)
        price = FileParser.extract_value(r"💵 Цена:\s*(.+)", block)
        link = FileParser.extract_value(r"\[🔗 Ссылка на товар\]\((https?://[^\)]+)\)", block)
        seller = FileParser.extract_value(r"👤 Продавец:\s*([A-Za-z0-9_.-]+)", block)

        if all([photo, title, price, link, seller]) and FileParser.is_valid_seller(seller):
            return Listing(
                photo=photo.strip(),
                title=title.strip(),
                price=price.strip(),
                link=link.strip(),
                seller=seller.strip(),
            )
        return None

    @staticmethod
    def extract_value(pattern: str, text: str) -> Optional[str]:
        match = re.search(pattern, text)
        return match.group(1).strip() if match else None

    @staticmethod
    def is_valid_seller(seller: Optional[str]) -> bool:
        return bool(seller) and bool(re.fullmatch(r"[A-Za-z0-9_.-]{4,30}", seller))


# --- Telegram Bot ---
class TelegramBot:
    def __init__(self, bot_token: str):
        self.bot = Bot(token=bot_token)
        self.dp = Dispatcher(self.bot)
        self.verifier = EmailVerifier()

        self.dp.register_message_handler(self.cmd_start, commands=['start'])
        self.dp.register_message_handler(self.handle_document, content_types=ContentType.DOCUMENT)

    async def cmd_start(self, message: types.Message):
        await message.reply(
            "📥 Send one or more .txt files from Carousell.\n\nSupports:\n🧩 Atom Parser (JSON)\n📜 G6 Parser (text)"
        )

    async def handle_document(self, message: types.Message):
        """Обработка загруженных файлов"""
        document = message.document
        temp_path = Path(f"temp_{document.file_id}.txt")
        try:
            await message.reply(
                f"📂 Received file: <b>{document.file_name}</b>\n⏳ Processing...",
                parse_mode=ParseMode.HTML,
            )
            file = await self.bot.get_file(document.file_id)
            await file.download(destination=temp_path)

            content = await FileParser.read_async(temp_path)
            listings, parser_name = FileParser.parse(content)

            if not listings:
                await message.answer(
                    f"⚠️ No valid listings found in <b>{document.file_name}</b>",
                    parse_mode=ParseMode.HTML,
                )
                return

            await message.answer(
                f"✅ File <b>{document.file_name}</b> detected as <b>{parser_name}</b>",
                parse_mode=ParseMode.HTML,
            )

            results = await self.verify_all(listings)
            await self.send_results(message, document.file_name, results)

        except Exception as e:
            logger.error(f"Error processing file {document.file_name}: {e}")
            await message.reply("⚠️ Error while processing file.")
        finally:
            if temp_path.exists():
                temp_path.unlink()

    async def verify_all(self, listings: Iterable[Listing]) -> List[Dict[str, object]]:
        """Параллельная проверка всех email."""
        tasks = []
        normalized_listings = list(listings)
        for listing in normalized_listings:
            email = listing.email
            already_verified = email in self.verifier.valid_emails
            tasks.append((email, already_verified, self.verifier.verify(email)))

        results = await asyncio.gather(*(task[2] for task in tasks))

        return [
            {
                "listing": normalized_listings[i],
                "email": tasks[i][0],
                "already_verified": tasks[i][1],
                "valid": results[i],
            }
            for i in range(len(normalized_listings))
        ]

    async def send_results(self, msg: types.Message, filename: str, results: List[Dict[str, object]]):
        """Отправка результатов пользователю."""
        valid_new = [r for r in results if r["valid"] and not r["already_verified"]]
        valid_cached = [r for r in results if r["valid"] and r["already_verified"]]
        invalid = [r for r in results if not r["valid"]]

        for r in valid_new:
            listing: Listing = r["listing"]
            email = r["email"]
            formatted = (
                f"📧 <b>Email:</b> {email}\n"
                f"📬 <b>Status:</b> ✅ deliverable (new)\n"
                f"🔍 <b>Title:</b> <code>{listing.title}</code>\n"
                f"💰 <b>Price:</b> <code>{listing.price}</code>\n"
                f"🔗 <a href=\"{listing.link}\">Link</a>"
            )
            await msg.answer(formatted, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

        summary = (
            f"📊 <b>{filename}</b>\n\n"
            f"Total sellers: <b>{len(results)}</b>\n"
            f"Deliverable (new): <b>{len(valid_new)}</b>\n"
            f"Deliverable (cached): <b>{len(valid_cached)}</b>\n"
            f"Undeliverable: <b>{len(invalid)}</b>"
        )

        await msg.answer(summary, parse_mode=ParseMode.HTML)
        await self.verifier.save_valid_emails()

        await self.send_export_file(msg, filename, valid_new + valid_cached)

    async def send_export_file(
        self, msg: types.Message, filename: str, valid_results: List[Dict[str, object]]
    ):
        """Формирует и отправляет файл с валидными email и заголовками товаров."""
        if not valid_results:
            await msg.answer("📄 Нет валидных email для экспорта.")
            return

        lines = ["email | title"]
        for result in valid_results:
            listing: Listing = result["listing"]
            lines.append(f"{result['email']} | {listing.title}")

        temp_file = None
        try:
            temp_file = tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=False, suffix=".txt")
            temp_file.write("\n".join(lines))
            temp_file.flush()
            temp_path = Path(temp_file.name)

            await msg.answer_document(
                types.InputFile(temp_path),
                caption=f"📄 Валидные адреса из {filename}",
            )
        except Exception:
            logger.exception("Failed to send export file to user")
            await msg.answer("⚠️ Не удалось отправить файл с валидными адресами.")
        finally:
            if temp_file:
                temp_file.close()
                Path(temp_file.name).unlink(missing_ok=True)

    async def run(self):
        logger.info("🤖 Bot started polling...")
        await self.dp.start_polling()


# --- Запуск ---
async def main():
    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise RuntimeError("BOT_TOKEN environment variable is not set")

    bot = TelegramBot(bot_token)
    await bot.run()


if __name__ == '__main__':
    asyncio.run(main())
