import logging
import re
import smtplib
import dns.resolver
import json
from pathlib import Path
from typing import List, Dict, Optional
from aiogram import Bot, Dispatcher, types
from aiogram.types import ContentType, ParseMode
import asyncio
import aiofiles

# --- Конфигурация ---
COMMON_EMAIL_DOMAINS = ['gmail.com']
MAX_CONCURRENT_REQUESTS = 10
VALID_EMAILS_FILE = "valid_emails.txt"

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
        """Загрузка кэшированных адресов"""
        if Path(VALID_EMAILS_FILE).exists():
            with open(VALID_EMAILS_FILE, 'r') as f:
                return set(line.strip() for line in f.readlines())
        return set()

    async def save_valid_emails(self):
        """Сохраняем новые валидные email-ы после завершения обработки"""
        if not self.new_valid_emails:
            return
        async with aiofiles.open(VALID_EMAILS_FILE, 'a') as f:
            for email in self.new_valid_emails:
                await f.write(email + '\n')
        logger.info(f"💾 Saved {len(self.new_valid_emails)} new valid emails to file.")
        self.new_valid_emails.clear()

    async def verify(self, email: str) -> bool:
        """Асинхронная верификация email"""
        if email in self.valid_emails:
            return True
        async with self.semaphore:
            result = await asyncio.to_thread(self.check_gmail_exists, email)
            if result:
                self.valid_emails.add(email)
                self.new_valid_emails.add(email)
            return result

    def check_gmail_exists(self, email: str) -> bool:
        """Проверяет deliverability Gmail через SMTP"""
        if not re.match(r"[^@]+@gmail\.com$", email):
            return False
        try:
            username, domain = email.split('@')
            records = dns.resolver.resolve(domain, 'MX')
            mx_record = str(records[0].exchange)

            with smtplib.SMTP(mx_record, timeout=5) as server:
                server.helo()
                server.mail('me@example.com')
                code, _ = server.rcpt(email)

            return code == 250
        except Exception:
            return False


# --- Парсинг Carousell ---
class FileParser:
    @staticmethod
    async def read_async(path: Path) -> str:
        async with aiofiles.open(path, 'r', encoding='utf-8', errors='ignore') as f:
            return await f.read()

    @staticmethod
    def parse(file_content: str) -> (List[Dict[str, str]], str):
        """Определяет формат файла и парсит данные"""
        file_content = file_content.strip().replace("\ufeff", "")
        try:
            data = json.loads(file_content)
            listings = FileParser.parse_carousell_json(data)
            if listings:
                return listings, "Atom Parser"
        except json.JSONDecodeError:
            pass

        listings = FileParser.parse_carousell_blocks(file_content)
        return listings, "G6 Parser"

    @staticmethod
    def parse_carousell_blocks(file_content: str) -> List[Dict[str, str]]:
        """Парсит старый текстовый формат Carousell"""
        product_blocks = re.split(r"🔸CAROUSELL", file_content)
        listings = []

        for block in product_blocks:
            block = block.strip()
            if not block:
                continue

            photo = FileParser.extract_value(r"(https://media\.karousell\.com[^\s]+)", block)
            title = FileParser.extract_value(r"🗂 Товар:\s*(.+)", block)
            price = FileParser.extract_value(r"💵 Цена:\s*(.+)", block)
            link = FileParser.extract_value(r"\[🔗 Ссылка на товар\]\((https?://[^\)]+)\)", block)
            seller = FileParser.extract_value(r"👤 Продавец:\s*([A-Za-z0-9_.-]+)", block)

            if all([photo, title, price, link, seller]) and FileParser.is_valid_seller(seller):
                listings.append({
                    "photo": photo.strip(),
                    "title": title.strip(),
                    "price": price.strip(),
                    "link": link.strip(),
                    "seller": seller.strip(),
                })
        return listings

    @staticmethod
    def parse_carousell_json(data: dict) -> List[Dict[str, str]]:
        """Парсит JSON-формат (Atom Parser)"""
        listings = []
        for item in data.values():
            seller = item.get("seller")
            if not FileParser.is_valid_seller(seller):
                continue
            listings.append({
                "photo": item.get("img_url"),
                "title": item.get("title"),
                "price": item.get("price"),
                "link": item.get("adLink"),
                "seller": seller.strip()
            })
        return listings

    @staticmethod
    def extract_value(pattern: str, text: str) -> Optional[str]:
        match = re.search(pattern, text)
        return match.group(1).strip() if match else None

    @staticmethod
    def is_valid_seller(seller: str) -> bool:
        return bool(seller) and re.fullmatch(r'[A-Za-z0-9_.-]{4,30}', seller)

    @staticmethod
    def generate_email(username: str) -> str:
        return f"{username}@gmail.com"


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
            await message.reply(f"📂 Received file: <b>{document.file_name}</b>\n⏳ Processing...", parse_mode=ParseMode.HTML)
            file = await self.bot.get_file(document.file_id)
            await file.download(destination=temp_path)

            content = await FileParser.read_async(temp_path)
            listings, parser_name = FileParser.parse(content)

            if not listings:
                await message.answer(f"⚠️ No valid listings found in <b>{document.file_name}</b>", parse_mode=ParseMode.HTML)
                return

            await message.answer(f"✅ File <b>{document.file_name}</b> detected as <b>{parser_name}</b>", parse_mode=ParseMode.HTML)

            results = await self.verify_all(listings)
            await self.send_results(message, document.file_name, results)

        except Exception as e:
            logger.error(f"Error processing file {document.file_name}: {e}")
            await message.reply("⚠️ Error while processing file.")
        finally:
            if temp_path.exists():
                temp_path.unlink()

    async def verify_all(self, listings: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Параллельная проверка всех email"""
        tasks = []
        for item in listings:
            email = FileParser.generate_email(item['seller'])
            already_verified = email in self.verifier.valid_emails
            tasks.append((email, already_verified, self.verifier.verify(email)))

        # выполняем все проверки параллельно
        results = await asyncio.gather(*[t[2] for t in tasks])

        return [
            {
                "listing": listings[i],
                "email": tasks[i][0],
                "already_verified": tasks[i][1],
                "valid": results[i]
            }
            for i in range(len(listings))
        ]

    async def send_results(self, msg: types.Message, filename: str, results: List[Dict[str, str]]):
        """Отправка результатов пользователю"""
        valid_new = [r for r in results if r["valid"] and not r["already_verified"]]
        valid_cached = [r for r in results if r["valid"] and r["already_verified"]]
        invalid = [r for r in results if not r["valid"]]

        for r in valid_new[:20]:  # максимум 20 новых результатов
            listing = r["listing"]
            email = r["email"]
            formatted = (
                f"📧 <b>Email:</b> {email}\n"
                f"📬 <b>Status:</b> ✅ deliverable (new)\n"
                f"🔍 <b>Title:</b> <code>{listing['title']}</code>\n"
                f"💰 <b>Price:</b> <code>{listing['price']}</code>\n"
                f"🔗 <a href=\"{listing['link']}\">Link</a>"
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

    async def run(self):
        logger.info("🤖 Bot started polling...")
        await self.dp.start_polling()


# --- Запуск ---
async def main():
    bot_token = "8571120569:AAF6KuSWjq5sUR9VxBDWAPmP8GiLetwOR8o"  # <-- вставь сюда свой токен
    bot = TelegramBot(bot_token)
    await bot.run()


if __name__ == '__main__':
    asyncio.run(main())