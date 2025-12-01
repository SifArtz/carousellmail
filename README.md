# carousellmail

Telegram-бот для проверки доставляемости email-адресов продавцов Carousell.

## Быстрый старт

1. Установите переменную окружения с токеном бота:

   ```bash
   export BOT_TOKEN=<your_bot_token>
   ```

2. Запустите бота:

   ```bash
   python karousell.py
   ```

Бот принимает `.txt` файлы в JSON или текстовом формате Carousell, извлекает продавцов, проверяет доступность их Gmail и сохраняет новые валидные адреса в `valid_emails.txt`.
