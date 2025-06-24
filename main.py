import logging
import os
import re
import asyncio
from functools import wraps

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, FSInputFile
from aiogram.utils.markdown import hbold, hcode
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

# Библиотеки для скачивания
from yandex_music import ClientAsync
import yt_dlp

# --- КОНФИГУРАЦИЯ ---
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
YM_TOKEN = os.getenv("YM_TOKEN")
REJECT_STICKER_ID = os.getenv("REJECT_STICKER_ID")
allowed_ids_str = os.getenv("ALLOWED_TG_IDS", "")
ALLOWED_IDS = [int(id.strip()) for id in allowed_ids_str.split(',')] if allowed_ids_str else []

if not all([BOT_TOKEN, YM_TOKEN, REJECT_STICKER_ID, ALLOWED_IDS]):
    logging.critical("КРИТИЧЕСКАЯ ОШИБКА: Отсутствуют переменные окружения в .env")
    exit()

# --- УПРАВЛЕНИЕ СОСТОЯНИЕМ ЗАДАЧ ---
USER_TASKS = {}  # {user_id: asyncio.Task}

# --- ИНИЦИАЛИЗАЦИЯ ---
dp = Dispatcher()
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
ym_client = ClientAsync(YM_TOKEN)

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def sanitize_filename(filename: str) -> str:
    """Удаляет из имени файла символы, запрещенные в Windows."""
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def cleanup_user_task(user_id: int):
    """Полная очистка данных о задаче пользователя."""
    USER_TASKS.pop(user_id, None)

# --- ДЕКОРАТОР ---
def access_control(handler):
    @wraps(handler)
    async def wrapper(message: Message, *args, **kwargs):
        if message.from_user.id not in ALLOWED_IDS:
            logging.warning(f"Unauthorized access denied for {message.from_user.id} ({message.from_user.username})")
            try:
                await message.answer_sticker(REJECT_STICKER_ID)
                await message.answer("Вам здесь не рады. Проход запрещен.")
            except TelegramBadRequest as e:
                logging.error(f"Could not send reject sticker. Maybe ID is wrong? Error: {e}")
                await message.answer("Access denied.")
            return
        return await handler(message, *args, **kwargs)
    return wrapper

# --- ХЭНДЛЕРЫ ---
@dp.message(CommandStart())
@access_control
async def command_start_handler(message: Message):
    await message.answer(
        f"Привет, хозяин {hbold(message.from_user.full_name)}!\n\n"
        "Отправь мне ссылку на трек, альбом, плейлист или видео.\n"
        "Или используй /favorites для скачивания 'Избранного'.\n"
        "Для отмены длинной загрузки используй /cancel."
    )
    await message.delete()

@dp.message(Command("cancel"))
@access_control
async def cancel_handler(message: Message):
    """Отменяет текущую задачу скачивания."""
    user_id = message.from_user.id
    task = USER_TASKS.get(user_id)

    if task and not task.done():
        task.cancel()
        await message.answer("✅ Задача по скачиванию отменена.")
    else:
        await message.answer("Нет активных задач для отмены.")
    await message.delete()

async def start_playlist_download(message: Message, track_ids: list, source_name: str):
    """Общая функция для запуска скачивания плейлиста/альбома."""
    user_id = message.from_user.id
    if USER_TASKS.get(user_id) and not USER_TASKS[user_id].done():
        await message.reply("Предыдущая задача еще не завершена. Для отмены используйте /cancel")
        return

    if not track_ids:
        await message.reply(f"Не найдено треков в '{source_name}'.")
        return

    progress_message = await message.answer(f"Подготовка к скачиванию {len(track_ids)} треков из '{source_name}'...")
    
    try:
        await bot.pin_chat_message(
            chat_id=user_id,
            message_id=progress_message.message_id,
            disable_notification=True
        )
    except Exception as e:
        logging.warning(f"Не удалось закрепить сообщение для пользователя {user_id}. Ошибка: {e}")

    task = asyncio.create_task(
        process_download_queue(user_id, track_ids, progress_message, source_name)
    )
    USER_TASKS[user_id] = task
    await message.delete()

@dp.message(Command("favorites"))
@access_control
async def download_favorites_handler(message: Message):
    """Скачивает плейлист 'Мне нравится'."""
    await message.reply("Получаю список ваших избранных треков...")
    try:
        liked_tracks = await ym_client.users_likes_tracks()
        track_ids = [track.id for track in liked_tracks.tracks]
        await start_playlist_download(message, track_ids, "Избранное")
    except Exception as e:
        logging.error(f"Error getting favorites: {e}", exc_info=True)
        await message.answer(f"🚫 Ошибка при получении 'Избранного': {e}")


@dp.message(F.text)
@access_control
async def main_downloader_handler(message: Message):
    """Главный обработчик ссылок."""
    user_id = message.from_user.id
    url_pattern = r'https?://[^\s]+'
    url_match = re.search(url_pattern, message.text)
    
    if not url_match:
        await message.reply("Пожалуйста, отправьте мне корректную ссылку.")
        return

    url = url_match.group(0)

    try:
        # Плейлист Я.Музыки
        playlist_match = re.search(r"users/([^/]+)/playlists/(\d+)", url)
        if playlist_match:
            username, playlist_id = playlist_match.groups()
            await message.reply("Получаю треки из плейлиста...")
            playlist = await ym_client.users_playlists_fetch_all(playlist_id, username)
            await start_playlist_download(message, [t.id for t in playlist.tracks], playlist.title)
            return

        # Альбом Я.Музыки
        album_match = re.search(r"album/(\d+)", url)
        if album_match and "/track/" not in url:
            album_id = album_match.group(1)
            await message.reply("Получаю треки из альбома...")
            album_with_tracks = await ym_client.albums_with_tracks(album_id)
            track_ids = []
            if album_with_tracks.volumes:
                for volume in album_with_tracks.volumes:
                    track_ids.extend([track.id for track in volume])
            await start_playlist_download(message, track_ids, album_with_tracks.title)
            return

        # Трек Я.Музыки
        track_match = re.search(r"album/(\d+)/track/(\d+)", url)
        if track_match:
            track_id = track_match.group(2)
            await download_single_track(track_id, user_id, message_to_delete=message)
            return

        # Другие сервисы
        if any(s in url for s in ["youtube.com", "youtu.be", "tiktok.com", "instagram.com"]):
            await download_with_yt_dlp(url, message)
            return

        await message.reply("Не могу распознать эту ссылку. Поддерживаются треки, альбомы, плейлисты Я.Музыки и видео с YouTube/TikTok/Instagram.")

    except Exception as e:
        logging.error(f"Error processing URL {url}: {e}", exc_info=True)
        await message.answer(f"🚫 Ошибка при обработке ссылки: {e}")

# --- ВОРКЕР И ОСНОВНЫЕ ФУНКЦИИ ---

async def process_download_queue(user_id: int, track_ids: list, progress_message: Message, source_name: str):
    """Фоновый обработчик очереди с индикатором прогресса."""
    total_tracks = len(track_ids)
    downloaded_count = 0
    DOWNLOAD_DELAY = 5

    try:
        for i, track_id in enumerate(track_ids):
            if i % 3 == 0 or i == total_tracks - 1:
                try:
                    await bot.edit_message_text(
                        chat_id=user_id,
                        message_id=progress_message.message_id,
                        text=f"Скачиваю '{source_name}'\n"
                             f"Прогресс: {downloaded_count}/{total_tracks}"
                    )
                except TelegramBadRequest as e:
                    if "message is not modified" not in str(e):
                        logging.warning(f"Could not edit progress message: {e}")

            try:
                await download_single_track(track_id, user_id)
                downloaded_count += 1
            except Exception as e:
                logging.error(f"Не удалось скачать track_id {track_id}: {e}", exc_info=True)
                await bot.send_message(user_id, f"🚫 Ошибка при скачивании трека с ID {track_id}. Пропускаю.")
            
            await asyncio.sleep(DOWNLOAD_DELAY)
        
        await bot.edit_message_text(
            chat_id=user_id,
            message_id=progress_message.message_id,
            text=f"✅ Скачивание '{source_name}' завершено!\n"
                 f"Загружено: {downloaded_count} из {total_tracks}."
        )

    except asyncio.CancelledError:
        await bot.edit_message_text(
            chat_id=user_id,
            message_id=progress_message.message_id,
            text=f"❌ Скачивание отменено пользователем.\n"
                 f"Успело загрузиться: {downloaded_count} из {total_tracks}."
        )
        logging.info(f"Task for user {user_id} was cancelled.")
        
    finally:
        try:
            await bot.unpin_chat_message(chat_id=user_id, message_id=progress_message.message_id)
        except Exception as e:
            logging.warning(f"Не удалось открепить сообщение для пользователя {user_id}. Ошибка: {e}")
        
        cleanup_user_task(user_id)

async def download_single_track(track_id: str, user_id: int, message_to_delete: Message = None):
    """Скачивает один трек и отправляет его."""
    try:
        track_info = (await ym_client.tracks([track_id]))[0]
        artist = track_info.artists_name()[0]
        title = track_info.title
        
        raw_filename = f"{artist} - {title}.mp3"
        filename = sanitize_filename(raw_filename)

        download_info_list = await track_info.get_download_info_async()
        if not download_info_list:
            raise ValueError("Не удалось получить информацию для скачивания.")

        sorted_downloads = sorted(download_info_list, key=lambda x: x.bitrate_in_kbps, reverse=True)
        best_download_info = sorted_downloads[0]
        best_bitrate = best_download_info.bitrate_in_kbps

        await best_download_info.download_async(filename)
        
        audio_file = FSInputFile(filename)
        await bot.send_audio(
            chat_id=user_id, audio=audio_file, caption=f"✅ Качество: {best_bitrate} kbps",
            performer=artist, title=title
        )
        os.remove(filename)

        if message_to_delete:
            await message_to_delete.delete()
    except Exception as e:
        if message_to_delete:
            await bot.send_message(user_id, f"Не удалось скачать трек из {message_to_delete.text}: {e}")
        raise e

async def download_with_yt_dlp(url: str, message: Message):
    """Скачивает видео с помощью yt-dlp."""
    output_template = sanitize_filename('%(title)s.%(ext)s')
    ydl_opts = {'outtmpl': output_template, 'format': 'best[ext=mp4][filesize<=50M]/best[filesize<=50M]', 'noplaylist': True}

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = await asyncio.to_thread(ydl.extract_info, url, download=True)
        
        filename = ydl.prepare_filename(info)
        video_file = FSInputFile(filename)
        await bot.send_video(
            chat_id=message.chat.id, video=video_file, 
            caption=f"✅ {info.get('title', 'Без названия')}",
            supports_streaming=True
        )
        os.remove(filename)
        await message.delete()

    except yt_dlp.utils.DownloadError as e:
        logging.warning(f"yt-dlp download error for {url}: {e}")
        await message.reply("🚫 Ошибка: Видео слишком большое ( >50 МБ), приватное или удалено.")
    except Exception as e:
        raise e

# --- ЗАПУСК БОТА ---
async def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    await ym_client.init()
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот остановлен вручную.")