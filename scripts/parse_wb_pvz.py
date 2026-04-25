#!/usr/bin/env python3
"""
Скрипт обновления локального кэша ПВЗ Wildberries (pvz-data.js).

Запуск из корня репозитория:
    python scripts/parse_wb_pvz.py

Что делает:
1. Скачивает актуальный список ПВЗ Wildberries с публичного эндпоинта WB.
2. Фильтрует по стране (по умолчанию RU).
3. Группирует точки по городу (парсит адрес).
4. Сохраняет результат в pvz-data.js рядом с index.html.

Зависимости: только стандартная библиотека Python 3 (никаких pip install).
"""

import json
import re
import urllib.request
import os
from collections import defaultdict

WB_URL = 'https://static-basket-01.wb.ru/vol0/data/all-poo-fr-v8.json'
COUNTRY = 'ru'
OUTPUT_FILE = 'pvz-data.js'

CITY_PREFIX_RE = re.compile(r'^(г\.|город\s+|г\s+)', re.IGNORECASE)
FEDERAL_CITIES = {'Москва', 'Санкт-Петербург', 'Севастополь'}


def extract_city(address):
    """Извлекает название города из адреса WB-формата."""
    if not address:
        return None
    parts = [p.strip() for p in address.split(',')]
    if len(parts) < 2:
        return None
    fed = re.sub(r'^г\.\s*', '', parts[0]).strip()
    if fed in FEDERAL_CITIES:
        return fed
    return CITY_PREFIX_RE.sub('', parts[1]).strip()


def main():
    print(f'Скачиваю список ПВЗ с {WB_URL}…')
    with urllib.request.urlopen(WB_URL, timeout=60) as resp:
        raw = resp.read()
    print(f'Получено {len(raw) / 1024 / 1024:.2f} МБ')

    all_countries = json.loads(raw)
    country_data = next((c for c in all_countries if c.get('country') == COUNTRY), None)
    if not country_data or not country_data.get('items'):
        raise SystemExit(f'В ответе WB нет данных по стране {COUNTRY!r}')

    items = country_data['items']
    print(f'Точек по стране {COUNTRY.upper()}: {len(items)}')

    # Группируем по городу
    by_city = defaultdict(list)
    skipped = 0
    for it in items:
        city = extract_city(it.get('address', ''))
        coords = it.get('coordinates') or []
        if not city or len(coords) < 2:
            skipped += 1
            continue
        by_city[city].append({
            'id': it.get('id'),
            'address': it['address'],
            'lat': coords[0],
            'lon': coords[1],
        })

    print(f'Сгруппировано городов: {len(by_city)}, пропущено точек: {skipped}')

    # Топ-10 городов для проверки
    top = sorted(by_city.items(), key=lambda x: -len(x[1]))[:10]
    print('\nТоп-10 городов:')
    for c, pts in top:
        print(f'  {c}: {len(pts)}')

    # Сохраняем как JS-файл (window.PVZ_DATA = {...})
    payload = json.dumps(dict(by_city), ensure_ascii=False, separators=(',', ':'))
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write('window.PVZ_DATA = ')
        f.write(payload)
        f.write(';\n')

    size_mb = os.path.getsize(OUTPUT_FILE) / 1024 / 1024
    print(f'\nГотово. Записано в {OUTPUT_FILE} ({size_mb:.2f} МБ)')


if __name__ == '__main__':
    main()
