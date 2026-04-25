#!/usr/bin/env python3
"""
Скрипт обновления локального кэша ПВЗ Wildberries.

Запуск из корня репозитория:
    python scripts/parse_wb_pvz.py

Что делает:
1. Скачивает актуальный список ПВЗ Wildberries с публичного эндпоинта WB.
2. Фильтрует по стране (по умолчанию RU).
3. Парсит адреса, выделяет город (умеет 4 формата адресов WB).
4. Делит точки на бакеты до ~100 КБ каждый:
   - крупные города (>=300 точек) — каждый в свои файлы (с чанками);
   - остальные — группируются по первой букве с бин-пакингом.
5. Сохраняет в data/index.js + data/{city|letter}-*.js.

Зависимости: только стандартная библиотека Python 3.
"""

import json
import re
import urllib.request
import os
import shutil
from collections import defaultdict

WB_URL = 'https://static-basket-01.wb.ru/vol0/data/all-poo-fr-v8.json'
COUNTRY = 'ru'
OUT_DIR = 'data'
LIMIT_BYTES = 100 * 1024  # макс размер бакет-файла

FEDERAL = {'Москва', 'Санкт-Петербург', 'Севастополь'}
REGION_KW = ('республика', 'край', 'область', 'округ', 'автономн', 'обл.', 'обл ', 'обл,')
COUNTRY_PFX = ('россия', 'российская федерация', 'рф')

CITY_PREFIX_RE = re.compile(r'^(г\.|город\s+|г\s+|пос\.?\s+|с\.\s+|д\.\s+|село\s+|деревня\s+)', re.IGNORECASE)
G_PREFIX_RE = re.compile(r'^г\.?\s+', re.IGNORECASE)


def is_region(s):
    return any(kw in s.lower() for kw in REGION_KW)


def is_country(s):
    return s.lower().strip().rstrip('.') in COUNTRY_PFX


def extract_city(address):
    """Достаёт город из адреса WB. Поддерживает 4 формата."""
    if not address:
        return None
    parts = [p.strip() for p in address.split(',')]
    if len(parts) < 2:
        return None
    if is_country(parts[0]):
        parts = parts[1:]
        if len(parts) < 2:
            return None
    p0 = parts[0]
    if '(' in p0 or G_PREFIX_RE.match(p0):
        city = G_PREFIX_RE.sub('', p0)
        city = re.sub(r'\s*\([^)]*\)\s*', '', city).strip()
        city = CITY_PREFIX_RE.sub('', city).strip()
        return city or None
    if p0 in FEDERAL:
        return p0
    if not is_region(p0):
        return CITY_PREFIX_RE.sub('', p0).strip() or None
    return CITY_PREFIX_RE.sub('', parts[1]).strip() or None


def safe_id(s):
    out = ''
    for ch in s:
        if ch.isalnum() or ch in '-АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзийклмнопрстуфхцчшщъыьэюя':
            out += ch
        elif ch == ' ':
            out += '-'
    return out


def estimate_size(obj):
    return len(json.dumps(obj, ensure_ascii=False, separators=(',', ':')).encode('utf-8'))


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
    for it in items:
        city = extract_city(it.get('address', ''))
        coords = it.get('coordinates') or []
        if not city or len(city) < 2 or len(coords) < 2:
            continue
        by_city[city].append({
            'id': it.get('id'),
            'address': it['address'],
            'lat': coords[0],
            'lon': coords[1],
        })

    print(f'Городов: {len(by_city)}')
    top = sorted(by_city.items(), key=lambda x: -len(x[1]))[:10]
    print('Топ-10 городов:')
    for c, pts in top:
        print(f'  {c}: {len(pts)}')

    # Делим на бакеты
    buckets = {}                    # bucket_id -> {city: [points]}
    city_to_buckets = defaultdict(list)  # city -> [bucket_ids]

    # 1. Крупные города (>=300 точек) — каждый в свои файлы (если нужно — на чанки)
    for city, pts in list(by_city.items()):
        if len(pts) < 300:
            continue
        base = 'city-' + safe_id(city)
        chunk = []
        chunk_idx = 1
        for pt in pts:
            test = chunk + [pt]
            if estimate_size({city: test}) > LIMIT_BYTES and chunk:
                bid = f'{base}-{chunk_idx}'
                buckets[bid] = {city: chunk}
                city_to_buckets[city].append(bid)
                chunk = [pt]
                chunk_idx += 1
            else:
                chunk = test
        if chunk:
            bid = f'{base}-{chunk_idx}'
            buckets[bid] = {city: chunk}
            city_to_buckets[city].append(bid)

    # 2. Остальные — группируем по букве, бин-пакинг
    remaining = {c: pts for c, pts in by_city.items() if c not in city_to_buckets}
    by_letter = defaultdict(list)
    for city, pts in remaining.items():
        first = city[0].upper()
        if not re.match(r'[А-ЯЁ]', first):
            first = 'OTHER'
        by_letter[first].append((city, pts))

    for letter, lst in sorted(by_letter.items()):
        lst.sort(key=lambda x: x[0])
        current = {}
        idx = 1
        for city, pts in lst:
            test = dict(current)
            test[city] = pts
            if estimate_size(test) > LIMIT_BYTES and current:
                bid = f'letter-{letter}-{idx}'
                buckets[bid] = current
                for c in current:
                    city_to_buckets[c].append(bid)
                current = {city: pts}
                idx += 1
            else:
                current = test
        if current:
            bid = f'letter-{letter}-{idx}'
            buckets[bid] = current
            for c in current:
                city_to_buckets[c].append(bid)

    # Индекс
    city_index = []
    for city in by_city:
        bids = city_to_buckets[city]
        city_index.append([city, bids, len(by_city[city])])
    city_index.sort(key=lambda x: -x[2])

    # Записываем
    if os.path.exists(OUT_DIR):
        shutil.rmtree(OUT_DIR)
    os.makedirs(OUT_DIR)

    with open(f'{OUT_DIR}/index.js', 'w', encoding='utf-8') as f:
        f.write('window.PVZ_INDEX = ')
        f.write(json.dumps(city_index, ensure_ascii=False, separators=(',', ':')))
        f.write(';\n')

    sizes = []
    for bid, cities in sorted(buckets.items()):
        path = f'{OUT_DIR}/{bid}.js'
        with open(path, 'w', encoding='utf-8') as f:
            f.write('window.PVZ_BUCKETS = window.PVZ_BUCKETS || {};\n')
            f.write(f'window.PVZ_BUCKETS[{json.dumps(bid)}] = ')
            f.write(json.dumps(cities, ensure_ascii=False, separators=(',', ':')))
            f.write(';\n')
        sizes.append(os.path.getsize(path))

    idx_size = os.path.getsize(f'{OUT_DIR}/index.js')
    total = (sum(sizes) + idx_size) / 1024 / 1024
    print(f'\nГотово.')
    print(f'  Файлов в {OUT_DIR}/: {len(sizes) + 1} ({len(sizes)} бакетов + index.js)')
    print(f'  Самый большой бакет: {max(sizes) / 1024:.1f} КБ')
    print(f'  Index.js: {idx_size / 1024:.1f} КБ')
    print(f'  Общий размер: {total:.2f} МБ')


if __name__ == '__main__':
    main()
