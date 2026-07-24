# SQL-приклади для `history.sqlite3`

Довідка з корисними запитами для ручної роботи з базою (DBeaver, `sqlite3` CLI тощо).
Стосується таблиць `downloads` та `image_messages`.

## Знайти запис за назвою галереї

```sql
SELECT id, gallery_name FROM downloads WHERE gallery_name LIKE '%4039555%';
```

## Подивитись повідомлення конкретного завантаження

```sql
SELECT im.*
FROM image_messages im
JOIN downloads d ON d.id = im.download_id
WHERE d.gallery_name LIKE '%4039555%';
```

## Змінити `kind` (photo/document) для всіх фото завантаження

```sql
UPDATE image_messages
SET kind = 'document'
WHERE download_id IN (
    SELECT id FROM downloads WHERE gallery_name LIKE '%4039555%'
);
```

Якщо треба тільки конкретні файли (наприклад лише `.webp`):

```sql
UPDATE image_messages
SET kind = 'document'
WHERE download_id IN (SELECT id FROM downloads WHERE gallery_name LIKE '%4039555%')
  AND file_name LIKE '%.webp';
```

## Перевірити дублікати `image_messages` для завантаження

```sql
SELECT * FROM image_messages
WHERE download_id IN (
    SELECT id FROM downloads WHERE gallery_name LIKE '%4039555%'
);
```

## Прибрати дублікати, залишивши по одному найновішому запису на файл+тип

Для конкретного `download_id`:

```sql
DELETE FROM image_messages
WHERE download_id = 582
AND id NOT IN (
    SELECT MAX(id)
    FROM image_messages
    WHERE download_id = 582
    GROUP BY file_name, kind
);
```

Для всієї бази одразу:

```sql
DELETE FROM image_messages
WHERE id NOT IN (
    SELECT MAX(id)
    FROM image_messages
    GROUP BY download_id, file_name, kind
);
```

> Порада: перед `DELETE`/`UPDATE` спочатку виконай той самий запит як `SELECT`,
> щоб побачити, скільки рядків зачепить, і не забудь **Commit** у DBeaver
> (за замовчуванням транзакції ручні).
