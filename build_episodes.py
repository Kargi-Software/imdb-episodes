import csv
import datetime
import gzip
import json
import shutil
import sqlite3
from pathlib import Path


DATASET_DIRECTORY = Path("datasets")
OUTPUT_DIRECTORY = Path("out")
EPISODE_DIRECTORY = OUTPUT_DIRECTORY / "episodes"
DATABASE_PATH = Path("episodes-build.sqlite3")

DATABASE_BATCH_SIZE = 25_000


def clean_text(value):
    if value in (None, "", r"\N"):
        return None

    return value


def open_dataset(filename):
    handle = gzip.open(
        DATASET_DIRECTORY / filename,
        mode="rt",
        encoding="utf-8",
        newline=""
    )

    reader = csv.DictReader(
        handle,
        delimiter="\t"
    )

    return handle, reader


def reset_build():
    if OUTPUT_DIRECTORY.exists():
        shutil.rmtree(OUTPUT_DIRECTORY)

    OUTPUT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True
    )

    EPISODE_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True
    )

    if DATABASE_PATH.exists():
        DATABASE_PATH.unlink()


def create_database():
    connection = sqlite3.connect(
        DATABASE_PATH,
        timeout=120
    )

    connection.execute(
        "PRAGMA journal_mode = OFF"
    )

    connection.execute(
        "PRAGMA synchronous = OFF"
    )

    connection.execute(
        "PRAGMA temp_store = MEMORY"
    )

    connection.execute(
        "PRAGMA cache_size = -250000"
    )

    # نکته: چون الان فقط اسم اپیزود لازمه (نه ژانر/کارگردان/بازیگر/...)،
    # جدول به سه ستون ساده شده - دیگه نیازی به title.crew,
    # title.principals و name.basics نیست.
    connection.execute("""
        CREATE TABLE episodes (
            tconst TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            parent_id TEXT
        ) WITHOUT ROWID
    """)

    connection.execute("""
        CREATE INDEX idx_episodes_parent
        ON episodes (parent_id, tconst)
    """)

    return connection


def load_episodes(connection):
    print(
        "Loading tvEpisode rows from "
        "title.basics..."
    )

    handle, reader = open_dataset(
        "title.basics.tsv.gz"
    )

    sql = """
        INSERT OR REPLACE INTO episodes (
            tconst,
            title
        )
        VALUES (?, ?)
    """

    batch = []
    count = 0

    try:
        for row in reader:
            if row.get("titleType") != "tvEpisode":
                continue

            title_id = row["tconst"]

            batch.append((
                title_id,
                clean_text(
                    row.get("primaryTitle")
                ) or title_id
            ))

            if len(batch) >= DATABASE_BATCH_SIZE:
                connection.executemany(sql, batch)
                count += len(batch)
                batch.clear()

                if count % 250_000 == 0:
                    print(
                        f"  Episodes loaded: "
                        f"{count:,}"
                    )

        if batch:
            connection.executemany(sql, batch)
            count += len(batch)

        connection.commit()

    finally:
        handle.close()

    print(f"Loaded {count:,} episodes.")

    return count


def load_episode_relations(connection):
    print(
        "Loading parent series (parentTconst) "
        "for each episode..."
    )

    handle, reader = open_dataset(
        "title.episode.tsv.gz"
    )

    sql = """
        UPDATE episodes
        SET parent_id = ?
        WHERE tconst = ?
    """

    batch = []
    count = 0

    try:
        for row in reader:
            batch.append((
                clean_text(
                    row.get("parentTconst")
                ),
                row["tconst"]
            ))

            if len(batch) >= DATABASE_BATCH_SIZE:
                connection.executemany(sql, batch)
                count += len(batch)
                batch.clear()

                if count % 250_000 == 0:
                    print(
                        "  Episode relations "
                        f"processed: {count:,}"
                    )

        if batch:
            connection.executemany(sql, batch)
            count += len(batch)

        connection.commit()

    finally:
        handle.close()

    print(
        f"Processed {count:,} "
        "episode relation rows."
    )


def get_series_shard_path(series_id):
    # tt1234567 -> episodes/12/tt1234567.json
    # همون فرمول قبلی: یک فایل به‌ازای هر سریال، توی پوشه‌ای که با دو رقم
    # اول آیدیِ سریال مشخص میشه (تا تعداد فایل هر پوشه معقول بمونه).
    shard_prefix = series_id[2:4].ljust(2, "0")

    directory = (
        EPISODE_DIRECTORY /
        shard_prefix
    )

    directory.mkdir(
        parents=True,
        exist_ok=True
    )

    return directory / f"{series_id}.json"


def write_shard(series_id, rows):
    if not rows:
        return 0

    # خروجی حالا فقط یک نگاشتِ ساده‌ی id -> عنوان است، نه یک آبجکت کامل.
    # این یعنی هم حجم فایل خیلی کمتره، هم دیگه نیازی به جدول names و
    # join با کارگردان/نویسنده/بازیگر نیست.
    output = {
        title_id: title
        for title_id, title in rows
    }

    output_path = get_series_shard_path(series_id)

    temporary_path = output_path.with_suffix(
        ".json.tmp"
    )

    with temporary_path.open(
        "w",
        encoding="utf-8"
    ) as output_file:
        json.dump(
            output,
            output_file,
            ensure_ascii=False,
            separators=(",", ":")
        )

    temporary_path.replace(output_path)

    return len(output)


def write_episode_shards(connection):
    print(
        "Writing episode shard files "
        "(grouped by series/parentId, "
        "title only)..."
    )

    # فقط اپیزودهایی که parent_id دارند در نظر گرفته می‌شوند، چون بدون
    # parent_id هیچ‌وقت از مسیر /series هم قابل دسترسی نبودند.
    query = """
        SELECT
            tconst,
            title,
            parent_id
        FROM episodes
        WHERE parent_id IS NOT NULL
        ORDER BY parent_id, tconst
    """

    cursor = connection.execute(query)

    current_parent_id = None
    current_rows = []

    episode_count = 0
    series_count = 0

    for tconst, title, parent_id in cursor:
        if current_parent_id is None:
            current_parent_id = parent_id

        elif parent_id != current_parent_id:
            written = write_shard(
                current_parent_id,
                current_rows
            )

            episode_count += written
            series_count += 1

            if series_count % 5_000 == 0:
                print(
                    f"  Series written: "
                    f"{series_count:,}; "
                    f"episodes: "
                    f"{episode_count:,}"
                )

            current_parent_id = parent_id
            current_rows = []

        current_rows.append((tconst, title))

    if current_rows:
        written = write_shard(
            current_parent_id,
            current_rows
        )

        episode_count += written
        series_count += 1

    print(
        f"Finished writing "
        f"{episode_count:,} episodes "
        f"into {series_count:,} series files."
    )

    return episode_count, series_count


def write_version_file(episode_count, series_count):
    version_data = {
        "updated": datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat(),
        "episodes": episode_count,
        "series": series_count,
        "formatVersion": 4,
        "shardStrategy": "parentId",
        "valueShape": "id -> title (string only)",
        "pathPattern": (
            "episodes/{prefix}/"
            "{seriesId}.json"
        )
    }

    with (
        OUTPUT_DIRECTORY /
        "version.json"
    ).open(
        "w",
        encoding="utf-8"
    ) as output_file:
        json.dump(
            version_data,
            output_file,
            ensure_ascii=False,
            separators=(",", ":")
        )


def main():
    reset_build()

    connection = create_database()

    try:
        expected_episode_count = load_episodes(
            connection
        )

        load_episode_relations(connection)

        episode_count, series_count = (
            write_episode_shards(connection)
        )

        skipped_count = (
            expected_episode_count - episode_count
        )

        if skipped_count > 0:
            print(
                f"Skipped {skipped_count:,} episodes "
                "without a parent series (these were "
                "never reachable via /series anyway)."
            )

        if skipped_count < 0:
            raise RuntimeError(
                "Episode count mismatch: "
                f"database={expected_episode_count:,}, "
                f"output={episode_count:,}"
            )

        write_version_file(
            episode_count,
            series_count
        )

    finally:
        connection.close()

    print("Build completed successfully.")
    print(
        f"Episodes: {episode_count:,}"
    )
    print(
        f"Series files: {series_count:,}"
    )


if __name__ == "__main__":
    main()
