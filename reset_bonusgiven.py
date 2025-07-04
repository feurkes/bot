import sqlite3

db_path = 'storage/plugins/steam_rental.db'
conn = sqlite3.connect(db_path)
c = conn.cursor()

# Сбросить bonus_given у всех аккаунтов, которые не в аренде
c.execute("""
    UPDATE accounts
    SET bonus_given = 0
    WHERE status != 'rented'
""")
conn.commit()
conn.close()

print("bonus_given успешно сброшен у всех свободных аккаунтов.")