import sqlite3

db_path = 'storage/plugins/steam_rental.db'
conn = sqlite3.connect(db_path)
c = conn.cursor()
c.execute("""
    UPDATE accounts
    SET status='free',
        tg_user_id=NULL,
        rented_until=NULL,
        warned_10min=0
    WHERE status='rented'
""")
conn.commit()
conn.close()
print("Все зависшие аренды успешно сброшены.")