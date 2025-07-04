import sqlite3
import os

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'storage', 'plugins', 'steam_rental.db'))

def get_account_by_id(acc_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT login, password, email_login, email_password, imap_host FROM accounts WHERE id=?", (acc_id,))
    row = c.fetchone()
    conn.close()
    return row

def update_account_password(acc_id, new_password):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE accounts SET password = ? WHERE id = ?", (new_password, acc_id))
    conn.commit()
    conn.close()
