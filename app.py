#!/usr/bin/env python3
"""
Script to fix/verify user authentication in analytics.db
Run this in the same directory as your app.py file
"""

import sqlite3
import hashlib
from pathlib import Path

DB_PATH = Path("analytics.db")

def hash_password(password: str) -> str:
    """Hash password using SHA256"""
    return hashlib.sha256(password.encode()).hexdigest()

def list_users():
    """List all users in database"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, username, password_hash, created_at FROM users")
    users = c.fetchall()
    conn.close()
    
    print("\n=== Current Users in Database ===")
    if not users:
        print("No users found!")
    else:
        for user_id, username, pwd_hash, created_at in users:
            print(f"ID: {user_id}")
            print(f"  Username: {username}")
            print(f"  Password Hash: {pwd_hash[:20]}...")
            print(f"  Created: {created_at}")
            print()
    return users

def test_login(username: str, password: str):
    """Test if username/password would work"""
    password_hash = hash_password(password)
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT password_hash FROM users WHERE username = ?", (username,))
    result = c.fetchone()
    conn.close()
    
    if not result:
        print(f"❌ User '{username}' not found")
        return False
    
    stored_hash = result[0]
    if stored_hash == password_hash:
        print(f"✅ Login would succeed for '{username}' with password '{password}'")
        return True
    else:
        print(f"❌ Password mismatch for '{username}'")
        print(f"   Expected hash: {stored_hash[:20]}...")
        print(f"   Provided hash: {password_hash[:20]}...")
        return False

def delete_all_users():
    """Delete all users (WARNING: destructive)"""
    response = input("⚠️  Are you sure you want to DELETE ALL USERS? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled.")
        return
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM users")
    conn.commit()
    deleted = c.rowcount
    conn.close()
    print(f"✅ Deleted {deleted} users")

def create_user(username: str, password: str):
    """Create or update a user"""
    password_hash = hash_password(password)
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Try to insert
    try:
        c.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)",
                 (username, password_hash))
        print(f"✅ Created user '{username}'")
    except sqlite3.IntegrityError:
        # User exists, update instead
        c.execute("UPDATE users SET password_hash = ? WHERE username = ?",
                 (password_hash, username))
        print(f"✅ Updated user '{username}'")
    
    conn.commit()
    conn.close()

def reset_default_users():
    """Reset to default users: mohammad and saud"""
    print("\n=== Resetting to Default Users ===")
    
    default_users = [
        ("mohammad", "mohammad123"),
        ("saud", "saud123")
    ]
    
    for username, password in default_users:
        create_user(username, password)
    
    print("\n=== Verifying Logins ===")
    for username, password in default_users:
        test_login(username, password)

def main():
    """Main interactive menu"""
    while True:
        print("\n" + "="*50)
        print("BGB Automator - User Management Tool")
        print("="*50)
        print("1. List all users")
        print("2. Test login credentials")
        print("3. Create/Update a user")
        print("4. Reset to default users (mohammad/saud)")
        print("5. Delete all users (DANGER!)")
        print("6. Exit")
        print()
        
        choice = input("Select option (1-6): ").strip()
        
        if choice == "1":
            list_users()
        
        elif choice == "2":
            username = input("Username: ").strip()
            password = input("Password: ").strip()
            test_login(username, password)
        
        elif choice == "3":
            username = input("Username: ").strip()
            password = input("Password: ").strip()
            if username and password:
                create_user(username, password)
            else:
                print("❌ Username and password cannot be empty")
        
        elif choice == "4":
            reset_default_users()
        
        elif choice == "5":
            delete_all_users()
        
        elif choice == "6":
            print("Goodbye!")
            break
        
        else:
            print("Invalid option, please try again")

if __name__ == "__main__":
    if not DB_PATH.exists():
        print(f"❌ Database not found at: {DB_PATH}")
        print("Please run this script in the same directory as your app.py")
        exit(1)
    
    print(f"✅ Found database at: {DB_PATH}")
    main()