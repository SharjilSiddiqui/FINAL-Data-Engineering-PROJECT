import os
import pandas as pd
import numpy as np
from pathlib import Path
from dateutil import parser
import pytz

DATA_DIR = Path("data")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

def read_csv(name):
    path = DATA_DIR / name
    if not path.exists():
        print(f"⚠️ Missing: {name}")
        return pd.DataFrame()
    print(f"✅ Reading {name}")
    return pd.read_csv(path)

def profile_df(df, name):
    profile = []
    for col in df.columns:
        profile.append({
            "table": name,
            "column": col,
            "null_count": int(df[col].isnull().sum()),
            "distinct_count": int(df[col].nunique(dropna=True))
        })
    return pd.DataFrame(profile)

def safe_initcap(val):
    if pd.isnull(val): return val
    try:
        return str(val).title()
    except:
        return val

def utc_to_local(ts, tz_name):
    if pd.isnull(ts): return pd.NaT
    try:
        dt = parser.parse(str(ts))
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)
        if tz_name and tz_name in pytz.all_timezones:
            return dt.astimezone(pytz.timezone(tz_name)).replace(tzinfo=None)
        else:
            return dt
    except Exception:
        return pd.NaT

# Load CSV Files 
lead_log = read_csv("lead_log.csv")
paid_transactions = read_csv("paid_transactions.csv")
referral_rewards = read_csv("referral_rewards.csv")
user_logs = read_csv("user_logs.csv")
user_referral_logs = read_csv("user_referral_logs.csv")
user_referral_statuses = read_csv("user_referral_statuses.csv")
user_referrals = read_csv("user_referrals.csv")

# Profiling 
profiles = []
for df, name in [
    (lead_log, "lead_log"),
    (paid_transactions, "paid_transactions"),
    (referral_rewards, "referral_rewards"),
    (user_logs, "user_logs"),
    (user_referral_logs, "user_referral_logs"),
    (user_referral_statuses, "user_referral_statuses"),
    (user_referrals, "user_referrals"),
]:
    if not df.empty:
        profiles.append(profile_df(df, name))

if profiles:
    pd.concat(profiles, ignore_index=True).to_csv(OUTPUT_DIR/"profiling_report.csv", index=False)
    print("📊 profiling_report.csv generated")

# Data Cleaning 
# Apply initcap on names (except homeclub)
for df in [user_referrals, user_logs]:
    for col in df.columns:
        if "name" in col and "homeclub" not in col:
            df[col] = df[col].apply(safe_initcap)

# Convert reward value to numeric
if 'reward_value' in referral_rewards.columns:
    referral_rewards['reward_value'] = pd.to_numeric(referral_rewards['reward_value'], errors='coerce')

# Timezone Conversion Example
if 'transaction_at' in paid_transactions.columns and 'timezone_transaction' in paid_transactions.columns:
    paid_transactions['transaction_at_local'] = paid_transactions.apply(
        lambda r: utc_to_local(r['transaction_at'], r['timezone_transaction']), axis=1
    )

# Join Tables
df = user_referrals.copy()

# Merge referral logs (latest per referral)
if not user_referral_logs.empty and 'user_referral_id' in user_referral_logs.columns:
    latest_logs = user_referral_logs.sort_values('created_at').groupby('user_referral_id').last().reset_index()
    df = df.merge(latest_logs.rename(columns={'user_referral_id': 'referral_id'}), how='left', on='referral_id')

# Merge referral rewards
if not referral_rewards.empty and 'id' in referral_rewards.columns:
    df = df.merge(referral_rewards.rename(columns={'id': 'referral_reward_id'}), how='left', on='referral_reward_id')

# Merge paid transactions
if not paid_transactions.empty and 'transaction_id' in paid_transactions.columns:
    df = df.merge(paid_transactions, how='left', on='transaction_id', suffixes=('','_trans'))

# Merge referrer info
if not user_logs.empty and 'user_id' in user_logs.columns:
    ref = user_logs.rename(columns={
        'user_id': 'referrer_id',
        'name': 'referrer_name',
        'phone_number': 'referrer_phone_number',
        'homeclub': 'referrer_homeclub'
    })[['referrer_id', 'referrer_name', 'referrer_phone_number', 'referrer_homeclub']]
    df = df.merge(ref, how='left', on='referrer_id')

# Business Logic: is_business_logic_valid
def check_valid(row):
    reward_val = row.get('reward_value')
    status = str(row.get('description','')).lower()
    tx_status = str(row.get('transaction_status','')).upper()
    tx_type = str(row.get('transaction_type','')).upper()
    tx_id = row.get('transaction_id')
    ref_time = pd.to_datetime(row.get('referral_at'), errors='coerce')
    tx_time = pd.to_datetime(row.get('transaction_at'), errors='coerce')
    reward_granted = pd.notnull(row.get('reward_granted_at'))

    # Valid 1
    if (
        pd.notnull(reward_val) and reward_val > 0 and
        status == 'berhasil' and
        pd.notnull(tx_id) and
        tx_status == 'PAID' and
        tx_type == 'NEW' and
        pd.notnull(tx_time) and pd.notnull(ref_time) and
        tx_time >= ref_time and
        reward_granted
    ):
        return True

    # Valid 2
    if status in ['menunggu','tidak berhasil'] and pd.isnull(reward_val):
        return True

    return False

df['is_business_logic_valid'] = df.apply(check_valid, axis=1)

# Final Columns
cols = [
    'referral_id','referral_source','referral_at','referrer_id','referrer_name',
    'referee_id','description','transaction_id','transaction_status','transaction_at',
    'reward_value','reward_granted_at','is_business_logic_valid'
]
cols = [c for c in cols if c in df.columns]
final = df[cols]

final.to_csv(OUTPUT_DIR/"final_report.csv", index=False)
print(f"✅ Final report generated with {len(final)} rows")
