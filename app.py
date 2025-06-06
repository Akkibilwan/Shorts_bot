
import streamlit as st
import threading
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from isodate import parse_duration
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

# --------------------------- Configuration ---------------------------

st.set_page_config(layout="wide")

API_KEY = st.secrets["youtube"]["api_key"]

CHANNEL_IDS = [
    "UC415bOPUcGSamy543abLmRA",
    "UCRzYN32xtBf3Yxsx5BvJWJw",
    "UCVOTBwF0vnSxMRIbfSE_K_g",
    "UCPk2s5c4R_d-EUUNvFFODoA",
    "UCwAdQUuPT6laN-AQR17fe1g",
    "UCA295QVkf9O1RQ8_-s3FVXg",
    "UCkw1tYo7k8t-Y99bOXuZwhg",
    "UCxgAuX3XZROujMmGphN_scA",
    "UCUUlw3anBIkbW9W44Y-eURw",
]

# Google Sheet Configuration
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1OdRsySMe4jcc7xxr01MJFmG94msoYEZWgEflVSj0vRs/edit?gid=0#gid=0"

# ----------------------- Google Sheets Setup ---------------------------

@st.cache_resource(ttl=3600)
def get_google_sheet_client():
    try:
        # Load credentials from Streamlit secrets
        creds_json = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Error setting up Google Sheets client: {e}")
        return None

@st.cache_resource(ttl=3600)
def get_spreadsheet(client):
    try:
        spreadsheet = client.open_by_url(GOOGLE_SHEET_URL)
        return spreadsheet
    except Exception as e:
        st.error(f"Error opening spreadsheet: {e}")
        return None

@st.cache_resource(ttl=3600)
def get_worksheet(spreadsheet, worksheet_name="Sheet1"):
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        return worksheet
    except Exception as e:
        st.error(f"Error opening worksheet '{worksheet_name}': {e}")
        return None

# ----------------------- Helper Functions ----------------------------

def create_youtube_client():
    """Each caller gets its own YouTube Data API client."""
    return build("youtube", "v3", developerKey=API_KEY)

def iso8601_to_seconds(duration_str: str) -> int:
    """Convert an ISO 8601 duration (e.g., 'PT45S') into total seconds."""
    try:
        return int(parse_duration(duration_str).total_seconds())
    except:
        return 0

def get_midnight_ist_utc() -> datetime:
    """
    Return a timezone-aware UTC datetime corresponding to today's midnight in IST.
    IST = UTC + 5:30 
    """
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    today_ist_date = now_ist.date()
    midnight_ist = datetime(
        today_ist_date.year,
        today_ist_date.month,
        today_ist_date.day,
        0,
        0,
        tzinfo=timezone(timedelta(hours=5, minutes=30))
    )
    return midnight_ist.astimezone(timezone.utc)

def is_within_today(published_at_str: str) -> bool:
    """
    Return True if a video's publishedAt (UTC) falls within “today in IST.”
    """
    try:
        pub_dt = datetime.fromisoformat(published_at_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except:
        return False
    midnight_utc = get_midnight_ist_utc()
    next_midnight_utc = midnight_utc + timedelta(hours=24)
    return midnight_utc <= pub_dt < next_midnight_utc

@st.cache_data(ttl=86400)  # cache for 24 hours
def discover_and_initial_stats():
    """
    1. Discover all Shorts (<= 180 s) published “today in IST” across CHANNEL_IDS.
    2. Return:
       - shorts_data: {video_id: [ (timestamp, viewCount, likeCount, commentCount), ... ]}
       - video_to_channel: {video_id: channel_title}
       - video_to_published: {video_id: published_datetime_UTC}
       - discovery_logs: [string, …]
    The initial stats fetch runs once here (cached), so it won't repeat on every rerun.
    """
    shorts_data = {}
    video_to_channel = {}
    video_to_published = {}
    logs = []
    no_shorts = False

    youtube = create_youtube_client()
    today_shorts = []

    for idx, channel_id in enumerate(CHANNEL_IDS, start=1):
        # 1) Fetch channel title & uploads playlist
        try:
            ch_resp = youtube.channels().list(
                part="snippet,contentDetails",
                id=channel_id
            ).execute()
        except HttpError as e:
            logs.append(f"API Error (channel fetch for {channel_id}): {e}")
            return {}, {}, {}, logs, True  # treat as "no_shorts" to stop UI
        
        channel_title = ch_resp["items"][0]["snippet"]["title"]
        uploads_playlist = ch_resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        logs.append(f"Checking channel {idx}/{len(CHANNEL_IDS)}: {channel_title}")

        # 2) Page through uploads playlist
        pl_req = youtube.playlistItems().list(
            part="snippet",
            playlistId=uploads_playlist,
            maxResults=50
        )
        channel_shorts = []
        while pl_req:
            try:
                pl_resp = pl_req.execute()
            except HttpError as e:
                logs.append(f"API Error (playlistItems for {channel_title}): {e}")
                return {}, {}, {}, logs, True

            for item in pl_resp["items"]:
                vid_id = item["snippet"]["resourceId"]["videoId"]
                published_at = item["snippet"]["publishedAt"]
                if not is_within_today(published_at):
                    continue

                # 3) For each candidate, fetch duration + snippet data
                try:
                    cd_resp = youtube.videos().list(
                        part="contentDetails,snippet",
                        id=vid_id
                    ).execute()
                except HttpError as e:
                    logs.append(f"API Error (video contentDetails for {vid_id}): {e}")
                    return {}, {}, {}, logs, True

                duration_secs = iso8601_to_seconds(cd_resp["items"][0]["contentDetails"]["duration"])
                if duration_secs <= 180:
                    pub_iso = cd_resp["items"][0]["snippet"]["publishedAt"]
                    pub_dt = datetime.fromisoformat(pub_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
                    video_to_channel[vid_id] = channel_title
                    video_to_published[vid_id] = pub_dt
                    channel_shorts.append(vid_id)

            pl_req = youtube.playlistItems().list_next(pl_req, pl_resp)

        if channel_shorts:
            logs.append(f"Channel {idx}: Found {len(channel_shorts)} Shorts in '{channel_title}'")
            today_shorts.extend(channel_shorts)
        else:
            logs.append(f"Channel {idx}: No Shorts found today in '{channel_title}'")

    if not today_shorts:
        logs.append("No Shorts published today in IST across all channels.")
        return {}, {}, {}, logs, True

    # 4) Initialize shorts_data and do an initial stats fetch (cached)
    for vid in today_shorts:
        shorts_data[vid] = []

    now_ts = datetime.now(timezone.utc).isoformat()
    for i in range(0, len(today_shorts), 50):
        batch = today_shorts[i:i+50]
        try:
            stats_resp = youtube.videos().list(
                part="statistics",
                id=",".join(batch)
            ).execute()
        except HttpError as e:
            logs.append(f"API Error (initial stats fetch): {e}")
            return {}, {}, {}, logs, True

        for vid_item in stats_resp["items"]:
            vid = vid_item["id"]
            stats = vid_item["statistics"]
            row = (
                now_ts,
                int(stats.get("viewCount", 0)),
                int(stats.get("likeCount", 0)),
                int(stats.get("commentCount", 0)),
            )
            shorts_data[vid].append(row)

    return shorts_data, video_to_channel, video_to_published, logs, False

def poll_stats_background():
    """
    Background thread: once discovery+initial fetch is done, this runs every hour
    to append new (timestamp, viewCount, likeCount, commentCount) per video.
    """
    global error_message

    # First, wait until we have run discovery (cached)
    while True:
        # If discovery had an error, bail
        if error_message:
            return

        # If the cached discovery returned no_shorts_flag=True, also bail
        if "no_shorts_flag" in st.session_state and st.session_state.no_shorts_flag:
            return

        # If discovery_keys exist in session_state, break out
        if "shorts_data" in st.session_state:
            break

        time.sleep(1)

    # Poll hourly
    while True:
        # If an error occurred, stop
        if st.session_state.error_message:
            return

        youtube = create_youtube_client()
        now_ts = datetime.now(timezone.utc).isoformat()

        vids = list(st.session_state.shorts_data.keys())
        for i in range(0, len(vids), 50):
            batch = vids[i:i+50]
            try:
                stats_resp = youtube.videos().list(
                    part="statistics",
                    id=",".join(batch)
                ).execute()
            except HttpError as e:
                st.session_state.error_message = f"API Error (polling): {e}"
                return

            # Append new row to each video
            for vid_item in stats_resp["items"]:
                vid = vid_item["id"]
                stats = vid_item["statistics"]
                row = (
                    now_ts,
                    int(stats.get("viewCount", 0)),
                    int(stats.get("likeCount", 0)),
                    int(stats.get("commentCount", 0)),
                )
                with data_lock:
                    st.session_state.shorts_data[vid].append(row)

        # Write to Google Sheet
        try:
            gc = get_google_sheet_client()
            if gc:
                spreadsheet = get_spreadsheet(gc)
                if spreadsheet:
                    worksheet = get_worksheet(spreadsheet)
                    if worksheet:
                        # Prepare data for Google Sheet
                        all_rows = []
                        for video_id, data_points in st.session_state.shorts_data.items():
                            channel_title = st.session_state.video_to_channel.get(video_id, "N/A")
                            published_at = st.session_state.video_to_published.get(video_id, "N/A")
                            for dp in data_points:
                                all_rows.append([
                                    video_id,
                                    channel_title,
                                    published_at.isoformat() if published_at != "N/A" else "N/A",
                                    dp[0], # timestamp
                                    dp[1], # viewCount
                                    dp[2], # likeCount
                                    dp[3]  # commentCount
                                ])
                        
                        # Clear existing data and write new data
                        worksheet.clear()
                        worksheet.append_row(["Video ID", "Channel Title", "Published At", "Timestamp", "View Count", "Like Count", "Comment Count"])
                        worksheet.append_rows(all_rows)
                        st.session_state.discovery_logs.append(f"Data written to Google Sheet at {now_ts}")
                    else:
                        st.session_state.discovery_logs.append("Error: Could not get worksheet.")
                else:
                    st.session_state.discovery_logs.append("Error: Could not get spreadsheet.")
            else:
                st.session_state.discovery_logs.append("Error: Could not get Google Sheet client.")
        except Exception as e:
            st.session_state.discovery_logs.append(f"Error writing to Google Sheet: {e}")

        # Sleep until next top of the hour
        now = datetime.now(timezone.utc)
        secs_until_next = 3600 - (now.minute * 60 + now.second)
        time.sleep(secs_until_next)

# ----------------------- Main App Logic ----------------------------

# Initialize data_lock for thread-safe access to session_state.shorts_data
if "data_lock" not in st.session_state:
    st.session_state.data_lock = threading.Lock()
data_lock = st.session_state.data_lock

# 1) Run discovery + initial stats via cached function
shorts_data_cache, video_to_channel_cache, video_to_published_cache, logs_cache, no_shorts_flag_cache = discover_and_initial_stats()

# Store into session_state so UI and background thread can see them
if "initialized" not in st.session_state:
    st.session_state.initialized = True
    st.session_state.shorts_data = shorts_data_cache
    st.session_state.video_to_channel = video_to_channel_cache
    st.session_state.video_to_published = video_to_published_cache
    st.session_state.discovery_logs = logs_cache
    st.session_state.error_message = None
    st.session_state.no_shorts_flag = no_shorts_flag_cache

    # Immediately show logs, then start the polling thread
    threading.Thread(target=poll_stats_background, daemon=True).start()

# 2) UI Rendering

st.title("📊 YouTube Shorts VPH & Engagement Tracker")

# Show discovery logs
st.subheader("Discovery Progress")
for log in st.session_state.discovery_logs:
    st.write(log)

# Show error or no-shorts, if any
if st.session_state.no_shorts_flag:
    st.info("No Shorts (≤ 3 minutes) were uploaded today in IST for the selected channels.")
    st.stop()

if st.session_state.error_message:
    st.error(st.session_state.error_message)
    st.stop()

# At this point, we have at least one video and initial stats
if not st.session_state.shorts_data:
    st.info("Waiting for initial stats fetch to complete...")
    st.stop()

# Let user pick a video
st.subheader("Available Shorts")
vids = list(st.session_state.shorts_data.keys())
options = [
    f"{st.session_state.video_to_channel[v]} → {v}"
    for v in vids
]
sel = st.selectbox("Select a channel → video", options)
vid_selected = sel.split(" → ")[1]

# Build DataFrame for that video
rows = st.session_state.shorts_data[vid_selected]
if not rows:
    st.warning("No stats captured yet for this video. Please wait a moment.")
    st.stop()

df = pd.DataFrame(rows, columns=["timestamp", "viewCount", "likeCount", "commentCount"])
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Compute VPH:
# - First row: total_views ÷ hours_since_published
# - Subsequent rows: diff in viewCount
published = st.session_state.video_to_published[vid_selected]
first_ts = df["timestamp"].iloc[0]
hours_since_pub = max((first_ts - published).total_seconds() / 3600, 1e-6)
first_vph = df["viewCount"].iloc[0] / hours_since_pub
vph_vals = [first_vph] + df["viewCount"].diff().iloc[1:].tolist()
df["vph"] = vph_vals

# Compute engagement rate = (likes + comments) / views
df["engagement_rate"] = (df["likeCount"] + df["commentCount"]) / df["viewCount"]

# Show metrics
st.subheader(f"Metrics for: {st.session_state.video_to_channel[vid_selected]} → {vid_selected}")
latest = df.iloc[-1]
st.markdown(f"""
- **Timestamp (UTC):** {latest["timestamp"]}
- **Total Views:** {int(latest["viewCount"])}
- **Views Per Hour (VPH):** {latest["vph"]:.2f}
- **Engagement Rate:** {latest["engagement_rate"]:.2%}
""")

# Charts
st.subheader("VPH Over Time")
st.line_chart(df.set_index("timestamp")["vph"])

st.subheader("Engagement Rate Over Time")
st.line_chart(df.set_index("timestamp")["engagement_rate"])

# Raw data
st.subheader("Raw Data Table")
st.dataframe(df, use_container_width=True)


