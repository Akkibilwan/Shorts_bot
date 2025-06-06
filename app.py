import streamlit as st
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from isodate import parse_duration
import gspread
from oauth2client.service_account import ServiceAccountCredentials

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

GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1OdRsySMe4jcc7xxr01MJFmG94msoYEZWgEflVSj0vRs/edit"


# ----------------------- Google Sheets Setup ---------------------------

@st.cache_resource(ttl=3600)
def get_google_sheet_client():
    try:
        creds_dict = st.secrets["gcp_service_account"]
        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Error setting up Google Sheets client: {e}")
        return None

@st.cache_resource(ttl=3600)
def get_worksheet():
    client = get_google_sheet_client()
    if not client:
        return None
    try:
        spreadsheet = client.open_by_url(GOOGLE_SHEET_URL)
        worksheet = spreadsheet.worksheet("Sheet1")
        return worksheet
    except Exception as e:
        st.error(f"Error opening worksheet 'Sheet1': {e}")
        return None


# ----------------------- YouTube Helper Functions ----------------------------

def create_youtube_client():
    return build("youtube", "v3", developerKey=API_KEY)

def iso8601_to_seconds(duration_str: str) -> int:
    try:
        return int(parse_duration(duration_str).total_seconds())
    except:
        return 0

def get_midnight_ist_utc() -> datetime:
    now_utc = datetime.now(timezone.utc)
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    now_ist = now_utc.astimezone(ist_tz)
    today_ist = now_ist.date()
    midnight_ist = datetime(
        year=today_ist.year,
        month=today_ist.month,
        day=today_ist.day,
        hour=0,
        minute=0,
        second=0,
        tzinfo=ist_tz
    )
    return midnight_ist.astimezone(timezone.utc)

def is_within_today(published_at_str: str) -> bool:
    try:
        pub_dt = datetime.fromisoformat(published_at_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except:
        return False
    midnight_utc = get_midnight_ist_utc()
    next_midnight_utc = midnight_utc + timedelta(days=1)
    return midnight_utc <= pub_dt < next_midnight_utc

def retry_youtube_call(func_or_request, *args, **kwargs):
    """
    If func_or_request is a HttpRequest, call .execute()
    If it's a callable, call func_or_request(*args, **kwargs).execute()
    Retry once on HttpError.
    """
    if hasattr(func_or_request, "execute") and not callable(func_or_request):
        request = func_or_request
        try:
            return request.execute()
        except HttpError as e:
            st.warning(f"YouTube API error (first attempt): {e}")
            time.sleep(2)
            try:
                return request.execute()
            except HttpError as e2:
                st.error(f"YouTube API error (second attempt): {e2}")
                return None
    else:
        try:
            return func_or_request(*args, **kwargs).execute()
        except HttpError as e:
            st.warning(f"YouTube API error (first attempt): {e}")
            time.sleep(2)
            try:
                return func_or_request(*args, **kwargs).execute()
            except HttpError as e2:
                st.error(f"YouTube API error (second attempt): {e2}")
                return None

def discover_shorts():
    """
    Discover all Shorts (<= 180s) published “today in IST” across CHANNEL_IDS.
    Returns:
      • video_to_channel: { video_id: channel_title }
      • video_to_published: { video_id: published_datetime_UTC }
      • logs: list of log strings
      • no_shorts_flag: True if no Shorts found or a fatal error occurred
    """
    youtube = create_youtube_client()
    video_to_channel = {}
    video_to_published = {}
    logs = []
    all_short_ids = []

    for idx, channel_id in enumerate(CHANNEL_IDS, start=1):
        ch_resp = retry_youtube_call(
            youtube.channels().list,
            part="snippet,contentDetails",
            id=channel_id
        )
        if not ch_resp or not ch_resp.get("items"):
            logs.append(f"❌ Error fetching channel info for {channel_id}; skipping")
            return {}, {}, logs, True

        channel_title = ch_resp["items"][0]["snippet"]["title"]
        uploads_playlist = ch_resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        logs.append(f"🔍 Checking channel {idx}/{len(CHANNEL_IDS)}: {channel_title}")

        pl_req = youtube.playlistItems().list(
            part="snippet",
            playlistId=uploads_playlist,
            maxResults=50
        )
        while pl_req:
            pl_resp = retry_youtube_call(pl_req)
            if not pl_resp:
                logs.append(f"❌ Error fetching playlistItems for {channel_title}; skipping channel.")
                return {}, {}, logs, True

            for item in pl_resp.get("items", []):
                vid_id = item["snippet"]["resourceId"]["videoId"]
                published_at = item["snippet"]["publishedAt"]
                if not is_within_today(published_at):
                    continue

                cd_resp = retry_youtube_call(
                    youtube.videos().list,
                    part="contentDetails,snippet",
                    id=vid_id
                )
                if not cd_resp or not cd_resp.get("items"):
                    logs.append(f"⚠️ Could not fetch contentDetails for {vid_id}; skipping")
                    continue

                duration_secs = iso8601_to_seconds(
                    cd_resp["items"][0]["contentDetails"]["duration"]
                )
                if duration_secs <= 180:
                    pub_iso = cd_resp["items"][0]["snippet"]["publishedAt"]
                    pub_dt = datetime.fromisoformat(pub_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
                    video_to_channel[vid_id] = channel_title
                    video_to_published[vid_id] = pub_dt
                    all_short_ids.append(vid_id)

            pl_req = youtube.playlistItems().list_next(pl_req, pl_resp)

        if all_short_ids:
            logs.append(f"✅ Found {len(all_short_ids)} Shorts in '{channel_title}' so far")

    if not all_short_ids:
        logs.append("ℹ️ No Shorts published today in IST across all channels.")
        return {}, {}, logs, True

    logs.append(f"ℹ️ Total discovered Shorts: {len(all_short_ids)}")
    return video_to_channel, video_to_published, logs, False

def fetch_statistics(video_ids):
    """
    Given a list of video IDs, return stats { video_id: { viewCount, likeCount, commentCount } }.
    """
    youtube = create_youtube_client()
    stats_dict = {}

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        resp = retry_youtube_call(
            youtube.videos().list,
            part="statistics",
            id=",".join(batch)
        )
        if not resp:
            continue
        for item in resp.get("items", []):
            vid = item["id"]
            stat = item.get("statistics", {})
            stats_dict[vid] = {
                "viewCount": int(stat.get("viewCount", 0)),
                "likeCount": int(stat.get("likeCount", 0)),
                "commentCount": int(stat.get("commentCount", 0)),
            }
    return stats_dict


# ----------------------- Core “Run Now” Function ----------------------------

def run_once_and_append():
    st.info("🔍 Reading existing sheet and discovering new Shorts…")
    ws = get_worksheet()
    if ws is None:
        st.error("Cannot connect to Google Sheet. Aborting.")
        return

    # 1) Read entire sheet
    try:
        all_data = ws.get_all_values()
    except Exception as e:
        st.error(f"Error reading sheet: {e}")
        return

    header = all_data[0] if all_data else []
    rows = all_data[1:] if len(all_data) > 1 else []

    # Ensure header has nine columns; if sheet is empty, set header
    if not header or len(header) < 9:
        try:
            ws.clear()
            ws.append_row([
                "video_id",
                "channel_title",
                "published_at",
                "timestamp",
                "viewCount",
                "likeCount",
                "commentCount",
                "vph",
                "engagement_rate"
            ], value_input_option="RAW")
            all_data = ws.get_all_values()
            header = all_data[0]
            rows = all_data[1:]
        except Exception as e:
            st.error(f"Error initializing header row: {e}")
            return

    # 2) Build maps from existing rows: track each video_id -> (channel_title, published_at)
    tracked_ids = set()
    video_to_channel_past = {}
    video_to_published_past = {}
    for r in rows:
        if len(r) < 4:
            continue
        vid = r[0]
        if vid not in tracked_ids:
            tracked_ids.add(vid)
            video_to_channel_past[vid] = r[1]
            try:
                video_to_published_past[vid] = datetime.fromisoformat(r[2].replace("Z", "+00:00")).astimezone(timezone.utc)
            except:
                # If parsing fails, skip
                pass

    # 3) Discover any new Shorts uploaded today in IST
    st.info("🔍 Discovering Shorts published today in IST…")
    video_to_channel_new, video_to_published_new, discover_logs, no_shorts_flag = discover_shorts()
    for line in discover_logs:
        st.write(line)

    if no_shorts_flag:
        st.warning("No Shorts published today in IST (or an error occurred). Proceeding to poll existing IDs only.")
    else:
        # Add newly discovered IDs to our maps
        for vid, ch in video_to_channel_new.items():
            if vid not in tracked_ids:
                tracked_ids.add(vid)
                video_to_channel_past[vid] = ch
                video_to_published_past[vid] = video_to_published_new[vid]
        st.success(f"ℹ️ Total tracked Shorts: {len(tracked_ids)}")

    # 4) Fetch current stats for all tracked IDs
    st.info(f"🕒 Fetching stats for {len(tracked_ids)} tracked Shorts…")
    all_ids = list(tracked_ids)
    stats = fetch_statistics(all_ids)
    if not stats:
        st.error("Failed to fetch statistics for any tracked videos.")
        return

    now_utc = datetime.now(timezone.utc)
    timestamp_iso = now_utc.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 5) Build new rows for each tracked video
    new_rows = []
    for vid in all_ids:
        if vid not in stats:
            st.warning(f"Skipping {vid} (no stats returned).")
            continue
        if vid not in video_to_published_past:
            st.warning(f"Skipping {vid} (missing published_at).")
            continue
        published_dt = video_to_published_past[vid]
        published_iso = published_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        channel_title = video_to_channel_past.get(vid, "N/A")

        viewCount = stats[vid]["viewCount"]
        likeCount = stats[vid]["likeCount"]
        commentCount = stats[vid]["commentCount"]

        delta_hours = max((now_utc - published_dt).total_seconds() / 3600.0, 1/3600.0)
        vph = viewCount / delta_hours
        engagement_rate = ((likeCount + commentCount) / viewCount) if viewCount > 0 else 0.0

        row = [
            vid,
            channel_title,
            published_iso,
            timestamp_iso,
            str(viewCount),
            str(likeCount),
            str(commentCount),
            f"{vph:.2f}",
            f"{engagement_rate:.4f}"
        ]
        new_rows.append(row)

    if not new_rows:
        st.info("No new stat rows to append.")
        return

    # 6) Filter out duplicates by checking (video_id, timestamp) in existing rows
    existing_pairs = set()
    for r in rows:
        if len(r) >= 4:
            existing_pairs.add((r[0], r[3]))

    filtered_rows = []
    for r in new_rows:
        key = (r[0], r[3])
        if key in existing_pairs:
            st.info(f"Skipping duplicate for {r[0]} @ {r[3]}")
        else:
            filtered_rows.append(r)

    if not filtered_rows:
        st.info("All new stats already exist in the sheet.")
        return

    # 7) Append filtered rows in one batch
    try:
        ws.append_rows(filtered_rows, value_input_option="RAW")
        st.success(f"✅ Appended {len(filtered_rows)} new row(s) to the sheet.")
    except Exception as e:
        st.error(f"Error appending to sheet: {e}")


# ----------------------- Streamlit UI ----------------------------

st.title("📊 YouTube Shorts VPH & Engagement Tracker")

st.write(
    """
    This app will:
    1. Read all tracked Short IDs from the Google Sheet.
    2. Discover any new Shorts uploaded *today in IST* across the nine channels and add them to tracking.
    3. Fetch current stats (viewCount, likeCount, commentCount) for every tracked Short (including older ones).
    4. Compute VPH and engagement rate, then append a new row (per video) with the current timestamp into the sheet.
    """
)

if st.button("▶️ Run Now: Discover & Append to Sheet"):
    run_once_and_append()

st.markdown("---")
st.subheader("View Entire Sheet Contents")

ws = get_worksheet()
if ws:
    try:
        data = ws.get_all_values()
        if data:
            df_sheet = pd.DataFrame(data[1:], columns=data[0])
            st.dataframe(df_sheet, height=600)
        else:
            st.info("Sheet is empty (no header or data).")
    except Exception as e:
        st.error(f"Could not read sheet contents: {e}")
else:
    st.error("Cannot connect to Google Sheet (check credentials).")
