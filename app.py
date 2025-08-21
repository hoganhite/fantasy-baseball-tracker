from forms import RegistrationForm, LoginForm, LinkLeagueForm, ContestForm, DeleteLeagueForm
from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta, date, timezone
import requests
import json
import urllib.parse
import os
from dotenv import load_dotenv
from espn_api.baseball import League
from flask_caching import Cache
import logging
from cryptography.fernet import Fernet, InvalidToken
import base64
from collections import defaultdict
import requests.exceptions
from sqlalchemy.exc import OperationalError
from time import sleep
from playwright.sync_api import sync_playwright
import io
import tempfile

# Load environment variables
load_dotenv()
YEAR = int(os.getenv('YEAR', datetime.now().year))
app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY')
if not app.secret_key:
    raise ValueError("No SECRET_KEY set in environment variables")

# Ensure instance path and cache directory exist
os.makedirs(app.instance_path, exist_ok=True)
cache_dir = os.path.join(app.instance_path, 'cache')
os.makedirs(cache_dir, exist_ok=True)

# Set up encryption key
key_path = os.path.join(app.instance_path, 'encryption_key.bin')
if os.getenv('ENCRYPTION_KEY'):
    encryption_key = base64.urlsafe_b64decode(os.getenv('ENCRYPTION_KEY'))
else:
    if os.path.exists(key_path):
        with open(key_path, 'rb') as f:
            encryption_key = f.read()
    else:
        encryption_key = os.urandom(32)
        with open(key_path, 'wb') as f:
            f.write(encryption_key)
ENCRYPTION_KEY = base64.urlsafe_b64encode(encryption_key)

# Configure database
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', f'sqlite:///{os.path.join(app.instance_path, "database.db")}')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_size': 5,
    'max_overflow': 10,
    'pool_timeout': 30,
    'pool_recycle': 1800,
    'pool_pre_ping': True
}
app.config['SESSION_COOKIE_SECURE'] = True
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
db = SQLAlchemy(app)

# Configure Flask-Caching
app.config['CACHE_TYPE'] = 'FileSystemCache'
app.config['CACHE_DIR'] = cache_dir
app.config['CACHE_DEFAULT_TIMEOUT'] = 86400
cache = Cache(app)

# Logging setup
logging.basicConfig(level=logging.DEBUG)
HEADERS = {'Connection': 'Keeping-alive', 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'}
MLB_BASE_URL = "https://statsapi.mlb.com/api/v1"

# Caches
mlb_id_cache = {}
game_log_cache = {}
processed_players = set()
team_names_cache = {}
roster_cache = {}

# Manual mappings
manual_mlb_mappings = {
    30820: 458681,  # Lance Lynn
    39832: 660271,  # Shohei Ohtani
    4917888: 686973,  # Louie Varland
    40934: 642557,  # Aaron Civale
    31864: 592836,  # Taijuan Walker
    32525: 593871,  # Jorge Polanco
    5134630: 684007  # Shota Imanaga
}

# Database models
class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    leagues = db.relationship('League', backref='user', lazy=True)
    contests = db.relationship('Contest', backref='user', lazy=True)

class League(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    name = db.Column(db.String(100))
    espn_league_id = db.Column(db.Integer, nullable=False)
    espn_s2 = db.Column(db.String(1024), nullable=False)
    swid = db.Column(db.String(1024), nullable=False)
    active_pitcher_slots = db.Column(db.Text, nullable=True)
    contests = db.relationship('Contest', backref='league', lazy=True)

    @property
    def espn_s2_decrypted(self):
        cipher = Fernet(ENCRYPTION_KEY)
        return cipher.decrypt(self.espn_s2.encode()).decode()

    def set_espn_s2(self, value):
        cipher = Fernet(ENCRYPTION_KEY)
        self.espn_s2 = cipher.encrypt(value.encode()).decode()

    @property
    def swid_decrypted(self):
        cipher = Fernet(ENCRYPTION_KEY)
        return cipher.decrypt(self.swid.encode()).decode()

    def set_swid(self, value):
        cipher = Fernet(ENCRYPTION_KEY)
        self.swid = cipher.encrypt(value.encode()).decode()

class Contest(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    league_id = db.Column(db.Integer, db.ForeignKey('league.id'), nullable=False, index=True)
    stat_category = db.Column(db.String(50), nullable=False)
    start_date = db.Column(db.String(10), nullable=False)
    end_date = db.Column(db.String(10), nullable=False)
    title = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    results = db.relationship('ContestResult', backref='contest', lazy=True)

class ContestResult(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    contest_id = db.Column(db.Integer, db.ForeignKey('contest.id'), nullable=False, index=True)
    rankings = db.Column(db.Text, nullable=False)
    chart_data = db.Column(db.Text, nullable=False)
    warning_message = db.Column(db.Text, nullable=True)
    status = db.Column(db.Text, nullable=False)
    last_updated = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))

class PlayerCache(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    espn_id = db.Column(db.Integer, nullable=False, unique=True, index=True)
    mlb_id = db.Column(db.Integer, nullable=True)
    player_name = db.Column(db.String(100), nullable=False)
    game_log = db.Column(db.Text, nullable=True)
    season = db.Column(db.Integer, nullable=False)
    group = db.Column(db.String(20), nullable=False)
    last_updated = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))

# Initialize database with retries
def init_db_with_retries(max_attempts=3, delay=5):
    attempts = 0
    while attempts < max_attempts:
        try:
            with app.app_context():
                db.create_all()
            logging.info("Database initialized successfully")
            return
        except OperationalError as e:
            attempts += 1
            logging.error(f"Database initialization attempt {attempts} failed: {str(e)}")
            if attempts < max_attempts:
                sleep(delay)
            else:
                logging.error("Failed to initialize database after max attempts")
                raise
init_db_with_retries()

# Flask-Login setup
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

@login_manager.user_loader
def load_user(user_id):
    max_attempts = 3
    attempts = 0
    while attempts < max_attempts:
        try:
            return db.session.get(User, int(user_id))
        except OperationalError as e:
            attempts += 1
            logging.error(f"Database error in load_user attempt {attempts}: {str(e)}")
            if attempts < max_attempts:
                sleep(2)
            db.session.rollback()
    return None

@cache.memoize(timeout=86400)
def get_mlb_id(player_name, player_id):
    logging.debug(f"Fetching MLB ID for player {player_name} (ESPN ID: {player_id})")
    if not player_name:
        return None
    if player_id in manual_mlb_mappings:
        logging.debug(f"Using manual mapping for player ID {player_id}: {manual_mlb_mappings[player_id]}")
        return manual_mlb_mappings[player_id]

    # Check database cache
    player_cache = PlayerCache.query.filter_by(espn_id=player_id, season=YEAR).first()
    if player_cache and player_cache.mlb_id:
        logging.debug(f"Database cache hit for MLB ID: {player_cache.mlb_id}")
        return player_cache.mlb_id

    # Fetch from API
    encoded_name = urllib.parse.quote(player_name)
    search_url = f"{MLB_BASE_URL}/people/search?names={encoded_name}&sportId=1&active=true"
    try:
        response = requests.get(search_url, timeout=5)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.debug(f"MLB API error for player {player_name} (ID: {player_id}): {str(e)}")
        return None
    data = response.json()
    logging.debug(f"Found {len(data.get('people', []))} active player matches for {player_name}")
    if data['people']:
        mlb_id = data['people'][0]['id']
        # Store in database
        max_attempts = 3
        attempts = 0
        while attempts < max_attempts:
            try:
                player_cache = PlayerCache.query.filter_by(espn_id=player_id, season=YEAR).first()
                if player_cache:
                    player_cache.mlb_id = mlb_id
                    player_cache.last_updated = datetime.now(timezone.utc)
                else:
                    player_cache = PlayerCache(
                        espn_id=player_id,
                        player_name=player_name,
                        mlb_id=mlb_id,
                        season=YEAR,
                        group='hitting'
                    )
                    db.session.add(player_cache)
                db.session.commit()
                logging.debug(f"Stored MLB ID {mlb_id} for player {player_name}")
                return mlb_id
            except OperationalError as e:
                attempts += 1
                logging.error(f"Database error storing MLB ID for {player_name}, attempt {attempts}: {str(e)}")
                db.session.rollback()
                if attempts < max_attempts:
                    sleep(2)
        logging.debug(f"Failed to store MLB ID for {player_name} after {max_attempts} attempts")
        return mlb_id
    logging.debug(f"No MLB ID found for player {player_name} (ESPN ID: {player_id})")
    return None

@cache.memoize(timeout=86400)
def get_team_names(league_id, cookies):
    logging.debug(f"Fetching team names for league {league_id}")
    if league_id in team_names_cache:
        logging.debug(f"Cache hit for team names for league {league_id}")
        return team_names_cache[league_id]
    base_url = f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/{YEAR}/segments/0/leagues/{league_id}"
    teams_url = f"{base_url}?view=mTeam"
    try:
        response = requests.get(teams_url, headers=HEADERS, cookies=cookies, timeout=5)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"API error fetching teams for league {league_id}: {str(e)}")
        raise ValueError(f"API error fetching teams: {str(e)}")
    teams_data = response.json()['teams']
    logging.debug("[TEAM_DATA] Raw team data: %s", teams_data)
    team_names = {}
    for t in teams_data:
        team_id = t['id']
        if t.get('name'):
            team_names[team_id] = t['name']
        else:
            location = t.get('location', '')
            nickname = t.get('nickname', '')
            team_names[team_id] = f"{location} {nickname}".strip() or f"Team {team_id}"
        logging.debug(f"[TEAM_DATA] Team ID {team_id}: name={team_names[team_id]}")
    team_names_cache[league_id] = team_names
    logging.debug(f"Stored team names for league {league_id}")
    return team_names

@cache.memoize(timeout=86400)
def get_team_rosters(league_id, cookies, start_date, end_date, season_start):
    logging.debug(f"Fetching rosters for league {league_id} from {start_date} to {end_date}")
    cache_key = f"rosters_{league_id}_{start_date}_{end_date}"
    if cache_key in roster_cache:
        logging.debug(f"Cache hit for rosters: {cache_key}")
        return roster_cache[cache_key]

    rosters = {}
    current = start_date
    while current <= end_date:
        scoring_period = (current - season_start).days + 1
        base_url = f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/{YEAR}/segments/0/leagues/{league_id}"
        roster_url = f"{base_url}?scoringPeriodId={scoring_period}&view=mRoster"
        try:
            response = requests.get(roster_url, headers=HEADERS, cookies=cookies, timeout=5)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.debug(f"API error fetching roster for date {current}: {str(e)}")
            rosters[current] = []
            current += timedelta(days=1)
            continue
        roster_data = response.json()['teams']
        rosters[current] = roster_data
        current += timedelta(days=1)

    roster_cache[cache_key] = rosters
    logging.debug(f"Stored rosters for {cache_key}")
    return rosters

def parse_ip(ip):
    try:
        ip_str = str(ip)
        if '.' not in ip_str:
            return float(ip_str)
        whole, frac = ip_str.split('.')
        return float(whole) + float(frac) / 3
    except Exception as e:
        logging.warning(f"Error parsing innings pitched '{ip_str}': {str(e)}")
        return 0.0

@cache.memoize(timeout=86400)
def compute_contest_stats(contest_id):
    logging.debug(f"Computing stats for contest {contest_id}")
    max_attempts = 3
    attempts = 0
    while attempts < max_attempts:
        try:
            contest = db.session.get(Contest, contest_id)
            if not contest:
                logging.error(f"Contest {contest_id} not found")
                raise ValueError("Contest not found.")
            league = contest.league
            if not league:
                logging.error(f"League not found for contest {contest_id}")
                raise ValueError("Linked league not found.")

            stat_category = contest.stat_category.upper()
            logging.debug(f"Using stat_category: {stat_category}")
            valid_stats = ['OBP', 'HR', 'RBI', 'AVG', 'HITS', 'RUNS SCORED', 'WALKS', 'STOLEN BASES', 'SLUGGING PERCENTAGE', 'INNINGS PITCHED', 'HITS ALLOWED', 'ERA', 'WALKS ALLOWED', 'STRIKEOUTS', 'QUALITY STARTS', 'WINS', 'SAVES', 'SAVES + HOLDS', 'WHIP', 'K/BB']
            if stat_category not in valid_stats:
                logging.error(f"Invalid stat_category: {stat_category}")
                raise ValueError(f"Invalid stat_category: {stat_category}. Choose from {', '.join(valid_stats)}.")

            hitting_categories = ['OBP', 'HR', 'RBI', 'AVG', 'HITS', 'RUNS SCORED', 'WALKS', 'STOLEN BASES', 'SLUGGING PERCENTAGE']
            pitching_categories = ['INNINGS PITCHED', 'HITS ALLOWED', 'ERA', 'WALKS ALLOWED', 'STRIKEOUTS', 'QUALITY STARTS', 'WINS', 'SAVES', 'SAVES + HOLDS', 'WHIP', 'K/BB']

            start_date = date.fromisoformat(contest.start_date)
            end_date = date.fromisoformat(contest.end_date)
            today = date.today()

            status = {}
            if start_date > today:
                status['is_started'] = False
                status['days_to_start'] = (start_date - today).days
                status['days_remaining'] = None
                status['is_complete'] = False
                status['winner'] = None
                rankings = []
                chart_data = {"labels": [], "datasets": [{"label": stat_category, "data": [], "backgroundColor": [], "borderColor": [], "borderWidth": 1}]}
                warning_message = "Contest not started yet."
                logging.debug(f"Contest {contest_id} not started, returning empty results")
                return rankings, chart_data, warning_message, status

            effective_end = min(end_date, today)
            no_data_days = []
            all_star_break = [date(2025, 7, 14), date(2025, 7, 15), date(2025, 7, 16), date(2025, 7, 17)]

            cookies = {'espn_s2': league.espn_s2_decrypted, 'swid': league.swid_decrypted}
            team_names = get_team_names(league.espn_league_id, cookies)

            team_stats = {team_name: {'num': 0.0, 'den': 0.0} if stat_category in ['OBP', 'AVG', 'SLUGGING PERCENTAGE', 'ERA', 'WHIP', 'K/BB'] else {'total': 0.0} for team_name in team_names.values()}
            season_start = date(YEAR, 3, 18)

            try:
                active_pitcher_slots = json.loads(league.active_pitcher_slots) if league.active_pitcher_slots else [13, 14, 15]
                active_pitcher_slots = [slot for slot in active_pitcher_slots if slot in [13, 14, 15]]
                if not active_pitcher_slots:
                    logging.warning(f"No valid pitcher slots found for league {league.espn_league_id}, using default [13, 14, 15]")
                    active_pitcher_slots = [13, 14, 15]
            except json.JSONDecodeError:
                logging.warning(f"Invalid active_pitcher_slots JSON for league {league.espn_league_id}, using default slots [13, 14, 15]")
                active_pitcher_slots = [13, 14, 15]
            logging.debug(f"Active pitcher slots for league {league.espn_league_id}: {active_pitcher_slots}")

            # Test additions
            is_june_hr_test = (stat_category == 'HR' and contest.start_date == '2025-06-01' and contest.end_date == '2025-06-30')
            hr_per_day = defaultdict(list) if is_june_hr_test else None
            is_march_rbi_test = (stat_category == 'RBI' and contest.start_date == '2025-03-18' and contest.end_date == '2025-03-31')
            is_april_rbi_test = (stat_category == 'RBI' and contest.start_date == '2025-04-01' and contest.end_date == '2025-04-30')
            is_may_rbi_test = (stat_category == 'RBI' and contest.start_date == '2025-05-01' and contest.end_date == '2025-05-31')
            is_june_rbi_test = (stat_category == 'RBI' and contest.start_date == '2025-06-01' and contest.end_date == '2025-06-30')
            is_july_rbi_test = (stat_category == 'RBI' and contest.start_date == '2025-07-01' and contest.end_date == '2025-07-31')
            rbi_per_day = defaultdict(list) if is_march_rbi_test or is_april_rbi_test or is_may_rbi_test or is_june_rbi_test or is_july_rbi_test else None
            is_july_ip_test = (stat_category == 'INNINGS PITCHED' and contest.start_date == '2025-07-01' and contest.end_date == '2025-07-31')
            ip_per_day = defaultdict(list) if is_july_ip_test else None

            # Process in weekly chunks
            chunk_size = 7
            current = start_date
            while current <= effective_end:
                chunk_end = min(current + timedelta(days=chunk_size - 1), effective_end)
                logging.debug(f"Processing chunk from {current} to {chunk_end}")
                rosters = get_team_rosters(league.espn_league_id, cookies, current, chunk_end, season_start)

                chunk_date = current
                while chunk_date <= chunk_end:
                    date_str = chunk_date.strftime('%Y-%m-%d')
                    logging.debug(f"Processing scoring period {(chunk_date - season_start).days + 1} for date {chunk_date}")
                    processed_players.clear()

                    roster_data = rosters.get(chunk_date, [])
                    if not roster_data:
                        if chunk_date not in all_star_break or stat_category not in pitching_categories:
                            no_data_days.append(chunk_date)
                        if is_july_ip_test:
                            ip_per_day[date_str] = []
                            logging.debug(f"No pitching stats for {date_str}, added empty IP entry for King Hoser")
                        chunk_date += timedelta(days=1)
                        continue

                    started_players = {}
                    daily_stats_found = False
                    for team in roster_data:
                        team_id = team['id']
                        started = []
                        all_players = []
                        for entry in team['roster']['entries']:
                            lineup_slot_id = entry['lineupSlotId']
                            player = entry.get('playerPoolEntry', {}).get('player', {})
                            player_name = player.get('fullName')
                            player_id = entry['playerId']
                            eligible_slots = player.get('eligibleSlots', [])
                            default_position_id = player.get('defaultPositionId', -1)
                            slot_status = "Active" if lineup_slot_id in active_pitcher_slots else "Bench" if lineup_slot_id == 16 else "Other"
                            logging.debug(f"Roster entry for team {team_names[team_id]}: player={player_name}, ID={player_id}, slot={lineup_slot_id}, status={slot_status}, eligible_slots={eligible_slots}, defaultPositionId={default_position_id}")
                            all_players.append((player_name, player_id, lineup_slot_id, slot_status))
                            if not player_name:
                                logging.debug(f"Skipping entry with no player name for team {team_names[team_id]}, ID={player_id}, slot={lineup_slot_id}")
                                continue
                            if stat_category in hitting_categories:
                                if lineup_slot_id <= 12:
                                    started.append((player_id, player_name, lineup_slot_id))
                            elif stat_category in pitching_categories:
                                if lineup_slot_id in active_pitcher_slots:
                                    started.append((player_id, player_name, lineup_slot_id))
                                else:
                                    logging.debug(f"Skipping {player_name} (ID: {player_id}) in slot {lineup_slot_id} as they are not in an active pitcher slot")
                                    continue
                            else:
                                logging.debug(f"Invalid stat_category for player {player_name}: {stat_category}")
                        started_players[team_id] = started
                        logging.debug(f"Team {team_names[team_id]} roster on {chunk_date}: {[(name, id, slot, status) for name, id, slot, status in all_players]}")
                        logging.debug(f"Team {team_names[team_id]} has {len(started)} started players")

                    group = 'hitting' if stat_category in hitting_categories else 'pitching'
                    for team_id, players in started_players.items():
                        for player_id, player_name, lineup_slot_id in players:
                            if player_id not in mlb_id_cache:
                                mlb_id = get_mlb_id(player_name, player_id)
                                if mlb_id:
                                    mlb_id_cache[player_id] = mlb_id
                                else:
                                    logging.warning(f"No MLB ID for player {player_name} (ESPN ID: {player_id})")
                                    continue
                            mlb_id = mlb_id_cache.get(player_id)
                            if not mlb_id:
                                logging.warning(f"Skipped player {player_name} (ESPN ID: {player_id}) for team {team_names[team_id]} on {date_str} due to no MLB ID")
                                continue
                            if mlb_id in processed_players:
                                continue
                            processed_players.add(mlb_id)

                            cache_key = f"game_log_{player_id}_{YEAR}_{group}"
                            player_cache = PlayerCache.query.filter_by(espn_id=player_id, season=YEAR, group=group).first()
                            if player_cache and player_cache.game_log:
                                game_log = json.loads(player_cache.game_log)
                                logging.debug(f"Database cache hit for game log: {cache_key}")
                            else:
                                game_log_url = f"{MLB_BASE_URL}/people/{mlb_id}/stats?stats=gameLog&season={YEAR}&group={group}"
                                try:
                                    game_log_response = requests.get(game_log_url, timeout=5)
                                    game_log_response.raise_for_status()
                                except requests.RequestException as e:
                                    logging.debug(f"MLB API error for player {player_name} (ID: {player_id}): {str(e)}")
                                    continue
                                game_log_data = game_log_response.json()
                                try:
                                    game_log = game_log_data['stats'][0]['splits']
                                    game_log_cache[cache_key] = game_log
                                    max_attempts_cache = max_attempts
                                    attempts_cache = 0
                                    while attempts_cache < max_attempts_cache:
                                        try:
                                            if player_cache:
                                                player_cache.game_log = json.dumps(game_log)
                                                player_cache.last_updated = datetime.now(timezone.utc)
                                            else:
                                                player_cache = PlayerCache(
                                                    espn_id=player_id,
                                                    player_name=player_name,
                                                    mlb_id=mlb_id,
                                                    season=YEAR,
                                                    group=group,
                                                    game_log=json.dumps(game_log)
                                                )
                                                db.session.add(player_cache)
                                            db.session.commit()
                                            logging.debug(f"Stored game log for player {player_name} (cache key: {cache_key})")
                                            break
                                        except OperationalError as e:
                                            attempts_cache += 1
                                            logging.error(f"Database error storing game log for {player_name}, attempt {attempts_cache}: {str(e)}")
                                            db.session.rollback()
                                            if attempts_cache < max_attempts_cache:
                                                sleep(2)
                                    if attempts_cache >= max_attempts_cache:
                                        logging.debug(f"Failed to store game log for {player_name} after {max_attempts_cache} attempts")
                                except (KeyError, IndexError) as e:
                                    logging.debug(f"No game log structure for player {player_name} (ESPN ID: {player_id}, MLB ID: {mlb_id})")
                                    continue
                            game_log = game_log_cache.get(cache_key, game_log)
                            daily_stats_list = [s['stat'] for s in game_log if s.get('date') == date_str]
                            if len(daily_stats_list) > 1:
                                logging.warning(f"Multiple game entries found for player {player_name} (MLB ID: {mlb_id}) on {date_str}: {len(daily_stats_list)} games")
                                logging.debug(f"Doubleheader detected for player {player_name} on {date_str}")
                            if not daily_stats_list:
                                logging.debug(f"No stats for player {player_name} on {date_str}")
                                continue
                            daily_stats_found = True

                            aggregated_stats = {}
                            for stat_dict in daily_stats_list:
                                for key, value in stat_dict.items():
                                    if key == 'inningsPitched':
                                        ip_value = parse_ip(value)
                                        aggregated_stats[key] = aggregated_stats.get(key, 0.0) + ip_value
                                        logging.debug(f"Parsed IP for {player_name} on {date_str}: raw={value}, parsed={ip_value}")
                                    else:
                                        try:
                                            aggregated_stats[key] = aggregated_stats.get(key, 0.0) + float(value)
                                        except (ValueError, TypeError):
                                            if key == 'homeRuns' and isinstance(value, str) and 'HR' in value:
                                                hr_count = 1 if 'HR' in value else 0
                                                aggregated_stats[key] = aggregated_stats.get(key, 0.0) + hr_count
                                                logging.debug(f"Parsed HR from string '{value}' for {player_name} on {date_str}: {hr_count}")
                                            elif key == 'rbi' and isinstance(value, str) and 'RBI' in value:
                                                rbi_count = 1 if 'RBI' in value else 0
                                                aggregated_stats[key] = aggregated_stats.get(key, 0.0) + rbi_count
                                                logging.debug(f"Parsed RBI from string '{value}' for {player_name} on {date_str}: {rbi_count}")
                                            else:
                                                logging.warning(f"Invalid stat value for {key}='{value}' for player {player_name} on {date_str}, skipping")
                                                aggregated_stats[key] = aggregated_stats.get(key, 0.0)

                            logging.debug(f"Aggregated daily stats for player ID {player_id} (MLB ID: {mlb_id}) on {date_str}: {aggregated_stats}")
                            if stat_category == 'OBP':
                                h = aggregated_stats.get('hits', 0)
                                bb = aggregated_stats.get('baseOnBalls', 0)
                                hbp = aggregated_stats.get('hitByPitch', 0)
                                ab = aggregated_stats.get('atBats', 0)
                                sf = aggregated_stats.get('sacFlies', 0)
                                pa = ab + bb + hbp + sf
                                if pa > 0:
                                    if h > ab:
                                        logging.warning(f"Invalid stats for player ID {player_id}: Hits ({h}) > At Bats ({ab})")
                                        continue
                                    team_stats[team_names[team_id]]['num'] += h + bb + hbp
                                    team_stats[team_names[team_id]]['den'] += pa
                            elif stat_category == 'AVG':
                                h = aggregated_stats.get('hits', 0)
                                bb = aggregated_stats.get('baseOnBalls', 0)
                                hbp = aggregated_stats.get('hitByPitch', 0)
                                ab = aggregated_stats.get('atBats', 0)
                                sf = aggregated_stats.get('sacFlies', 0)
                                pa = ab + bb + hbp + sf
                                if pa > 0:
                                    if h > ab:
                                        logging.warning(f"Invalid stats for player ID {player_id}: Hits ({h}) > At Bats ({ab})")
                                        continue
                                    team_stats[team_names[team_id]]['num'] += h
                                    team_stats[team_names[team_id]]['den'] += ab
                            elif stat_category == 'HR':
                                hr = aggregated_stats.get('homeRuns', 0)
                                team_stats[team_names[team_id]]['total'] += hr
                                if is_june_hr_test and team_names[team_id] == "B. Hackenburg" and hr > 0:
                                    hr_per_day[date_str].append((player_name, hr))
                                    logging.debug(f"Adding {hr} HR for player {player_name} (ESPN ID: {player_id}, MLB ID: {mlb_id}) on {date_str} to team B. Hackenburg")
                            elif stat_category == 'RBI':
                                rbi = aggregated_stats.get('rbi', 0)
                                team_stats[team_names[team_id]]['total'] += rbi
                                if (is_march_rbi_test or is_april_rbi_test or is_may_rbi_test or is_june_rbi_test or is_july_rbi_test) and team_names[team_id] == "B. Hackenburg" and rbi > 0:
                                    rbi_per_day[date_str].append((player_name, rbi))
                                    logging.debug(f"Adding {rbi} RBI for player {player_name} (ESPN ID: {player_id}, MLB ID: {mlb_id}) on {date_str} to team B. Hackenburg")
                            elif stat_category == 'HITS':
                                team_stats[team_names[team_id]]['total'] += aggregated_stats.get('hits', 0)
                            elif stat_category == 'RUNS SCORED':
                                team_stats[team_names[team_id]]['total'] += aggregated_stats.get('runs', 0)
                            elif stat_category == 'WALKS':
                                team_stats[team_names[team_id]]['total'] += aggregated_stats.get('baseOnBalls', 0)
                            elif stat_category == 'STOLEN BASES':
                                team_stats[team_names[team_id]]['total'] += aggregated_stats.get('stolenBases', 0)
                            elif stat_category == 'SLUGGING PERCENTAGE':
                                total_bases = aggregated_stats.get('totalBases', 0)
                                bb = aggregated_stats.get('baseOnBalls', 0)
                                hbp = aggregated_stats.get('hitByPitch', 0)
                                ab = aggregated_stats.get('atBats', 0)
                                sf = aggregated_stats.get('sacFlies', 0)
                                pa = ab + bb + hbp + sf
                                if pa > 0:
                                    if total_bases > 4 * ab:
                                        logging.warning(f"Invalid stats for player ID {player_id}: Total Bases ({total_bases}) > 4 * At Bats ({ab})")
                                        continue
                                    team_stats[team_names[team_id]]['num'] += total_bases
                                    team_stats[team_names[team_id]]['den'] += ab
                            elif stat_category == 'INNINGS PITCHED':
                                ip = aggregated_stats.get('inningsPitched', 0)
                                if lineup_slot_id not in active_pitcher_slots:
                                    logging.warning(f"Player {player_name} (ID: {player_id}) in slot {lineup_slot_id} is not active but has {ip} IP on {date_str}, skipping")
                                    continue
                                team_stats[team_names[team_id]]['total'] += ip
                                if is_july_ip_test and team_names[team_id] == "King Hoser" and ip > 0:
                                    ip_per_day[date_str].append((player_name, ip, lineup_slot_id))
                                    logging.debug(f"Adding {ip} IP for player {player_name} (ESPN ID: {player_id}, MLB ID: {mlb_id}) on {date_str} to team King Hoser in slot {lineup_slot_id}")
                            elif stat_category == 'HITS ALLOWED':
                                team_stats[team_names[team_id]]['total'] += aggregated_stats.get('hits', 0)
                            elif stat_category == 'ERA':
                                er = aggregated_stats.get('earnedRuns', 0)
                                ip = aggregated_stats.get('inningsPitched', 0)
                                if ip > 0:
                                    if er < 0:
                                        logging.warning(f"Invalid stats for player ID {player_id}: Earned Runs ({er}) < 0")
                                        continue
                                    team_stats[team_names[team_id]]['num'] += er * 9
                                    team_stats[team_names[team_id]]['den'] += ip
                            elif stat_category == 'WALKS ALLOWED':
                                team_stats[team_names[team_id]]['total'] += aggregated_stats.get('baseOnBalls', 0)
                            elif stat_category == 'STRIKEOUTS':
                                team_stats[team_names[team_id]]['total'] += aggregated_stats.get('strikeOuts', 0)
                            elif stat_category == 'QUALITY STARTS':
                                team_stats[team_names[team_id]]['total'] += aggregated_stats.get('qualityStarts', 0)
                            elif stat_category == 'WINS':
                                team_stats[team_names[team_id]]['total'] += aggregated_stats.get('wins', 0)
                            elif stat_category == 'SAVES':
                                team_stats[team_names[team_id]]['total'] += aggregated_stats.get('saves', 0)
                            elif stat_category == 'SAVES + HOLDS':
                                team_stats[team_names[team_id]]['total'] += aggregated_stats.get('saves', 0) + aggregated_stats.get('holds', 0)
                            elif stat_category == 'WHIP':
                                hits = aggregated_stats.get('hits', 0)
                                bb = aggregated_stats.get('baseOnBalls', 0)
                                ip = aggregated_stats.get('inningsPitched', 0)
                                if ip > 0:
                                    if hits < 0 or bb < 0:
                                        logging.warning(f"Invalid stats for player ID {player_id}: Hits ({hits}) or Walks ({bb}) < 0")
                                        continue
                                    team_stats[team_names[team_id]]['num'] += hits + bb
                                    team_stats[team_names[team_id]]['den'] += ip
                            elif stat_category == 'K/BB':
                                k = aggregated_stats.get('strikeOuts', 0)
                                bb = aggregated_stats.get('baseOnBalls', 0)
                                team_stats[team_names[team_id]]['num'] += k
                                team_stats[team_names[team_id]]['den'] += bb
                    if not daily_stats_found and stat_category in pitching_categories and chunk_date not in all_star_break:
                        no_data_days.append(chunk_date)
                        if is_july_ip_test:
                            ip_per_day[date_str] = []
                            logging.debug(f"No pitching stats for {date_str}, added empty IP entry for King Hoser")
                    chunk_date += timedelta(days=1)
                current = chunk_end + timedelta(days=1)

            # Test additions logging
            if is_june_hr_test:
                logging.info("June HR Test Results for Team B. Hackenburg (Daily Breakdown):")
                total_hr = 0
                for day in sorted(hr_per_day.keys()):
                    daily_hr = hr_per_day[day]
                    if daily_hr:
                        daily_total = sum(hr for _, hr in daily_hr)
                        total_hr += daily_total
                        player_str = ", ".join(f"{player}: {int(hr)}" for player, hr in sorted(daily_hr))
                        logging.info(f"Date {day}: Total HR {int(daily_total)}, Players: {player_str}")
                    else:
                        logging.info(f"Date {day}: Total HR 0, No HRs hit")
                logging.info(f"Overall Total HR for B. Hackenburg in June: {int(total_hr)}")

            if is_march_rbi_test or is_april_rbi_test or is_may_rbi_test or is_june_rbi_test or is_july_rbi_test:
                month = "March" if is_march_rbi_test else "April" if is_april_rbi_test else "May" if is_may_rbi_test else "June" if is_june_rbi_test else "July"
                logging.info(f"{month} RBI Test Results for Team B. Hackenburg (Daily Breakdown):")
                total_rbi = 0
                for day in sorted(rbi_per_day.keys()):
                    daily_rbi = rbi_per_day[day]
                    if daily_rbi:
                        daily_total = sum(rbi for _, rbi in daily_rbi)
                        total_rbi += daily_total
                        player_str = ", ".join(f"{player}: {int(rbi)}" for player, rbi in sorted(daily_rbi))
                        logging.info(f"Date {day}: Total RBI {int(daily_total)}, Players: {player_str}")
                    else:
                        logging.info(f"Date {day}: Total RBI 0, No RBIs")
                logging.info(f"Overall Total RBI for B. Hackenburg in {month}: {int(total_rbi)}")

            if is_july_ip_test:
                logging.info("July IP Test Results for King Hoser (Daily Breakdown):")
                total_ip = 0.0
                current = start_date
                while current <= end_date:
                    day_str = current.strftime('%Y-%m-%d')
                    daily_ip = ip_per_day.get(day_str, [])
                    if daily_ip:
                        daily_total = sum(ip for _, ip, _ in daily_ip)
                        total_ip += daily_total
                        player_str = ", ".join(f"{player}: {format_stat(ip, 'INNINGS PITCHED')} (slot {slot})" for player, ip, slot in sorted(daily_ip))
                        logging.info(f"Date {day_str}: Total IP {format_stat(daily_total, 'INNINGS PITCHED')}, Players: {player_str}")
                    else:
                        logging.info(f"Date {day_str}: Total IP 0.0, No IP")
                    current += timedelta(days=1)
                logging.info(f"Overall Total IP for King Hoser in July: {format_stat(total_ip, 'INNINGS PITCHED')}")

            logging.debug(f"Team {stat_category} components:")
            for team_name, data in team_stats.items():
                if 'num' in data and 'den' in data:
                    value = data['num'] / data['den'] if data['den'] > 0 else 999.0 if data['num'] > 0 else 0.0
                    logging.debug(f"Team {team_name}: num={data['num']}, den={data['den']}, {stat_category}={value:.4f}")
                else:
                    logging.debug(f"Team {team_name}: {stat_category}={data['total']}")

            rankings = []
            for team_name, data in team_stats.items():
                if 'num' in data and 'den' in data:
                    value = data['num'] / data['den'] if data['den'] > 0 else 999.0 if data['num'] > 0 else 0.0
                else:
                    value = data['total']
                rankings.append((team_name, value))

            lower_is_better = ['HITS ALLOWED', 'ERA', 'WALKS ALLOWED', 'WHIP']
            rankings.sort(key=lambda x: x[1], reverse=(stat_category not in lower_is_better))

            warning_message = ""
            if no_data_days:
                warning_message = f"Warning: No pitching stats found for {len(no_data_days)} day(s): {', '.join(str(d) for d in no_data_days)}. Try a different date range."

            chart_data = {
                "labels": [team for team, _ in rankings],
                "datasets": [{
                    "label": stat_category,
                    "data": [value for _, value in rankings],
                    "backgroundColor": ["#4CAF50", "#2196F3", "#FF9800", "#F44336", "#9C27B0", "#3F51B5", "#FFEB3B", "#009688", "#E91E63", "#607D8B", "#FFC107", "#795548"],
                    "borderColor": ["#388E3C", "#1976D2", "#F57C00", "#D32F2F", "#7B1FA2", "#303F9F", "#FBC02D", "#00796B", "#C2185B", "#455A64", "#FFB300", "#5D4037"],
                    "borderWidth": 1
                }]
            }

            status['is_started'] = True
            if end_date > today:
                status['is_complete'] = False
                status['days_remaining'] = (end_date - today).days
                status['winner'] = None
            else:
                status['is_complete'] = True
                status['days_remaining'] = None
                if rankings:
                    top_score = rankings[0][1]
                    winners = [team for team, value in rankings if value == top_score]
                    status['winner'] = winners
                    logging.debug(f"Contest {contest_id} winners: {winners}")
                else:
                    status['winner'] = []
                    logging.debug(f"Contest {contest_id} has no rankings data")

            logging.debug(f"Computed stats for contest {contest_id}: {len(rankings)} teams, status={status}")
            return rankings, chart_data, warning_message, status

        except OperationalError as e:
            attempts += 1
            logging.error(f"Database error in compute_contest_stats attempt {attempts}: {str(e)}")
            db.session.rollback()
            if attempts < max_attempts:
                sleep(2)
            else:
                raise ValueError(f"Database error after {max_attempts} attempts: {str(e)}")
        except Exception as e:
            logging.error(f"Error computing stats for contest {contest_id}: {str(e)}")
            raise ValueError(f"Error computing contest stats: {str(e)}")

def get_contest_data(contest_id):
    logging.debug(f"Getting contest data for contest {contest_id}")
    max_attempts = 3
    attempts = 0
    while attempts < max_attempts:
        try:
            contest = db.session.get(Contest, contest_id)
            if not contest:
                logging.error(f"Contest {contest_id} not found in database")
                raise ValueError("Contest not found.")

            result = ContestResult.query.filter_by(contest_id=contest_id).order_by(ContestResult.last_updated.desc()).first()
            today = date.today()
            end_date = date.fromisoformat(contest.end_date)
            needs_update = not result or (result.last_updated.date() < today and end_date >= today)

            if not needs_update:
                logging.debug(f"Using stored ContestResult for contest {contest_id}, last updated {result.last_updated}")
                try:
                    return json.loads(result.rankings), json.loads(result.chart_data), result.warning_message, json.loads(result.status)
                except json.JSONDecodeError as e:
                    logging.error(f"Error decoding JSON for contest {contest_id}: {str(e)}")
                    needs_update = True

            logging.debug(f"Computing new stats for contest {contest_id}, needs_update={needs_update}")
            rankings, chart_data, warning_message, status = compute_contest_stats(contest_id)

            new_result = ContestResult(
                contest_id=contest_id,
                rankings=json.dumps(rankings),
                chart_data=json.dumps(chart_data),
                warning_message=warning_message,
                status=json.dumps(status),
                last_updated=datetime.now(timezone.utc)
            )
            db.session.add(new_result)
            db.session.commit()
            logging.debug(f"Saved new ContestResult for contest {contest_id}")

            return rankings, chart_data, warning_message, status

        except OperationalError as e:
            attempts += 1
            logging.error(f"Database error in get_contest_data attempt {attempts}: {str(e)}")
            db.session.rollback()
            if attempts < max_attempts:
                sleep(2)
        except Exception as e:
            logging.error(f"Error computing stats for contest {contest_id}: {str(e)}")
            raise ValueError(f"Error computing contest stats: {str(e)}")
    raise ValueError(f"Database error after {max_attempts} attempts")

@app.route('/')
def home():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return "Hello, Fantasy Baseball Tracker! Go to /login or /create-contest."

@app.route('/register', methods=['GET', 'POST'])
def register():
    form = RegistrationForm()
    if form.validate_on_submit():
        max_attempts = 3
        attempts = 0
        while attempts < max_attempts:
            try:
                existing_user = User.query.filter_by(username=form.username.data).first()
                if existing_user:
                    flash("Username already exists. Please choose another.")
                    return render_template('register.html', form=form)
                hashed_password = generate_password_hash(form.password.data, method='scrypt')
                new_user = User(username=form.username.data, email=form.email.data, password_hash=hashed_password)
                db.session.add(new_user)
                db.session.commit()
                login_user(new_user)
                return redirect(url_for('link_league'))
            except OperationalError as e:
                attempts += 1
                logging.error(f"Database error during registration attempt {attempts}: {str(e)}")
                db.session.rollback()
                if attempts < max_attempts:
                    sleep(2)
        flash("Error creating user due to database issues. Please try again later.", "error")
        return render_template('register.html', form=form)
    return render_template('register.html', form=form)

@app.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        max_attempts = 3
        attempts = 0
        while attempts < max_attempts:
            try:
                user = User.query.filter_by(username=form.username.data).first()
                if user and check_password_hash(user.password_hash, form.password.data):
                    login_user(user)
                    if not user.leagues:
                        return redirect(url_for('link_league'))
                    return redirect(url_for('dashboard'))
                flash("Invalid credentials. Try again.")
                return render_template('login.html', form=form)
            except OperationalError as e:
                attempts += 1
                logging.error(f"Database error during login attempt {attempts}: {str(e)}")
                db.session.rollback()
                if attempts < max_attempts:
                    sleep(2)
        flash("Error logging in due to database issues. Please try again later.", "error")
        return render_template('login.html', form=form)
    return render_template('login.html', form=form)

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/link-league', methods=['GET', 'POST'])
@login_required
def link_league():
    form = LinkLeagueForm()
    if form.validate_on_submit():
        espn_league_id = form.league_id.data
        espn_s2 = form.espn_s2.data
        swid = form.swid.data
        cookies = {'espn_s2': espn_s2, 'swid': swid}
        settings_url = f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/{YEAR}/segments/0/leagues/{espn_league_id}?view=mSettings"
        try:
            response = requests.get(settings_url, headers=HEADERS, cookies=cookies, timeout=5)
            response.raise_for_status()
        except requests.RequestException:
            flash("Invalid league details or credentials. Please check and try again.")
            return render_template('link_league.html', form=form)
        data = response.json()
        league_name = data.get('settings', {}).get('name', f'League {espn_league_id}')
        lineupSlotCounts = data.get('settings', {}).get('rosterSettings', {}).get('lineupSlotCounts', {})
        logging.debug(f"Raw lineupSlotCounts for league {espn_league_id}: {lineupSlotCounts}")
        active_pitcher_slots = [int(k) for k, v in lineupSlotCounts.items() if 13 <= int(k) <= 15 and v > 0]
        if not active_pitcher_slots:
            logging.warning(f"No valid pitcher slots found in lineupSlotCounts for league {espn_league_id}, using default [13, 14, 15]")
            active_pitcher_slots = [13, 14, 15]
        logging.debug(f"Active pitcher slots for league {espn_league_id}: {active_pitcher_slots}")
        new_league = League(
            user_id=current_user.id,
            name=league_name,
            espn_league_id=espn_league_id,
            espn_s2='',
            swid='',
            active_pitcher_slots=json.dumps(active_pitcher_slots)
        )
        new_league.set_espn_s2(espn_s2)
        new_league.set_swid(swid)
        db.session.add(new_league)
        max_attempts = 3
        attempts = 0
        while attempts < max_attempts:
            try:
                db.session.commit()
                return redirect(url_for('dashboard'))
            except OperationalError as e:
                attempts += 1
                logging.error(f"Database error during league linking attempt {attempts}: {str(e)}")
                db.session.rollback()
                if attempts < max_attempts:
                    sleep(2)
        flash("Error linking league due to database issues. Please try again later.", "error")
        return render_template('link_league.html', form=form)
    return render_template('link_league.html', form=form)

@app.route('/create-contest', methods=['GET', 'POST'])
@login_required
def create_contest():
    if not current_user.leagues:
        return redirect(url_for('link_league'))
    form = ContestForm()
    form.league_id.choices = [(l.id, l.name) for l in current_user.leagues]
    if form.validate_on_submit():
        if form.start_date.data >= form.end_date.data:
            flash("Start date must be before end date.")
            return render_template('create_contest.html', form=form, current_date=date.today().strftime('%Y-%m-%d'), start_of_month=date.today().replace(day=1).strftime('%Y-%m-%d'), leagues=current_user.leagues)
        contest = Contest(
            user_id=current_user.id,
            league_id=form.league_id.data,
            stat_category=form.stat_category.data,
            start_date=form.start_date.data.strftime('%Y-%m-%d'),
            end_date=form.end_date.data.strftime('%Y-%m-%d'),
            title=form.title.data
        )
        db.session.add(contest)
        max_attempts = 3
        attempts = 0
        while attempts < max_attempts:
            try:
                db.session.commit()
                break
            except OperationalError as e:
                attempts += 1
                logging.error(f"Database error during contest creation attempt {attempts}: {str(e)}")
                db.session.rollback()
                if attempts < max_attempts:
                    sleep(2)
        if attempts >= max_attempts:
            flash("Error creating contest due to database issues. Please try again later.", "error")
            return render_template('create_contest.html', form=form, current_date=date.today().strftime('%Y-%m-%d'), start_of_month=date.today().replace(day=1).strftime('%Y-%m-%d'), leagues=current_user.leagues)

        start_date = date.fromisoformat(contest.start_date)
        if start_date <= date.today():
            max_attempts = 3
            attempts = 0
            while attempts < max_attempts:
                try:
                    rankings, chart_data, warning_message, status = compute_contest_stats(contest.id)
                    new_result = ContestResult(
                        contest_id=contest.id,
                        rankings=json.dumps(rankings),
                        chart_data=json.dumps(chart_data),
                        warning_message=warning_message,
                        status=json.dumps(status),
                        last_updated=datetime.now(timezone.utc)
                    )
                    db.session.add(new_result)
                    db.session.commit()
                    logging.debug(f"Stored initial ContestResult for new contest {contest.id}")
                    break
                except (ValueError, OperationalError) as e:
                    attempts += 1
                    logging.error(f"Error during contest stats computation attempt {attempts}: {str(e)}")
                    db.session.rollback()
                    if attempts < max_attempts:
                        sleep(2)
            if attempts >= max_attempts:
                flash("Error computing contest stats due to database or API issues. Contest created but results not available.", "error")

        return redirect(url_for('results', contest_id=contest.id))
    today = date.today()
    return render_template('create_contest.html', form=form, current_date=today.strftime('%Y-%m-%d'), start_of_month=date.today().replace(day=1).strftime('%Y-%m-%d'), leagues=current_user.leagues)

@app.route('/dashboard', methods=['GET', 'POST'])
@login_required
def dashboard():
    if not current_user.leagues:
        return redirect(url_for('link_league'))
    contests = Contest.query.filter_by(user_id=current_user.id).order_by(Contest.created_at.desc()).all()
    contest_data = []
    for contest in contests:
        try:
            rankings, chart_data, warning_message, status = get_contest_data(contest.id)
            contest_data.append({
                'contest': contest,
                'chart_data': chart_data,
                'warning_message': warning_message,
                'status': status
            })
        except InvalidToken:
            flash("Encryption key mismatch detected. Clearing old leagues and please link again.", "error")
            return redirect(url_for('clear_leagues'))
        except ValueError as e:
            flash(str(e), "error")
            continue
    return render_template('dashboard.html', contest_data=contest_data)

@app.route('/results/<int:contest_id>')
@login_required
def results(contest_id):
    contest = db.session.get(Contest, contest_id)
    if not contest or contest.user_id != current_user.id:
        return "Contest not found or you don't have access."

    try:
        rankings, chart_data, warning_message, status = get_contest_data(contest_id)
    except InvalidToken:
        flash("Encryption key mismatch detected. Clearing old leagues and please link again.", "error")
        return redirect(url_for('clear_leagues'))
    except ValueError as e:
        return str(e)

    return render_template('results.html', rankings=rankings, stat_category=contest.stat_category, contest=contest, chart_data=chart_data, warning_message=warning_message, status=status)

@app.route('/download_snapshot/<int:contest_id>')
@login_required
def download_snapshot(contest_id):
    contest = db.session.get(Contest, contest_id)
    if not contest or contest.user_id != current_user.id:
        flash("Contest not found or you don't have access.", "error")
        return redirect(url_for('dashboard'))

    try:
        rankings, chart_data, warning_message, status = get_contest_data(contest_id)
    except InvalidToken:
        flash("Encryption key mismatch detected. Clearing old leagues and please link again.", "error")
        return redirect(url_for('clear_leagues'))
    except ValueError as e:
        flash(str(e), "error")
        return redirect(url_for('dashboard'))

    # Generate screenshot using Playwright
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={'width': 1280, 'height': 720},
                device_scale_factor=2
            )
            page = context.new_page()

            # Set cookies to maintain session
            session_cookie = {
                'name': 'session',
                'value': request.cookies.get('session'),
                'domain': request.host,  # Use the current host (localhost locally, Render domain on server)
                'path': '/',
                'secure': False
            }
            context.add_cookies([session_cookie])
            logging.debug(f"Set session cookie: {session_cookie}")

            # Navigate to the results page
            results_url = url_for('results', contest_id=contest_id, _external=True)
            logging.debug(f"Navigating to results URL: {results_url}")
            page.goto(results_url)

            # Check if contest has started
            if not status['is_started']:
                logging.warning("Contest not started, no snapshot area available")
                flash("Cannot generate snapshot: Contest has not started yet.", "error")
                browser.close()
                return redirect(url_for('results', contest_id=contest_id))

            # Wait for the snapshot area with a longer timeout
            try:
                page.wait_for_selector('#snapshot-area', timeout=60000)  # Increased to 60 seconds
                page.wait_for_selector('#rankingsChart', timeout=60000)
                page.wait_for_timeout(2000)  # Extra wait for chart rendering
            except Exception as e:
                logging.warning(f"Chart loading warning: {str(e)}. Proceeding with screenshot.")

            # Debug: Capture full page screenshot
            debug_path = os.path.join(os.path.dirname(__file__), 'debug_full_page.png')
            page.screenshot(path=debug_path)
            logging.debug(f"Saved debug screenshot to {debug_path}")

            # Capture the snapshot-area div
            snapshot_area = page.locator('#snapshot-area')
            if not snapshot_area.is_visible():
                logging.error("Snapshot area not visible on the page")
                flash("Error generating snapshot: Content area not visible.", "error")
                browser.close()
                return redirect(url_for('results', contest_id=contest_id))

            # Take screenshot
            screenshot_bytes = snapshot_area.screenshot()

            browser.close()

            # Serve the screenshot
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
            temp_file.write(screenshot_bytes)
            temp_file.close()

            filename = f"contest_{contest_id}_results.png"
            return send_file(
                temp_file.name,
                mimetype='image/png',
                as_attachment=True,
                download_name=filename
            )

        except Exception as e:
            logging.error(f"Error generating screenshot for contest {contest_id}: {str(e)}")
            flash("Error generating snapshot. Please try again later.", "error")
            if 'browser' in locals():
                browser.close()
            return redirect(url_for('results', contest_id=contest_id))

@app.route('/delete-contest/<int:contest_id>', methods=['POST'])
@login_required
def delete_contest(contest_id):
    contest = db.session.get(Contest, contest_id)
    if contest and contest.user_id == current_user.id:
        ContestResult.query.filter_by(contest_id=contest_id).delete()
        db.session.delete(contest)
        max_attempts = 3
        attempts = 0
        while attempts < max_attempts:
            try:
                db.session.commit()
                cache.delete_memoized(compute_contest_stats, contest_id)
                flash("Contest deleted successfully.", "success")
                return redirect(url_for('dashboard'))
            except OperationalError as e:
                attempts += 1
                logging.error(f"Database error during contest deletion attempt {attempts}: {str(e)}")
                db.session.rollback()
                if attempts < max_attempts:
                    sleep(2)
        flash("Error deleting contest due to database issues. Please try again later.", "error")
    else:
        flash("Contest not found or you don't have permission to delete it.", "error")
    return redirect(url_for('dashboard'))

@app.route('/clear-contests')
@login_required
def clear_contests():
    ContestResult.query.filter(ContestResult.contest_id.in_([c.id for c in current_user.contests])).delete()
    Contest.query.filter_by(user_id=current_user.id).delete()
    max_attempts = 3
    attempts = 0
    while attempts < max_attempts:
        try:
            db.session.commit()
            cache.clear()
            flash("All contests cleared successfully.", "success")
            return redirect(url_for('dashboard'))
        except OperationalError as e:
            attempts += 1
            logging.error(f"Database error during contests clearing attempt {attempts}: {str(e)}")
            db.session.rollback()
            if attempts < max_attempts:
                sleep(2)
    flash("Error clearing contests due to database issues. Please try again later.", "error")
    return redirect(url_for('dashboard'))

@app.route('/my-leagues', methods=['GET', 'POST'])
@login_required
def my_leagues():
    leagues = current_user.leagues
    forms = [DeleteLeagueForm(prefix=str(league.id), league_id=league.id) for league in leagues]
    if request.method == 'POST':
        logging.debug(f"Received POST request to /my-leagues with form data: {request.form}")
        submitted_prefix = None
        for key in request.form.keys():
            if key.endswith('-league_id'):
                submitted_prefix = key.split('-')[0]
                break

        if not submitted_prefix:
            logging.warning("No league_id field found in form data")
            flash("No league selected for deletion.", "error")
            return redirect(url_for('my_leagues'))

        league_id_values = request.form.getlist(f"{submitted_prefix}-league_id")
        if not league_id_values:
            logging.warning(f"No league_id value provided for prefix {submitted_prefix}")
            flash("No league selected for deletion.", "error")
            return redirect(url_for('my_leagues'))
        submitted_league_id = league_id_values[0]

        for form in forms:
            logging.debug(f"Checking form with prefix: {form._prefix}, submitted_prefix: {submitted_prefix}")
            if form._prefix.rstrip('-') == submitted_prefix:
                form.process(formdata=request.form)
                if form.validate():
                    logging.debug(f"Form validated successfully, league_id: {form.league_id.data}")
                    max_attempts = 3
                    attempts = 0
                    while attempts < max_attempts:
                        try:
                            league_id = int(form.league_id.data)
                            league = db.session.get(League, league_id)
                            if league and league.user_id == current_user.id:
                                logging.debug(f"Found league {league_id} for user {current_user.id}, deleting...")
                                ContestResult.query.filter(ContestResult.contest_id.in_([c.id for c in league.contests])).delete()
                                Contest.query.filter_by(league_id=league.id).delete()
                                db.session.delete(league)
                                db.session.commit()
                                cache.clear()
                                flash("League deleted successfully.", "success")
                                logging.info(f"Successfully deleted league {league_id}")
                                return redirect(url_for('my_leagues'))
                            else:
                                logging.warning(f"League {league_id} not found or user {current_user.id} lacks permission")
                                flash("League not found or you don't have permission to delete it.", "error")
                                return redirect(url_for('my_leagues'))
                        except OperationalError as e:
                            attempts += 1
                            logging.error(f"Database error during league deletion attempt {attempts}: {str(e)}")
                            db.session.rollback()
                            if attempts < max_attempts:
                                sleep(2)
                    flash("Error deleting league due to database issues. Please try again later.", "error")
                    return redirect(url_for('my_leagues'))
                else:
                    logging.warning(f"Form validation failed for league_id {form.league_id.data}: {form.errors}")
                    flash(f"Form validation failed: {form.errors}", "error")
                    return redirect(url_for('my_leagues'))

        logging.warning(f"No form matched submitted prefix {submitted_prefix}")
        flash("Invalid league selection.", "error")
        return redirect(url_for('my_leagues'))

    leagues_forms = list(zip(leagues, forms))
    logging.debug(f"Rendering my_leagues.html with {len(leagues)} leagues")
    return render_template('my_leagues.html', leagues_forms=leagues_forms)

@app.template_filter('format_stat')
def format_stat(value, category):
    if category in ['OBP', 'AVG', 'SLUGGING PERCENTAGE', 'ERA', 'WHIP', 'K/BB']:
        return f"{value:.4f}"
    elif category == 'INNINGS PITCHED':
        total_outs = round(value * 3)
        whole = total_outs // 3
        frac = total_outs % 3
        return f"{whole}.{frac}"
    else:
        return f"{int(value)}"

@app.route('/clear-leagues')
@login_required
def clear_leagues():
    max_attempts = 3
    attempts = 0
    while attempts < max_attempts:
        try:
            ContestResult.query.filter(ContestResult.contest_id.in_([c.id for c in current_user.contests])).delete()
            Contest.query.filter_by(user_id=current_user.id).delete()
            League.query.filter_by(user_id=current_user.id).delete()
            db.session.commit()
            cache.clear()
            flash("All leagues cleared successfully. Please link your leagues again.", "success")
            return redirect(url_for('link_league'))
        except OperationalError as e:
            attempts += 1
            logging.error(f"Database error during leagues clearing attempt {attempts}: {str(e)}")
            db.session.rollback()
            if attempts < max_attempts:
                sleep(2)
    flash("Error clearing leagues due to database issues. Please try again later.", "error")
    return redirect(url_for('link_league'))

if __name__ == '__main__':
    app.run()