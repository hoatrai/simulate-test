#!/usr/bin/env python3
"""
simulate.py — Giả lập user thật (join kèo, contextual chat, finding-keo)
để test app "nhau" bằng cách gọi thẳng REST API (giống app Flutter gọi),
nên sẽ kích hoạt luôn cả Phoenix socket / notification giống thật.

Cách dùng:
    1. Copy config.example.json -> config.json, điền accounts/product_ids thật.
    2. pip install requests
    3. python3 simulate.py --config config.json

KIẾN TRÚC: chạy ĐA LUỒNG — nhiều "worker" độc lập cùng hoạt động song song
(mỗi worker tự chọn user + action + nghỉ theo kiểu người thật của riêng nó),
giống nhiều người dùng thật cùng online một lúc, thay vì 1 vòng lặp tuần tự
chỉ có 1 hành động tại 1 thời điểm. Số luồng chỉnh qua config["concurrent_workers"].
State dùng chung (open_invites, invite_members...) được bảo vệ bằng 1 Lock;
lock KHÔNG bao giờ bị giữ trong lúc gọi API (network I/O) để tránh việc giữ
lock làm nghẽn ngược lại thành chạy tuần tự.

An toàn khi chạy nhiều lần: script không xoá dữ liệu, chỉ tạo/join/chat thêm.
CHỈ chạy trên site TEST, không trỏ base_url sang production.
"""

import json
import math
import random
import sys
import time
import argparse
import logging
import signal
import threading
import itertools
from datetime import datetime, timedelta

import requests

# 🆕 MAP PRESENCE: dùng để giữ WebSocket sống cho tính năng "hiện user
# xung quanh trên map" (channel online_users:lobby bên Phoenix). Optional —
# nếu chưa cài `pip install websocket-client` thì các action REST cũ vẫn
# chạy bình thường, chỉ mỗi tính năng map presence tự tắt + log cảnh báo.
try:
    import websocket  # pip install websocket-client
    HAS_WEBSOCKET_CLIENT = True
except ImportError:
    HAS_WEBSOCKET_CLIENT = False

# --------------------------------------------------------------------------
# Setup
# --------------------------------------------------------------------------

def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def setup_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )


class ApiClient:
    """
    requests.Session dùng chung giữa các thread là an toàn cho việc gọi
    request (connection pool của urllib3 tự xử lý đồng thời), nên không cần
    1 session riêng cho mỗi worker.
    """
    def __init__(self, base_url):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def _url(self, path):
        return f"{self.base_url}{path}"

    def post(self, path, token=None, json_body=None, params=None, timeout=15):
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            r = self.session.post(self._url(path), headers=headers,
                                   json=json_body, params=params, timeout=timeout)
            return r
        except requests.RequestException as e:
            logging.error(f"POST {path} network error: {e}")
            return None

    def get(self, path, token=None, params=None, timeout=15):
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            r = self.session.get(self._url(path), headers=headers,
                                  params=params, timeout=timeout)
            return r
        except requests.RequestException as e:
            logging.error(f"GET {path} network error: {e}")
            return None


class TestUser:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.token = None
        self.user_id = None
        self.display_name = username

    def __repr__(self):
        return f"<User {self.username} id={self.user_id}>"



# --------------------------------------------------------------------------
# Contextual group-chat engine
# --------------------------------------------------------------------------
#
# Chat KHÔNG còn random từng câu độc lập. Mỗi invite có một conversation
# state riêng: flow -> turn -> speaker role. Worker nào được chọn invite đó
# sẽ gửi đúng câu tiếp theo trong ngữ cảnh.
#
# role:
#   host   = chủ kèo
#   member = một người đã join
#
# stage:
#   before_join / after_join / before_start / arrival / during_meet / after_meet
#
DEFAULT_CHAT_FLOWS = {
    "before_join": [
        [
            {"role": "member", "text": "Kèo này mấy giờ bắt đầu vậy?"},
            {"role": "host", "text": "Tối nay 7h nha"},
            {"role": "member", "text": "Quán ở đâu vậy?"},
            {"role": "host", "text": "Ở Quận 7 nha, mình gửi map"},
            {"role": "member", "text": "Ok mình join nha"},
            {"role": "host", "text": "Ok bạn, lát gặp nha"},
        ],
        [
            {"role": "member", "text": "Còn chỗ trống không mọi người?"},
            {"role": "host", "text": "Còn nha, đang có mấy người rồi"},
            {"role": "member", "text": "Cho mình join với"},
            {"role": "host", "text": "Ok bạn join nha"},
            {"role": "member", "text": "Mình đi một mình được không?"},
            {"role": "host", "text": "Được nha, mọi người cũng mới quen nhau thôi"},
        ],
        [
            {"role": "member", "text": "Quán này đồ ăn ngon không mọi người?"},
            {"role": "host", "text": "Ổn nha, đồ ăn khá ngon"},
            {"role": "member", "text": "Có món gì ngon vậy?"},
            {"role": "host", "text": "Mình thấy món nướng với hải sản khá ổn"},
            {"role": "member", "text": "Ok vậy tối gặp nha"},
            {"role": "host", "text": "Ok nha"},
        ],
        [
            {"role": "member", "text": "Kèo này còn nhận người không?"},
            {"role": "host", "text": "Còn nha, cứ join đi"},
            {"role": "member", "text": "Mình mới vô app nên chưa quen ai hết"},
            {"role": "host", "text": "Không sao, vô giao lưu cho vui"},
            {"role": "member", "text": "Ok mình tham gia"},
            {"role": "host", "text": "Welcome nha haha"},
        ],
    ],
    "after_join": [
        [
            {"role": "member", "text": "Mình join rồi nha"},
            {"role": "host", "text": "Ok thấy bạn rồi"},
            {"role": "member", "text": "Tối gặp nha"},
            {"role": "host", "text": "Ok nha, tới cứ nhắn trong group"},
        ],
        [
            {"role": "member", "text": "Có ai đi từ Bình Thạnh không?"},
            {"role": "member", "text": "Mình đi từ Bình Thạnh nè"},
            {"role": "member", "text": "Vậy đi chung không?"},
            {"role": "member", "text": "Ok lát mình nhắn nha"},
        ],
        [
            {"role": "member", "text": "Có cần đặt bàn trước không?"},
            {"role": "host", "text": "Mình đặt rồi nha"},
            {"role": "member", "text": "Ok vậy yên tâm rồi"},
            {"role": "host", "text": "Tới đúng giờ là được"},
        ],
        [
            {"role": "member", "text": "Quán có chỗ để xe không?"},
            {"role": "host", "text": "Có nha, ngay trước quán"},
            {"role": "member", "text": "Ok cảm ơn nha"},
            {"role": "host", "text": "Không có gì"},
        ],
    ],
    "before_start": [
        [
            {"role": "member", "text": "Mọi người chuẩn bị đi chưa?"},
            {"role": "member", "text": "Mình đang chuẩn bị ra nè"},
            {"role": "host", "text": "Ok nha, mình cũng sắp đi"},
            {"role": "member", "text": "Lát tới mình báo trong group"},
        ],
        [
            {"role": "member", "text": "5 phút nữa mình ra"},
            {"role": "host", "text": "Ok nha, đi đường cẩn thận"},
            {"role": "member", "text": "Hôm nay đường đông quá"},
            {"role": "host", "text": "Không sao, cứ tới từ từ"},
        ],
        [
            {"role": "member", "text": "Kèo vẫn giữ nguyên chứ?"},
            {"role": "host", "text": "Giữ nguyên nha, 7h ở quán cũ"},
            {"role": "member", "text": "Ok chốt nha"},
            {"role": "host", "text": "Chốt luôn"},
        ],
    ],
    "arrival": [
        [
            {"role": "member", "text": "Mình tới nơi rồi, mọi người ở bàn nào vậy?"},
            {"role": "host", "text": "Bàn trong góc bên phải nha"},
            {"role": "member", "text": "Ok mình thấy rồi"},
            {"role": "host", "text": "Qua đây ngồi nha"},
        ],
        [
            {"role": "member", "text": "Mình đứng ngoài quán nè"},
            {"role": "host", "text": "Ra đón bạn nha"},
            {"role": "member", "text": "Ok mình thấy mọi người rồi"},
            {"role": "host", "text": "Vô ngồi đi bạn"},
        ],
        [
            {"role": "member", "text": "Mọi người tới chưa?"},
            {"role": "member", "text": "Mình đang chạy qua"},
            {"role": "host", "text": "Mình tới rồi nha"},
            {"role": "member", "text": "Ok tới mình gọi"},
        ],
    ],
    "during_meet": [
        [
            {"role": "member", "text": "Haha vui quá"},
            {"role": "member", "text": "Mới gặp mà nói chuyện hợp ghê"},
            {"role": "host", "text": "Đúng rồi haha"},
            {"role": "member", "text": "Kèo này vui nha"},
        ],
        [
            {"role": "member", "text": "Ai cụng ly đầu tiên đây?"},
            {"role": "host", "text": "Chủ kèo xin phép mở màn nha"},
            {"role": "member", "text": "Haha dzô anh em"},
            {"role": "member", "text": "Dzô 🍻"},
        ],
        [
            {"role": "member", "text": "Món này ngon nè"},
            {"role": "member", "text": "Để mình thử coi"},
            {"role": "member", "text": "Ngon thiệt haha"},
            {"role": "host", "text": "Gọi thêm một phần không?"},
            {"role": "member", "text": "Gọi thêm đi, đông người mà"},
        ],
        [
            {"role": "member", "text": "Có ai hát karaoke không?"},
            {"role": "member", "text": "Tí nữa tăng 2 mình hát nha haha"},
            {"role": "host", "text": "Kèo này có vẻ tới tăng 2 rồi"},
            {"role": "member", "text": "Chơi luôn"},
        ],
        [
            {"role": "member", "text": "Nay ai làm chủ xị vậy?"},
            {"role": "host", "text": "Hôm nay mình làm chủ xị nha haha"},
            {"role": "member", "text": "Vậy phải dzô mạnh rồi"},
            {"role": "host", "text": "Từ từ thôi anh em 😂"},
        ],
    ],
    "after_meet": [
        [
            {"role": "member", "text": "Hôm nay vui quá mọi người"},
            {"role": "member", "text": "Haha lần đầu gặp mà vui ghê"},
            {"role": "member", "text": "Lần sau rủ mình nữa nha"},
            {"role": "host", "text": "Chắc chắn rồi"},
        ],
        [
            {"role": "member", "text": "Mọi người về tới nhà chưa?"},
            {"role": "member", "text": "Mình về tới rồi nha"},
            {"role": "member", "text": "Mình cũng vừa tới"},
            {"role": "host", "text": "Ok ngủ ngon nha, hẹn kèo sau"},
        ],
        [
            {"role": "member", "text": "Kèo sau nhớ hú mình nha"},
            {"role": "host", "text": "Ok luôn, có kèo mình tag"},
            {"role": "member", "text": "Chốt nha"},
            {"role": "host", "text": "Hẹn mọi người kèo sau"},
        ],
    ],
    "cancel": [
        [
            {"role": "member", "text": "Hôm nay mình bận đột xuất, chắc không qua được"},
            {"role": "host", "text": "Ok nha, khi nào rảnh tham gia kèo sau"},
            {"role": "member", "text": "Cảm ơn nha, hẹn lần sau"},
        ],
    ],
    "late_arrival": [
        [
            {"role": "member", "text": "Mình kẹt xe chút, tới trễ xíu nha"},
            {"role": "host", "text": "Ok nha, cứ đi từ từ"},
            {"role": "member", "text": "Mọi người cứ vô trước đi"},
            {"role": "host", "text": "Ok, tới gọi mình"},
        ],
    ],
}

def normalize_chat_flows(cfg):
    """
    Cho phép config.json có chat_flows riêng. Nếu không có, dùng flow mặc định
    ở trên. Cũng hỗ trợ dạng flow = ["câu 1", "câu 2"] bằng cách tự chuyển
    thành turn member/host xen kẽ.
    """
    raw = cfg.get("chat_flows")
    if not isinstance(raw, dict) or not raw:
        return DEFAULT_CHAT_FLOWS

    normalized = {}
    for stage, conversations in raw.items():
        if not isinstance(conversations, list):
            continue
        out = []
        for conversation in conversations:
            if not isinstance(conversation, list) or not conversation:
                continue
            turns = []
            for idx, turn in enumerate(conversation):
                if isinstance(turn, str):
                    turns.append({
                        "role": "member" if idx % 2 == 0 else "host",
                        "text": turn,
                    })
                elif isinstance(turn, dict) and turn.get("text"):
                    role = turn.get("role", "member")
                    if role not in ("host", "member"):
                        role = "member"
                    turns.append({"role": role, "text": str(turn["text"])})
            if turns:
                out.append(turns)
        if out:
            normalized[stage] = out

    for stage, conversations in DEFAULT_CHAT_FLOWS.items():
        normalized.setdefault(stage, conversations)

    return normalized


def get_invite_stage_locked(invite_id, state: "SharedState"):
    """
    Suy ra stage từ thời gian kèo + trạng thái mô phỏng.
    - Trước giờ: before_start
    - Trong 4 giờ sau giờ bắt đầu: during_meet
    - Sau đó: after_meet
    Nếu kèo chưa có start_time: dùng số member để phân biệt sơ bộ.
    """
    now = datetime.now()
    start_dt = state.invite_start_time.get(invite_id)

    if state.invite_status.get(invite_id) in ("closed", "finished", "completed"):
        return "after_meet"

    if start_dt:
        if now < start_dt - timedelta(minutes=30):
            return "after_join"
        if now < start_dt:
            return "before_start"
        if now <= start_dt + timedelta(hours=4):
            return "during_meet"
        return "after_meet"

    members = state.invite_members.get(invite_id, set())
    if len(members) >= 2:
        return "after_join"
    return "before_join"


def choose_sender_for_role_locked(role, users, host_id, member_ids):
    if role == "host":
        candidates = [u for u in users if u.user_id == host_id]
        return random.choice(candidates) if candidates else None

    candidates = [u for u in users if u.user_id in member_ids and u.user_id != host_id]
    if not candidates:
        candidates = [u for u in users if u.user_id in member_ids]
    return random.choice(candidates) if candidates else None


def get_contextual_group_turn(cfg, users, invite_id, state: "SharedState"):
    """
    Lấy đúng 1 turn kế tiếp của một conversation đang chạy trong invite.
    Không random câu độc lập. Nếu flow trước đó hết, tạo flow mới phù hợp
    với stage hiện tại.
    """
    flows = normalize_chat_flows(cfg)

    with state.lock:
        host_id = state.invite_hosts.get(invite_id)
        member_ids = set(state.invite_members.get(invite_id, set()))
        if host_id:
            member_ids.add(host_id)

        if len(member_ids) < 2:
            return None

        stage = get_invite_stage_locked(invite_id, state)
        conversations = flows.get(stage) or flows.get("after_join") or []

        if not conversations:
            return None

        conv = state.invite_chat_conversations.get(invite_id)
        if (
            not conv
            or conv.get("stage") != stage
            or conv.get("turn_index", 0) >= len(conv.get("turns", []))
        ):
            turns = random.choice(conversations)
            state.invite_chat_conversations[invite_id] = {
                "stage": stage,
                "turns": turns,
                "turn_index": 0,
                "started_at": datetime.now(),
            }
            conv = state.invite_chat_conversations[invite_id]

        turn = conv["turns"][conv["turn_index"]]
        sender = choose_sender_for_role_locked(
            turn.get("role", "member"), users, host_id, member_ids
        )

        if sender is None:
            # Nếu host/member không còn nằm trong danh sách login hiện tại,
            # chọn một member thực tế của invite để không gửi sai user.
            available = [u for u in users if u.user_id in member_ids]
            if not available:
                return None
            sender = random.choice(available)

        return {
            "stage": stage,
            "sender": sender,
            "message": turn["text"],
            "turn_index": conv["turn_index"],
            "total_turns": len(conv["turns"]),
        }


def advance_contextual_group_turn(invite_id, state: "SharedState"):
    with state.lock:
        conv = state.invite_chat_conversations.get(invite_id)
        if conv:
            conv["turn_index"] = conv.get("turn_index", 0) + 1



class SharedState:
    """
    Bọc toàn bộ state dùng chung + 1 threading.Lock để nhiều worker đọc/ghi
    an toàn. QUY TẮC: chỉ giữ lock trong lúc thao tác dict/set thuần tuý
    (rất nhanh, micro-giây), KHÔNG BAO GIỜ giữ lock trong lúc gọi api.post/
    api.get (network I/O, có thể mất vài trăm ms - vài giây) — nếu không sẽ
    vô tình biến lại thành chạy tuần tự y hệt bản cũ.
    """
    def __init__(self, seed_invite_ids):
        self.lock = threading.Lock()
        self.invited_products = set()
        self.open_invites = set(seed_invite_ids)
        self.invite_hosts = {}
        self.invite_members = {}
        self.invite_start_time = {}
        self.invite_status = {}
        self.finding_on = set()

        # 🆕 ATTENDANCE: invite_id -> {user_id: attendance_status}, để nhớ
        # user nào đã ở trạng thái nào rồi (tránh gọi lại API set đúng
        # status cũ, và để quyết định bước tiếp theo trong hành trình
        # undecided -> on_the_way -> going/late/not_going).
        self.invite_attendance = {}
        # 🆕 Cache toạ độ quán của từng invite (lat, lng) hoặc False nếu đã
        # thử fetch mà không có -> tránh gọi lại meeting-point liên tục.
        self.invite_venue_coords = {}

        # Contextual group-chat state:
        # invite_id -> {"stage", "turns", "turn_index", "started_at"}
        self.invite_chat_conversations = {}


# --------------------------------------------------------------------------
# Auth
# --------------------------------------------------------------------------

def login_all(api: ApiClient, accounts):
    users = []
    for acc in accounts:
        u = TestUser(acc["username"], acc["password"])
        r = api.post("/wp-json/jwt-auth/v1/token", json_body={
            "username": u.username,
            "password": u.password,
        })
        if r is None or r.status_code != 200:
            body = r.text[:200] if r is not None else "no response"
            logging.warning(f"Login FAILED cho {u.username}: {body}")
            continue

        data = r.json()
        token = data.get("token") or data.get("data", {}).get("token")
        if not token:
            logging.warning(f"Login OK nhưng không thấy token cho {u.username}: {data}")
            continue

        u.token = token

        # user_id: 1 số site trả sẵn trong response login, nếu không thì
        # gọi 1 endpoint xác thực bất kỳ để suy ra qua get_current_user_id()
        # phía server (ví dụ /nhau/v1/blocked-list trả rỗng nhưng còn login).
        u.user_id = (
            data.get("user_id")
            or data.get("id")
            or (data.get("data") or {}).get("user_id")
        )
        users.append(u)
        logging.info(f"Login OK: {u.username} (user_id={u.user_id})")

    if not users:
        logging.error("Không login được tài khoản nào — kiểm tra lại username/password/base_url.")
        sys.exit(1)

    # Nếu user_id chưa suy ra được từ response login, thử endpoint riêng.
    # Nhân tiện lấy luôn display name thật (dùng để hiện tên trong group chat
    # thay vì chỉ có username kỹ thuật).
    for u in users:
        r = api.get("/wp-json/wp/v2/users/me", token=u.token)
        if r is not None and r.status_code == 200:
            me = r.json()
            if not u.user_id:
                u.user_id = me.get("id")
                logging.info(f"Suy ra user_id qua /wp/v2/users/me cho {u.username}: {u.user_id}")
            if me.get("name"):
                u.display_name = me["name"]

    users = [u for u in users if u.user_id]
    return users


# --------------------------------------------------------------------------
# Action helpers — mỗi hàm map đúng 1 route thật trong plugin.
# Pattern chung: (1) đọc state cần thiết dưới lock, (2) gọi API KHÔNG giữ
# lock, (3) cập nhật state dưới lock dựa trên kết quả.
# --------------------------------------------------------------------------

def action_create_invite(api, cfg, host: TestUser, state: SharedState):
    with state.lock:
        product_ids = [pid for pid in cfg.get("product_ids", []) if pid not in state.invited_products]
    if not product_ids:
        return None

    product_id = random.choice(product_ids)
    max_people = random.randint(2, 8)
    start_dt = datetime.now() + timedelta(hours=random.randint(1, 6))
    start_time = start_dt.strftime("%Y-%m-%d %H:%M:%S")

    r = api.post("/wp-json/nhau/v1/invite/create", token=host.token, json_body={
        "product_id": product_id,
        "max_people": max_people,
        "start_time": start_time,
    })
    if r is None:
        return None
    data = safe_json(r)
    if data and data.get("success"):
        invite_id = data.get("invite_id")
        with state.lock:
            state.invited_products.add(product_id)
            state.open_invites.add(invite_id)
            state.invite_hosts[invite_id] = host.user_id
            state.invite_members.setdefault(invite_id, set()).add(host.user_id)
            state.invite_start_time[invite_id] = start_dt
        logging.info(f"[INVITE] {host.username} tạo kèo mới -> invite_id={invite_id} (product={product_id})")
        return invite_id
    logging.info(f"[INVITE] {host.username} tạo kèo thất bại: {data}")
    return None


def _is_invite_live_locked(invite_id, state: SharedState):
    """
    Gọi khi ĐANG giữ state.lock. 'Live' = kèo đang diễn ra ngay bây giờ.
    Ưu tiên status rõ ràng nếu server trả (vd 'live'/'ongoing'); nếu không
    có thì suy ra từ start_time: coi là live nếu đã tới giờ và chưa quá 4
    tiếng (thời lượng ước lượng 1 buổi nhậu/karaoke/bar). Không biết
    start_time -> coi là "không chắc live", không được ưu tiên.
    """
    status = state.invite_status.get(invite_id)
    if status in ("live", "ongoing", "in_progress", "started"):
        return True

    start_dt = state.invite_start_time.get(invite_id)
    if start_dt:
        now = datetime.now()
        return start_dt <= now <= start_dt + timedelta(hours=4)

    return False


def _pick_invite_weighted_locked(candidates, state: SharedState, live_weight=1.0, other_weight=0.8):
    """
    Gọi khi ĐANG giữ state.lock. Ưu tiên kèo đang live, kèo khác vẫn có thể
    được chọn nhưng với xác suất tương đối = other_weight/live_weight
    (mặc định 80%).
    """
    if not candidates:
        return None
    weights = [live_weight if _is_invite_live_locked(i, state) else other_weight for i in candidates]
    return random.choices(candidates, weights=weights, k=1)[0]


def action_join_invite(api, cfg, user: TestUser, state: SharedState):
    with state.lock:
        candidates = [
            i for i in state.open_invites
            if state.invite_hosts.get(i) != user.user_id
            and user.user_id not in state.invite_members.get(i, set())
        ]
        if not candidates:
            invite_id = None
        else:
            invite_id = _pick_invite_weighted_locked(candidates, state)
            live_tag = "LIVE" if _is_invite_live_locked(invite_id, state) else "not-live"

    if invite_id is None:
        logging.info(f"[JOIN] {user.username} chưa có kèo nào (mới/chưa join) để join, bỏ qua round này.")
        return

    r = api.post("/wp-json/nhau/v1/invite/join", token=user.token, json_body={
        "invite_id": invite_id,
    })
    if r is None:
        return
    data = safe_json(r)
    if data and data.get("success"):
        with state.lock:
            state.invite_members.setdefault(invite_id, set()).add(user.user_id)
        logging.info(f"[JOIN] {user.username} join invite_id={invite_id} [{live_tag}]")
    else:
        logging.info(f"[JOIN] {user.username} join invite_id={invite_id} [{live_tag}] thất bại: {data}")


def action_send_chat(api, cfg, users, state: SharedState):
    if len(users) < 2:
        return
    sender, receiver = random.sample(users, 2)
    chat_messages = cfg.get("chat_messages") or [
        "Alo, hôm nay có ai rảnh không?",
        "Mọi người đang ở đâu vậy?",
        "Ok nha",
        "Haha vui ghê",
    ]
    message = random.choice(chat_messages)

    r = api.post("/wp-json/spiritwebs/v1/send-message", token=sender.token, json_body={
        "sender_id": sender.user_id,
        "receiver_id": receiver.user_id,
        "message": message,
    })
    if r is None:
        return
    if r.status_code == 200:
        logging.info(f"[CHAT] {sender.username} -> {receiver.username}: {message}")
    else:
        logging.info(f"[CHAT] {sender.username} -> {receiver.username} thất bại ({r.status_code}): {r.text[:150]}")


def action_send_group_chat(api, cfg, users, state: SharedState):
    """
    Chat nhóm theo NGỮ CẢNH của từng invite.

    Không còn random.choice(chat_messages). Mỗi invite có một conversation
    riêng và mỗi lần worker chạy sẽ gửi đúng TURN tiếp theo:
        member -> host -> member -> host ...

    Stage được suy ra từ thời gian kèo:
        after_join -> before_start -> during_meet -> after_meet
    """
    with state.lock:
        all_invite_ids = list(set(state.open_invites) | set(state.invite_members.keys()))
        if not all_invite_ids:
            return

        # Ưu tiên invite có từ 2 member đã biết để conversation có đối thoại.
        viable = [
            invite_id for invite_id in all_invite_ids
            if len(state.invite_members.get(invite_id, set())) >= 2
        ]
        candidates = viable or all_invite_ids
        invite_id = _pick_invite_weighted_locked(candidates, state)
        live_tag = "LIVE" if _is_invite_live_locked(invite_id, state) else "not-live"

    turn = get_contextual_group_turn(cfg, users, invite_id, state)
    if not turn:
        logging.info(
            f"[GROUP-CHAT] invite_id={invite_id} [{live_tag}] "
            "chưa đủ member hoặc chưa có flow phù hợp -> bỏ qua."
        )
        return

    sender = turn["sender"]
    message = turn["message"]

    r = api.post("/wp-json/spiritwebs/v1/send-group-message", token=sender.token, json_body={
        "group_id": invite_id,
        "sender_id": sender.user_id,
        "sender_name": sender.display_name,
        "message": message,
    })
    if r is None:
        return

    data = safe_json(r)
    if data and data.get("success"):
        advance_contextual_group_turn(invite_id, state)
        logging.info(
            f"[GROUP-CHAT] invite_id={invite_id} [{turn['stage']}] "
            f"turn={turn['turn_index'] + 1}/{turn['total_turns']} | "
            f"{sender.display_name}: {message}"
        )
    else:
        logging.info(
            f"[GROUP-CHAT] invite_id={invite_id} thất bại: {data}"
        )


def action_finding_keo(api, cfg, user: TestUser, state: SharedState):
    district = random.choice(cfg["districts"])
    center = cfg.get("district_centers", {}).get(district, cfg["hcmc_center"])
    jitter = cfg["coord_jitter_deg"]
    lat = center["lat"] + random.uniform(-jitter, jitter)
    lng = center["lng"] + random.uniform(-jitter, jitter)
    option = random.choice(cfg["finding_options"])
    activity = random.choice(cfg["activity_types"])

    with state.lock:
        turning_on = user.user_id not in state.finding_on

    if turning_on:
        r = api.post("/wp-json/custom/v1/finding-keo/on", json_body={
            "user_id": user.user_id,
            "lat": lat,
            "lng": lng,
            "district": district,
            "finding_option": option,
            "activity_type": activity,
        })
        rdata = safe_json(r) if r is not None else None
        if rdata and rdata.get("success"):
            with state.lock:
                state.finding_on.add(user.user_id)
            logging.info(f"[FINDING-KEO ON] {user.username} @ {district} ({activity}, {option})")
    else:
        r = api.post("/wp-json/custom/v1/finding-keo/off", json_body={
            "user_id": user.user_id,
        })
        if r is not None:
            with state.lock:
                state.finding_on.discard(user.user_id)
            logging.info(f"[FINDING-KEO OFF] {user.username}")


# --------------------------------------------------------------------------
# Map presence bots — khác HOÀN TOÀN với các action REST ở trên. Tính năng
# "hiện user xung quanh" bên flutter_map.dart KHÔNG đọc DB, mà dựa vào
# Phoenix Presence: chỉ user nào đang giữ 1 WebSocket THẬT SỰ SỐNG, đã join
# channel "online_users:lobby", mới được server track và hiện marker cho
# người khác thấy. Không có đường tắt REST nào thay được việc này.
#
# Protocol lấy đúng từ phoenix_socket_web/channels/online_users_channel.ex:
#   - join("online_users:lobby", %{user_id, username, avatar, latitude,
#     longitude}) -> server tự Presence.track() + push "presence_state".
#   - handle_in("update_presence", %{username, latitude, longitude, avatar})
#     -> cập nhật vị trí, coi như user vẫn đang "sống"/di chuyển.
#   - Tầng transport (không phải channel) cần heartbeat định kỳ, nếu không
#     Phoenix tự ngắt kết nối sau khoảng ~60s im lặng.
# Wire format là chuẩn Phoenix Channels v2 (JSON serializer):
#   [join_ref, ref, topic, event, payload]
# --------------------------------------------------------------------------

class MapPresenceBot:
    """
    Giữ 1 WebSocket riêng cho 1 user, chạy trong thread riêng (daemon), tự
    reconnect (backoff tăng dần) nếu rớt kết nối, tự dừng khi stop_event
    được set. Tương đương hành vi connectPhoenix() bên flutter_map.dart,
    chỉ khác là giả lập bằng Python thay vì app thật.
    """

    HEARTBEAT_INTERVAL_SECONDS = 25  # Phoenix mặc định coi là chết nếu im lặng ~60s

    # Sau khi "tới nơi", đứng/ngồi yên 1 khoảng ngẫu nhiên trong khoảng này
    # trước khi tự chọn điểm đến mới -> giả lập đang ngồi nhậu/cà phê tại chỗ.
    SIT_MIN_SECONDS = 8 * 60
    SIT_MAX_SECONDS = 20 * 60

    def __init__(self, ws_url, user: "TestUser", cfg, stop_event: threading.Event,
                 avatar="", shared_district=None, shared_destination=None):
        self.ws_url = ws_url
        self.user = user
        self.cfg = cfg
        self.stop_event = stop_event
        self.avatar = avatar
        # Nếu được gán sẵn (bot thuộc 1 "nhóm bạn hẹn nhau") thì dùng chung
        # quận + điểm đến với cả nhóm -> nhiều marker cùng hội tụ về 1 chỗ
        # trên map, giống 1 nhóm người thật cùng đi tới 1 quán.
        self.shared_district = shared_district
        self.shared_destination = shared_destination
        self._ref_seq = itertools.count(1)
        self.thread = threading.Thread(target=self._run, daemon=True)
        # State vị trí/hướng đi hiện tại — set thật ở _init_position() mỗi khi
        # (re)connect, để mỗi lần rớt-nối-lại có thể đổi sang quận/khu vực khác.
        self.district = None
        self.center = None
        self.lat = None
        self.lng = None
        self.destination = None
        self.state = "moving"   # "moving" (đang đi tới destination) | "sitting" (đã tới, đứng yên)
        self.sit_until = None

    def start(self):
        self.thread.start()

    def stop_and_join(self, timeout=5):
        self.thread.join(timeout=timeout)

    def _next_ref(self):
        return str(next(self._ref_seq))

    def _random_point_around(self, center, jitter):
        r = random.uniform(0, jitter)
        angle = random.uniform(0, 360)
        return (center["lat"] + r * math.cos(math.radians(angle)),
                center["lng"] + r * math.sin(math.radians(angle)))

    def _pick_destination(self):
        """Chọn 1 'điểm hẹn' mới (giả lập 1 quán cụ thể) trong quận hiện tại."""
        jitter = self.cfg["coord_jitter_deg"]
        return self._random_point_around(self.center, jitter)

    def _init_position(self):
        """
        Chọn quận + điểm đến ban đầu. Nếu bot này thuộc 1 "nhóm bạn" (được
        start_map_presence_bots gán shared_district/shared_destination) thì
        dùng chung điểm đến với cả nhóm -> nhiều marker cùng hội tụ về 1 chỗ.
        Vị trí xuất phát là 1 điểm khác ngẫu nhiên trong quận (chưa tới nơi),
        để bot có quãng đường "đi tới" chứ không tự nhiên đứng sẵn ở đích.
        """
        if self.shared_district and self.shared_destination:
            self.district = self.shared_district
            self.destination = self.shared_destination
        else:
            self.district = _choose_weighted_district(self.cfg)

        self.center = self.cfg.get("district_centers", {}).get(self.district, self.cfg["hcmc_center"])
        jitter = self.cfg["coord_jitter_deg"]
        self.lat, self.lng = self._random_point_around(self.center, jitter)
        if self.destination is None:
            self.destination = self._pick_destination()
        self.state = "moving"
        self.sit_until = None
        return self.lat, self.lng

    def _step_coords(self):
        """
        Hành vi 2 pha, giống người thật đi nhậu:
        - "moving": đi dần về self.destination theo từng bước nhỏ (hướng lệch
          nhẹ +/-15° mỗi lần cho tự nhiên, không đi thẳng tắp như robot).
          Tới đủ gần đích -> chuyển sang "sitting".
        - "sitting": đứng/ngồi gần như yên 1 chỗ (chỉ dao động vài mét, kiểu
          đổi tư thế cầm điện thoại) trong SIT_MIN..SIT_MAX giây, sau đó tự
          chọn 1 điểm đến mới trong quận và quay lại "moving" (kèo tan, đi
          chỗ khác / về nhà).
        """
        jitter = self.cfg["coord_jitter_deg"]
        now = time.time()

        if self.state == "sitting":
            if now >= self.sit_until:
                self.destination = self._pick_destination()
                self.state = "moving"
            else:
                micro = jitter * 0.01
                self.lat += random.uniform(-micro, micro)
                self.lng += random.uniform(-micro, micro)
                return self.lat, self.lng

        dlat = self.destination[0] - self.lat
        dlng = self.destination[1] - self.lng
        dist = math.hypot(dlat, dlng)
        arrival_threshold = jitter * 0.03

        if dist <= arrival_threshold:
            self.state = "sitting"
            self.sit_until = now + random.uniform(self.SIT_MIN_SECONDS, self.SIT_MAX_SECONDS)
            logging.info(f"[MAP] {self.user.username} đã tới điểm hẹn ở {self.district}, "
                         f"dừng lại ~{int((self.sit_until - now) / 60)} phút.")
            return self.lat, self.lng

        step = min(dist, jitter * random.uniform(0.08, 0.18))
        angle_to_dest = math.degrees(math.atan2(dlng, dlat))
        heading = math.radians(angle_to_dest + random.uniform(-15, 15))
        self.lat += step * math.cos(heading)
        self.lng += step * math.sin(heading)
        return self.lat, self.lng

    def _run(self):
        backoff = 3
        while not self.stop_event.is_set():
            try:
                self._connect_and_loop()
                backoff = 3  # vừa chạy ổn -> reset backoff cho lần rớt kế tiếp
            except Exception as e:
                logging.warning(f"[MAP] {self.user.username} socket lỗi: {e}")
            if self.stop_event.is_set():
                return
            # Backoff tăng dần khi rớt liên tục, tránh dội reconnect dồn dập
            # nếu server/mạng đang có vấn đề.
            time.sleep(backoff + random.uniform(0, 2))
            backoff = min(backoff * 2, 60)

    def _connect_and_loop(self):
        ws = websocket.create_connection(self.ws_url, timeout=10)
        try:
            join_ref = self._next_ref()
            lat, lng = self._init_position()
            username = self.user.display_name or self.user.username

            ws.send(json.dumps([
                join_ref, self._next_ref(), "online_users:lobby", "phx_join",
                {
                    "user_id": self.user.user_id,
                    "username": username,
                    "avatar": self.avatar,
                    "latitude": lat,
                    "longitude": lng,
                },
            ]))
            logging.info(f"[MAP] {self.user.username} join online_users:lobby "
                         f"(lat={lat:.5f}, lng={lng:.5f})")

            ws.settimeout(5)
            last_heartbeat = time.time()
            last_update = time.time()
            update_interval = self.cfg.get("map_presence_update_interval_seconds", 25)

            while not self.stop_event.is_set():
                now = time.time()

                if now - last_heartbeat >= self.HEARTBEAT_INTERVAL_SECONDS:
                    # Topic "phoenix" là kênh đặc biệt dành riêng cho heartbeat
                    # ở tầng transport, join_ref để rỗng.
                    ws.send(json.dumps(["", self._next_ref(), "phoenix", "heartbeat", {}]))
                    last_heartbeat = now

                if now - last_update >= update_interval:
                    # Giả lập user "di chuyển" nhẹ quanh khu vực -> đi tiếp 1
                    # bước nhỏ theo hướng hiện tại (random walk), giống người
                    # thật cầm điện thoại đi bộ, thay vì nhảy toạ độ lung tung.
                    lat, lng = self._step_coords()
                    ws.send(json.dumps([
                        join_ref, self._next_ref(), "online_users:lobby", "update_presence",
                        {
                            "username": username,
                            "avatar": self.avatar,
                            "latitude": lat,
                            "longitude": lng,
                        },
                    ]))
                    last_update = now

                try:
                    ws.recv()  # chỉ để rút cạn buffer (presence_diff của user khác...)
                except websocket.WebSocketTimeoutException:
                    continue
        finally:
            try:
                ws.close()
            except Exception:
                pass


def _get_invite_venue_coords(api, invite_id, token, state: SharedState):
    """
    Lấy toạ độ quán (điểm hẹn) của 1 invite qua GET /invite/meeting-point,
    có cache trong state để không gọi lại API mỗi lần cần check-in cho
    cùng 1 invite. Trả về (lat, lng) hoặc None nếu invite chưa có toạ độ /
    fetch lỗi.
    """
    with state.lock:
        cached = state.invite_venue_coords.get(invite_id)
    if cached is not None:
        return cached if cached is not False else None

    r = api.get("/wp-json/nhau/v1/invite/meeting-point", token=token,
                params={"invite_id": invite_id})
    coords = None
    if r is not None and r.status_code == 200:
        data = safe_json(r)
        if data and data.get("success"):
            try:
                coords = (float(data["lat"]), float(data["lng"]))
            except (TypeError, ValueError, KeyError):
                coords = None

    with state.lock:
        state.invite_venue_coords[invite_id] = coords if coords is not None else False
    return coords


def action_update_attendance(api, cfg, users, state: SharedState):
    """
    🆕 Giả lập user tự cập nhật trạng thái tham gia kèo theo thời gian thực,
    y hệt luồng thật trong app (nút "Đang tới" / "Đã tới" / "Trễ" / "Không
    tới" ở product_detail_page.dart), qua route
    POST /nhau/v1/invite/update-attendance.

    Hành trình mô phỏng, dựa theo stage suy ra từ start_time (dùng lại
    đúng get_invite_stage_locked() đang dùng cho group-chat):
        before_start -> phần lớn chuyển 'on_the_way' (đang tới), thỉnh
                        thoảng huỷ ('not_going')
        arrival / during_meet -> phần lớn chuyển 'going' (ĐÃ TỚI — cần
                        check-in GPS thật, script tự lấy toạ độ quán qua
                        /invite/meeting-point rồi rải toạ độ giả quanh đó
                        trong bán kính cho phép), số ít bị 'late' (trễ,
                        không cần GPS).
    Không đụng tới user đã 'going' hoặc 'not_going' (coi như đã chốt).
    """
    with state.lock:
        candidates = []
        for invite_id, member_ids in state.invite_members.items():
            stage = get_invite_stage_locked(invite_id, state)
            if stage not in ("before_start", "arrival", "during_meet"):
                continue
            for uid in member_ids:
                current = state.invite_attendance.get(invite_id, {}).get(uid)
                if current in ("going", "not_going"):
                    continue  # đã chốt trạng thái, không đổi nữa
                candidates.append((invite_id, uid, stage, current))

    if not candidates:
        return

    invite_id, user_id, stage, current = random.choice(candidates)
    user = next((u for u in users if u.user_id == user_id), None)
    if user is None:
        return

    roll = random.random()
    if stage == "before_start":
        if current == "late":
            return
        new_status = "not_going" if roll < 0.08 else "on_the_way"
    else:  # arrival / during_meet -> tới giờ hẹn hoặc đã qua giờ
        if current == "late":
            return
        new_status = "going" if roll < 0.85 else "late"

    if new_status == current:
        return

    payload = {"invite_id": invite_id, "status": new_status}

    if new_status == "going":
        coords = _get_invite_venue_coords(api, invite_id, user.token, state)
        if coords is None:
            # Kèo chưa có toạ độ quán -> không check-in GPS được, fallback
            # 'late' để user vẫn có trạng thái cập nhật thay vì kẹt mãi.
            new_status = "late"
            payload["status"] = "late"
        else:
            # Rải quanh quán trong khoảng vài chục mét, luôn nằm trong bán
            # kính check-in cho phép (NHAU_CHECKIN_RADIUS_METERS = 300m
            # bên server) để mô phỏng "đã đứng gần quán".
            jitter_deg = 0.0004  # ~40-45m
            payload["lat"] = coords[0] + random.uniform(-jitter_deg, jitter_deg)
            payload["lng"] = coords[1] + random.uniform(-jitter_deg, jitter_deg)

    r = api.post("/wp-json/nhau/v1/invite/update-attendance", token=user.token, json_body=payload)
    if r is None:
        return

    data = safe_json(r)
    label = {
        "on_the_way": "ĐANG TỚI",
        "going": "ĐÃ TỚI",
        "late": "TRỄ",
        "not_going": "KHÔNG THAM GIA",
    }.get(new_status, new_status)

    if data and data.get("success"):
        with state.lock:
            state.invite_attendance.setdefault(invite_id, {})[user_id] = data.get(
                "attendance_status", new_status
            )
        logging.info(f"[ATTENDANCE] {user.username} invite_id={invite_id} -> {label}")
    else:
        logging.info(
            f"[ATTENDANCE] {user.username} invite_id={invite_id} -> {label} thất bại: {data}"
        )


def start_map_presence_bots(cfg, users, stop_event: threading.Event):
    """
    Chọn ngẫu nhiên map_presence_worker_count user (trong số đã login) để
    giữ WebSocket sống, hiện marker trên map. Trả về list bot đã start (để
    main() join lại lúc dừng script).
    """
    if not cfg.get("map_presence_enabled"):
        logging.info("[MAP] map_presence_enabled=false -> bỏ qua, không có bot nào hiện trên map.")
        return []

    ws_url = cfg.get("websocket_url")
    if not ws_url:
        logging.warning("[MAP] map_presence_enabled=true nhưng thiếu \"websocket_url\" trong "
                         "config -> bỏ qua tính năng hiện user trên map.")
        return []

    if not HAS_WEBSOCKET_CLIENT:
        logging.error("[MAP] Thiếu thư viện websocket-client -> chạy: pip install websocket-client "
                       "rồi chạy lại script để bật tính năng hiện user trên map.")
        return []

    count = min(cfg.get("map_presence_worker_count", 10), len(users))
    chosen = random.sample(users, count)
    avatars = cfg.get("bot_avatars") or [""]

    # Chia ngẫu nhiên 1 phần bot thành các "nhóm bạn" (2-4 người) cùng hẹn tới
    # 1 điểm trong cùng 1 quận -> nhìn trên map sẽ thấy nhiều marker hội tụ
    # lại gần nhau đúng kiểu 1 nhóm người thật đi nhậu chung, thay vì rải đều
    # ngẫu nhiên khắp nơi. group_ratio = tỉ lệ user được xếp vào nhóm.
    group_ratio = cfg.get("map_presence_group_ratio", 0.5)
    pool = chosen[:]
    random.shuffle(pool)
    n_grouped = int(len(pool) * group_ratio)
    grouped_pool, solo_pool = pool[:n_grouped], pool[n_grouped:]

    bots = []
    i = 0
    while i < len(grouped_pool):
        size = min(random.randint(2, 4), len(grouped_pool) - i)
        members = grouped_pool[i:i + size]
        i += size

        district = _choose_weighted_district(cfg)
        center = cfg.get("district_centers", {}).get(district, cfg["hcmc_center"])
        jitter = cfg["coord_jitter_deg"]
        # Điểm hẹn chung của cả nhóm (giả lập toạ độ 1 quán cụ thể).
        r = random.uniform(0, jitter * 0.6)  # nằm gần tâm quận hơn 1 chút cho hợp lý
        angle = random.uniform(0, 360)
        destination = (center["lat"] + r * math.cos(math.radians(angle)),
                       center["lng"] + r * math.sin(math.radians(angle)))

        for u in members:
            bots.append(MapPresenceBot(
                ws_url, u, cfg, stop_event,
                avatar=random.choice(avatars),
                shared_district=district,
                shared_destination=destination,
            ))
        logging.info(f"[MAP] Nhóm {len(members)} user cùng hẹn nhau ở {district} "
                     f"(lat={destination[0]:.5f}, lng={destination[1]:.5f}).")

    for u in solo_pool:
        bots.append(MapPresenceBot(ws_url, u, cfg, stop_event, avatar=random.choice(avatars)))

    random.shuffle(bots)  # tránh join theo đúng thứ tự nhóm, rải ngẫu nhiên hơn
    for bot in bots:
        bot.start()
        # Rải thời điểm join ra 1 chút, giống cách rải worker REST bên dưới —
        # tránh N kết nối cùng bắn lên server trong đúng 1 khoảnh khắc.
        time.sleep(random.uniform(0, 1.5))

    logging.info(f"[MAP] Đã bật presence cho {len(bots)}/{len(users)} user đã login -> họ sẽ hiện "
                 f"marker trên map trong lúc script còn chạy.")
    return bots


def _choose_weighted_district(cfg):
    """
    Chọn 1 quận, ưu tiên trọng số nếu config có khai báo "district_weights"
    (vd khu trung tâm hay có kèo nhậu hơn ngoại thành). Nếu không khai báo
    hoặc thiếu quận nào trong map trọng số -> mặc định weight=1 (đều tay như
    cũ, không breaking config hiện có).
    """
    districts = cfg["districts"]
    weights_cfg = cfg.get("district_weights", {})
    weights = [weights_cfg.get(d, 1) for d in districts]
    return random.choices(districts, weights=weights, k=1)[0]


def fetch_product_ids(api: ApiClient, cfg, max_products=60):
    """
    Nếu config chưa điền product_ids, tự lấy danh sách product đang publish
    trên site qua WooCommerce Store API (public, không cần API key) —
    endpoint mặc định của WooCommerce: /wp-json/wc/store/v1/products.
    Nếu site tắt Store API hoặc trả lỗi, log rõ để anh tự điền tay.
    """
    if cfg.get("product_ids"):
        logging.info(f"Dùng {len(cfg['product_ids'])} product_id đã điền sẵn trong config.")
        return cfg["product_ids"]

    logging.info("Chưa có product_ids trong config -> thử tự fetch qua WooCommerce Store API...")
    r = api.get("/wp-json/wc/store/v1/products", params={"per_page": max_products})
    if r is None or r.status_code != 200:
        logging.warning(
            "Không fetch được product list qua Store API "
            f"(status={r.status_code if r else 'no response'}). "
            "Hành vi 'tạo kèo mới' sẽ bị bỏ qua — điền tay product_ids vào config nếu cần."
        )
        return []

    try:
        products = r.json()
        ids = [p["id"] for p in products if "id" in p]
    except Exception as e:
        logging.warning(f"Parse response Store API lỗi: {e}")
        return []

    logging.info(f"Fetch được {len(ids)} product_id từ Store API: {ids[:10]}{'...' if len(ids) > 10 else ''}")
    return ids


def fetch_existing_invites(api: ApiClient, cfg, product_ids, token=None):
    """
    Lấy các kèo đang mở sẵn trên site qua GET /wp-json/nhau/v1/invite/by-products
    (theo đúng list product_id đã có), để seed vào state ngay từ đầu thay vì
    chỉ join/chat nhóm được sau khi bot tự tạo kèo mới hoặc phải điền tay
    seed_invite_ids.

    Schema đã xác nhận từ code PHP thật (nhau_get_invite_by_products_bulk
    trong invite-api.php):
      GET /wp-json/nhau/v1/invite/by-products?ids=20112,20108,20096
      -> { "success": true,
           "data": {
               "<product_id>": {
                   "success": true,
                   "invite_id": 123,
                   "status": "open" | "closed" | ...,
                   "is_joined": bool,
                   "is_full": bool,
                   "joined_count": int,
                   "max_people": int,
                   "invite": {
                       "id": 123, "product_id": 20112, "host_id": 45,
                       "status": "open", "max_people": 5,
                       "start_time": "2026-07-27 20:00:00"
                   }
               },
               ... (chỉ product nào có kèo mới xuất hiện trong "data")
           }
         }
    Lưu ý: "data" là OBJECT khoá theo product_id, KHÔNG phải mảng.
    """
    if not product_ids:
        logging.info("Không có product_ids -> bỏ qua fetch kèo có sẵn.")
        return []

    r = api.get("/wp-json/nhau/v1/invite/by-products", token=token,
                params={"ids": ",".join(str(p) for p in product_ids)})
    if r is None or r.status_code != 200:
        logging.warning(
            "Không fetch được kèo có sẵn qua /invite/by-products "
            f"(status={r.status_code if r else 'no response'}). "
            "Bỏ qua, chỉ dùng seed_invite_ids trong config (nếu có)."
        )
        return []

    data = safe_json(r)
    if not data or not data.get("success"):
        logging.warning(f"Response /invite/by-products không hợp lệ hoặc success=false: {data}")
        return []

    by_product = data.get("data") or {}
    if not isinstance(by_product, dict):
        logging.warning("Field 'data' không phải object như kỳ vọng, bỏ qua.")
        return []

    found = []
    for product_id_str, entry in by_product.items():
        if not isinstance(entry, dict):
            continue
        invite = entry.get("invite") or {}
        invite_id = invite.get("id") or entry.get("invite_id")
        if not invite_id:
            continue
        status = str(invite.get("status") or entry.get("status") or "").lower()
        # Chỉ bỏ qua khi biết chắc đã đóng; is_full vẫn giữ lại để có thể
        # chat nhóm (không cần join được mới chat được).
        if status == "closed":
            continue
        found.append({
            "invite_id": invite_id,
            "host_id": invite.get("host_id"),
            "status": status,
            "start_time": invite.get("start_time"),
            "is_full": entry.get("is_full"),
            "product_id": invite.get("product_id") or product_id_str,
        })

    logging.info(f"Fetch được {len(found)} kèo đang mở sẵn từ /invite/by-products (trong "
                 f"{len(product_ids)} product được hỏi).")
    return found


def human_delay(cfg):
    """
    Sinh thời gian nghỉ giữa 2 hành động theo kiểu người thật: phần lớn là
    thao tác nhanh liên tiếp (đang rảnh, đang chat qua lại), thỉnh thoảng
    nghỉ ngắn (đang làm việc khác), hiếm khi nghỉ dài (rời app một lúc).
    Tránh việc mọi action đều cách nhau đúng 3-6s như máy chạy đều đặn.
    """
    r = random.random()
    if r < 0.55:
        # đang tương tác nhanh (đang chat qua lại, vừa join xong bấm tiếp...)
        return random.uniform(1.5, 6)
    elif r < 0.85:
        # nghỉ ngắn kiểu đang lướt app, đọc tin nhắn
        return random.uniform(8, 35)
    elif r < 0.97:
        # nghỉ vừa, như đang làm việc khác rồi quay lại
        return random.uniform(45, 180)
    else:
        # hiếm khi: rời app khá lâu rồi quay lại (đi ngủ, đi làm...)
        return random.uniform(300, 900)


def safe_json(r):
    try:
        return r.json()
    except Exception:
        return None


# --------------------------------------------------------------------------
# Worker (mỗi worker chạy trong 1 thread riêng, độc lập với các worker khác)
# --------------------------------------------------------------------------

class RoundCounter:
    """Đếm round tổng dùng chung giữa các worker, thread-safe, để log số
    round liên tục và để enforce num_rounds hữu hạn (nếu có)."""
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()

    def next(self):
        with self._lock:
            self._value += 1
            return self._value


def worker_loop(worker_id, api, cfg, users, state: SharedState, weighted_actions,
                 stop_event: threading.Event, counter: RoundCounter,
                 num_rounds, infinite):
    while not stop_event.is_set():
        round_num = counter.next()
        if not infinite and round_num > num_rounds:
            break

        action = random.choice(weighted_actions)
        user = random.choice(users)

        label = f"{round_num}" if infinite else f"{round_num}/{num_rounds}"
        logging.info(f"--- [W{worker_id}] Round {label}: action={action}, user={user.username} ---")

        try:
            if action == "create_invite":
                action_create_invite(api, cfg, user, state)
            elif action == "join_invite":
                action_join_invite(api, cfg, user, state)
            elif action == "send_chat":
                action_send_chat(api, cfg, users, state)
            elif action == "group_chat":
                action_send_group_chat(api, cfg, users, state)
            elif action == "finding_keo":
                action_finding_keo(api, cfg, user, state)
            elif action == "update_attendance":
                action_update_attendance(api, cfg, users, state)
        except Exception as e:
            # Không để 1 lỗi lẻ tẻ (network chập chờn, response lạ...) làm
            # chết cả worker đang chạy vô hạn/chạy nền dài hạn.
            logging.error(f"[W{worker_id}] Round {label} lỗi bất ngờ, bỏ qua và tiếp tục: {e}")

        if cfg.get("human_like_delay", True):
            delay = human_delay(cfg)
        else:
            delay = random.uniform(cfg["delay_seconds_min"], cfg["delay_seconds_max"])

        if delay > 60:
            logging.info(f"[W{worker_id}] (nghỉ dài {delay/60:.1f} phút, giống user rời app rồi quay lại...)")

        # stop_event.wait(delay) thay vì time.sleep(delay): vẫn nghỉ đúng
        # thời gian nhưng dừng ngay lập tức nếu có tín hiệu stop, không
        # phải đợi hết nốt khoảng nghỉ (có thể tới 15 phút) mới thoát được.
        stop_event.wait(delay)

    logging.info(f"[W{worker_id}] Dừng.")


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()

    cfg = load_config(args.config)
    setup_logging(cfg.get("log_file", "simulate_log.txt"))

    api = ApiClient(cfg["base_url"])
    users = login_all(api, cfg["accounts"])
    if cfg.get("chat_flows"):
        logging.info("[CHAT] Đã bật contextual chat flows từ config.")
    else:
        logging.info("[CHAT] Không có chat_flows -> dùng DEFAULT_CHAT_FLOWS tích hợp trong simulator.")
    logging.info(f"Đã login {len(users)}/{len(cfg['accounts'])} tài khoản test.")

    cfg["product_ids"] = fetch_product_ids(api, cfg)

    state = SharedState(cfg.get("seed_invite_ids", []))

    # Seed thêm các kèo đang mở sẵn trên site (ngoài seed_invite_ids điền tay
    # và trước khi bot tự tạo kèo mới) để join/group_chat có đối tượng ngay
    # từ round đầu.
    existing_invites = fetch_existing_invites(
        api, cfg, cfg["product_ids"], token=users[0].token if users else None
    )
    for inv in existing_invites:
        invite_id = inv.get("invite_id")
        host_id = inv.get("host_id")
        state.open_invites.add(invite_id)
        if host_id:
            state.invite_hosts[invite_id] = host_id
            state.invite_members.setdefault(invite_id, set()).add(host_id)

        raw_status = inv.get("status")
        if raw_status:
            state.invite_status[invite_id] = str(raw_status).lower()

        raw_start = inv.get("start_time")
        if raw_start:
            parsed = None
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                try:
                    parsed = datetime.strptime(str(raw_start), fmt)
                    break
                except ValueError:
                    continue
            if parsed:
                state.invite_start_time[invite_id] = parsed
    if existing_invites:
        logging.info(f"Đã seed {len(existing_invites)} kèo có sẵn vào state (open_invites hiện có "
                     f"{len(state.open_invites)} kèo tổng).")

    actions = cfg["actions_enabled"]
    weighted_actions = []
    if actions.get("create_invite"):
        weighted_actions += ["create_invite"] * 2
    if actions.get("join_invite"):
        weighted_actions += ["join_invite"] * 6
    if actions.get("send_chat"):
        weighted_actions += ["send_chat"] * 1
    if actions.get("group_chat"):
        weighted_actions += ["group_chat"] * 6
    if actions.get("finding_keo"):
        weighted_actions += ["finding_keo"] * 2
    if actions.get("update_attendance"):
        # 🆕 Trọng số cao vừa phải: đây là hành vi "mọi member tự cập nhật
        # trạng thái tới/chưa tới" theo thời gian thực, nên cần chạy đều
        # đặn hơn create_invite/finding_keo nhưng không cần dày bằng chat.
        weighted_actions += ["update_attendance"] * 5

    if not weighted_actions:
        logging.error("Không có action nào được bật trong actions_enabled.")
        sys.exit(1)

    num_rounds = cfg.get("num_rounds", 80)
    infinite = num_rounds <= 0
    if infinite:
        logging.info("num_rounds <= 0 -> chạy VÔ HẠN cho tới khi bị dừng (Ctrl+C hoặc systemd stop).")

    # Số worker chạy song song. Mặc định 10 nếu config chưa có — đủ để
    # thấy nhiều hành động chồng lấn cùng lúc (giống nhiều người dùng thật
    # online) mà không dội quá nhiều request cùng lúc vào site test.
    # Tự động không vượt quá số user đã login (không có ý nghĩa việc có
    # nhiều worker hơn số user để chọn).
    concurrent_workers = min(cfg.get("concurrent_workers", 10), len(users))
    logging.info(f"Chạy {concurrent_workers} worker song song (config concurrent_workers, "
                 f"tối đa = số user đã login = {len(users)}).")

    stop_event = threading.Event()
    counter = RoundCounter()

    def handle_stop_signal(signum, frame):
        logging.info("Nhận tín hiệu dừng (SIGTERM/SIGINT) -> báo các worker dừng lại...")
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_stop_signal)
    signal.signal(signal.SIGINT, handle_stop_signal)

    # 🆕 MAP PRESENCE: bật song song với các worker REST bên dưới, dùng
    # chung stop_event để Ctrl+C / systemd stop tắt cả 2 cùng lúc.
    map_bots = start_map_presence_bots(cfg, users, stop_event)

    threads = []
    for i in range(concurrent_workers):
        t = threading.Thread(
            target=worker_loop,
            args=(i + 1, api, cfg, users, state, weighted_actions,
                  stop_event, counter, num_rounds, infinite),
            daemon=True,
        )
        t.start()
        threads.append(t)
        # Rải thời điểm khởi động của từng worker ra 1 chút (0-3s) để
        # không có N request bắn ra CÙNG 1 mili-giây lúc mới start —
        # trông tự nhiên hơn là cả đàn cùng "tỉnh dậy" một lúc.
        time.sleep(random.uniform(0, 3))

    # Main thread chỉ đứng chờ, join có timeout để vẫn bắt được Ctrl+C
    # (join() không timeout sẽ chặn luôn signal handler trên 1 số hệ).
    try:
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=1)
                if stop_event.is_set():
                    break
    except KeyboardInterrupt:
        stop_event.set()

    for t in threads:
        t.join(timeout=30)

    for bot in map_bots:
        bot.stop_and_join(timeout=10)

    logging.info("Hoàn tất mô phỏng.")
    logging.info(f"Tổng invite đã tạo: {len(state.invited_products)}")
    logging.info(f"Tổng invite đang 'mở' trong state: {len(state.open_invites)}")


if __name__ == "__main__":
    main()