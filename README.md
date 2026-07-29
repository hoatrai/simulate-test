# Script mô phỏng hoạt động user test — app "nhau"

Gọi thẳng vào các route REST API thật (giống Flutter gọi) nên sẽ kích hoạt
luôn Phoenix socket + notification, phù hợp để test end-to-end.

Bao gồm 4 hành vi anh chọn:
- Tạo / join kèo: `nhau/v1/invite/create`, `nhau/v1/invite/join`
- Chat 1-1: `spiritwebs/v1/send-message`
- Bật/tắt finding-keo: `custom/v1/finding-keo/on|off`
- 🆕 **Hiện user trên map**: giữ WebSocket sống, join channel Phoenix
  `online_users:lobby` — xem mục "Map presence" bên dưới, cơ chế này
  KHÁC HẲN 3 hành vi trên (không phải gọi REST một lần rồi thôi).

## ⚠️ Chỉ chạy trên site TEST

Kiểm tra kỹ `base_url` trong config trỏ đúng site test/staging, không phải
production, vì script sẽ tạo dữ liệu thật (invite, chat message, vị trí...).

## Cài đặt

```bash
pip install requests websocket-client
cp config.example.json config.json
```

`websocket-client` chỉ cần thiết nếu bật `map_presence_enabled` (xem bên
dưới) — nếu không cài, script vẫn chạy bình thường các hành vi REST cũ,
chỉ mỗi phần "hiện trên map" tự tắt kèm log cảnh báo.

`config.json` đính kèm đã điền sẵn:

1. **`base_url`** = `https://spiritwebs.okinawanew.com` — domain staging lấy từ
   `$ALLOWED_STAGING_HOSTS` trong `test.php` (script seed-bot-users chỉ cho
   chạy trên host này hoặc `localhost`, KHÔNG phải production `spiritwebs.com`).
   Nếu site staging thật của anh khác domain này, sửa lại cho đúng.
2. **`accounts`** = 85 tài khoản lấy từ bảng `wp_users` anh gửi (đã loại
   `adminroot`), mật khẩu `1` theo anh xác nhận. Script tự gọi
   `POST /wp-json/jwt-auth/v1/token` để login từng người lấy JWT.
3. **`product_ids`** — để trống có chủ đích. Script giờ **tự động fetch**
   danh sách product đang publish qua WooCommerce Store API
   (`GET /wp-json/wc/store/v1/products`, endpoint public mặc định của
   WooCommerce, không cần API key) ngay khi chạy. Nếu site tắt endpoint này
   hoặc trả lỗi, script sẽ log cảnh báo và tự bỏ qua hành vi "tạo kèo mới"
   (vẫn chạy join/chat/finding-keo bình thường) — lúc đó anh điền tay
   `product_ids` vào config nếu muốn bật lại.
4. **`seed_invite_ids`** *(tuỳ chọn)* — nếu anh đã biết sẵn vài `invite_id`
   đang ở trạng thái `open` trên site, điền vào đây để có sẵn đối tượng
   join/chat ngay từ round đầu tiên.
5. **`websocket_url`** + **`map_presence_*`** — xem mục "Map presence" riêng
   bên dưới.
6. Có thể chỉnh `num_rounds`, `delay_seconds_min/max` để tăng/giảm tốc độ và
   khối lượng dữ liệu giả lập.

## Chạy

```bash
python3 simulate.py --config config.json
```

Log realtime in ra console và lưu vào file cấu hình ở `log_file`
(mặc định `simulate_log.txt`), ví dụ:

```
2026-07-27 10:00:01 [INFO] Login OK: test01 (user_id=42)
2026-07-27 10:00:12 [INFO] --- Round 1/60: action=send_chat, user=test03 ---
2026-07-27 10:00:12 [INFO] [CHAT] test03 -> test07: Alo có ai rảnh nhậu tối nay không?
2026-07-27 10:00:18 [INFO] --- Round 2/60: action=create_invite, user=test01 ---
2026-07-27 10:00:18 [INFO] [INVITE] test01 tạo kèo mới -> invite_id=118 (product=20112)
```

## 🆕 Map presence — hiện user "xung quanh" trên map thật

Đây là tính năng khác biệt hẳn so với `finding-keo`. Map bên Flutter
(`flutter_map.dart`) **không đọc DB** để vẽ marker — nó dựa vào **Phoenix
Presence**: chỉ user nào đang giữ **1 WebSocket thật sự sống**, đã join
channel `online_users:lobby`, mới được server track và phát cho các client
khác thấy qua `presence_state`/`presence_diff`. Gọi REST `finding-keo/on`
KHÔNG làm user hiện lên map này (2 cơ chế độc lập nhau).

Vì vậy script giờ có thêm `MapPresenceBot`: mỗi bot giữ 1 kết nối
WebSocket riêng, tự gửi:
- `phx_join` vào `online_users:lobby` kèm `user_id/username/avatar/lat/lng`.
- `update_presence` định kỳ (mặc định 25s) với toạ độ mới — giả lập như
  đang di chuyển nhẹ quanh khu vực.
- `heartbeat` định kỳ ở tầng transport để Phoenix không tự ngắt kết nối do
  im lặng quá lâu.

Cấu hình liên quan trong `config.json`:

```json
"websocket_url": "wss://socket.okinawanew.com/socket/websocket",
"map_presence_enabled": true,
"map_presence_worker_count": 10,
"map_presence_update_interval_seconds": 25
```

- `websocket_url` — bắt buộc nếu muốn bật, lấy từ `PHX_HOST` trong
  `docker-compose.yml`/`runtime.exs` bên Phoenix (mount ở path `/socket`).
- `map_presence_enabled: false` để tắt hẳn tính năng này (script vẫn chạy
  các hành vi REST như cũ bình thường).
- `map_presence_worker_count` — số user (trong `accounts` đã login) sẽ
  "online" trên map cùng lúc, độc lập với `concurrent_workers` (số worker
  cho create/join/chat).

Log riêng cho phần này có tiền tố `[MAP]`, ví dụ:

```
2026-07-28 02:10:03 [INFO] [MAP] Đã bật presence cho 10/85 user đã login -> họ sẽ hiện marker trên map trong lúc script còn chạy.
2026-07-28 02:10:04 [INFO] [MAP] huy join online_users:lobby (lat=10.74532, lng=106.71980)
```

Bot tự reconnect (backoff tăng dần) nếu rớt kết nối, và tự đóng socket sạch
sẽ khi script dừng (Ctrl+C / `systemctl stop`).

## Vài lưu ý về hành vi đã cài trong plugin (đã đọc code trước khi viết)

- `invite/join` server tự chặn nếu 2 người đang block nhau, hoặc kèo đã đủ
  người/đã đóng — script sẽ log "thất bại" và tự bỏ qua, không cần xử lý gì
  thêm.
- `send-message` cũng tự chặn nếu sender/receiver đang block nhau.
- `finding-keo/on` không cần JWT (route hiện đang `permission_callback =>
  __return_true` và đọc `user_id` thẳng từ body) — script gửi kèm `user_id`
  đã login được, không cần token cho action này.
- `finding-keo` off sau khi giả lập xong nếu anh không muốn user test hiện
  trên radar nữa: có thể set `finding_keo: false` ở round cuối hoặc gọi tay
  `finding-keo/off` cho từng user.
- Channel `online_users:lobby` bên Phoenix **không yêu cầu xác thực** để
  join (`user_socket.ex` chấp nhận mọi kết nối mặc định), nên bot connect
  được thẳng mà không cần JWT — chỉ cần gửi đúng `user_id` để hiện đúng
  tên/avatar khi người dùng thật bấm vào marker.

## Mở rộng thêm

Nếu sau này anh muốn thêm mô phỏng mini-game (spin / truth-or-dare / dice)
hoặc report/block, chỉ cần thêm 1 hàm `action_xxx()` mới gọi đúng route
tương ứng (`nhau/v1/game/...`, `nhau/v1/report`, `nhau/v1/block`) rồi thêm
vào danh sách `weighted_actions`, cấu trúc script đã sẵn để mở rộng.