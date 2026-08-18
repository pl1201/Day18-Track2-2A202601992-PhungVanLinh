# Architecture Brief — Lakehouse Observability cho LLM ở quy mô 1B request/ngày

**Topic A.** Team foundation-model API, 1B request/ngày, ~5 KB/request →
5 TB/ngày raw. Tác giả: Phùng Văn Linh.

---

## 1. Problem statement

Một hệ thống LLM API log lại mọi request/response để phục vụ observability
về cost & latency. Quy mô: **1B request/ngày, 5 KB/request → 5 TB/ngày
raw**. Yêu cầu: (1) dashboard cost & latency theo tenant, refresh ≤ 5 phút;
(2) giữ full prompt/response 7 ngày để phục vụ incident review, sau đó
**chỉ giữ aggregates** cho phần còn lại của năm (lý do pháp lý + chi phí
lưu trữ); (3) PII (user ID, prompt có thể chứa PII) không được phép đọc
được ở dạng thô trước khi redact; (4) tổng chi phí storage ≤ **$5K/tháng**.
Cái khó không nằm ở từng yêu cầu riêng lẻ, mà ở chỗ (2) và (3) mâu thuẫn
nhau (cần dữ liệu thô để debug incident, nhưng không ai được đọc nó ở dạng
chưa redact), còn (1) và (4) cũng mâu thuẫn nhau (freshness 5 phút ở quy mô
này thường đòi hỏi một cluster chạy liên tục — chiếm phần lớn ngân sách).

---

## 2. Architecture diagram

```
Producers (API gateway, 1B req/ngày, trung bình 5 KB)
        │
        ▼
   Kafka (retention 7 ngày, partition theo tenant_id)
        │  micro-batch, trigger 1 phút
        ▼
┌─────────────────────────────────────────────────────────┐
│ TOKENIZE (HMAC token vault) — prompt/response → token mờ │
│ PII thô không bao giờ chạm disk ở dạng chưa mã hoá       │
└─────────────────────────────────────────────────────────┘
        │
        ▼
  BRONZE  (Iceberg, partition theo day(ts), cluster theo tenant_id)
  full record đã tokenize · retention: 7 ngày hot
        │
        │  job chạy đêm, tại mốc ngày thứ 7:
        │  overwrite partition → bỏ prompt/response đã tokenize,
        │  chỉ giữ field có cấu trúc (tokens, latency, cost, status)
        ▼
  SILVER  (Iceberg, partition theo day(ts))
  đã dedup, đã redact còn metadata · retention: 1 năm, có tiering
        │
        │  streaming agg, tumbling window 5 phút
        ▼
  GOLD    (Iceberg, partition theo day(ts), nhỏ)
  p50/p95 latency, cost_usd, error_rate THEO tenant × model × khung 5 phút
        │
        ▼
  Trino / DuckDB  ──▶  Dashboard (Grafana), refresh 5 phút
        │
        ▼
  Đường incident replay: analyst chỉ query BRONZE trong cửa sổ 7 ngày,
  mọi lần đọc được ghi vào audit table (ai, tenant nào, khi nào) — token
  vault chỉ de-tokenize khi có mã incident ticket hợp lệ.
```

Một diagram duy nhất, ba tầng, hai lifecycle chạy độc lập (TTL 7 ngày cho
raw, TTL 1 năm cho metadata) trên cùng một layout partition theo ngày.

---

## 3. Các quyết định chính (6 quyết định, kèm alternative đã loại)

**1. Table format: Iceberg.**
Chọn Iceberg thay vì Delta và Hudi. Loại **Delta** vì hệ thống này cần nội
dung partition Bronze→Silver *đổi shape* tại mốc ngày 7 (full record →
chỉ metadata) trong khi query vào các partition Silver cũ vẫn phải chạy
được — hidden partitioning và partition-spec evolution của Iceberg cho
phép partition theo day(ts) giữ nguyên ổn định trong khi schema của từng
"thế hệ" partition tiến hoá mà không cần rewrite toàn bộ table. Với Delta
(Hive-style partitioning theo thư mục), đổi partition strategy sau này sẽ
buộc phải rewrite lại toàn bộ lịch sử table. Loại **Hudi**: câu chuyện
incremental/CDC mạnh của Hudi không cần thiết ở đây (đây là dữ liệu
event append-mostly, không phải upsert từ nguồn mutable), và team chưa có
kinh nghiệm vận hành Hudi — rủi ro vận hành lớn hơn lợi ích biên.

**2. Xử lý PII: tokenize ngay lúc ingest vào một vault riêng, không redact
tại chỗ.**
Chọn tokenization dạng format-preserving (HMAC + vault tra cứu) thực hiện
ngay trong micro-batch Kafka→Bronze, trước khi byte nào chạm tới Bronze.
Loại **redact một chiều** (hash/mask text prompt trước khi lưu) vì yêu cầu
(2) cần dữ liệu incident có thể *replay* trong 7 ngày — redact một chiều
thì không thể hoàn tác để debug hợp lệ. Loại **lưu thô, chặn bằng ACL ở
table**: ACL cấp table không ngăn được việc một sự cố cấu hình quyền sai
làm lộ PII thô (đây chính là failure mode mà yêu cầu (3) sinh ra để ngăn)
— tokenize nghĩa là kể cả khi có lỗi cấp quyền toàn table, thứ bị lộ cũng
chỉ là token mờ, không phải PII.

**3. Cơ chế retention: overwrite-in-place theo partition, không tạo bảng
riêng rồi copy.**
Job chạy tại mốc ngày 7 làm `INSERT OVERWRITE` lên partition day(ts) với
schema đã redact, giữ nguyên lineage snapshot/partition của Iceberg. Loại
**tách riêng bảng raw và bảng metadata rồi copy job**: nhân đôi storage
trong lúc overlap 7 ngày và sinh thêm bài toán reconciliation (đã copy
xong hẳn trước khi xoá nguồn chưa?). Cách overwrite khiến câu hỏi
"partition N đã redact chưa" trả lời được chỉ bằng cách đọc schema của
partition đó, không cần đối chiếu chéo hai bảng.

**4. Nhịp ingest: micro-batch 1 phút qua Spark Structured Streaming, không
ghi theo từng request.**
Ở quy mô 1B req/ngày (~11.6K req/s trung bình, có burst), ghi theo từng
request sẽ sinh ra hàng trăm nghìn file Parquet nhỏ/ngày — đúng là
anti-pattern small-file mà NB2/NB6 đã đo được: ở mức file count đó, phần
chi phí tính theo số object trong hoá đơn compaction (24% trong ví dụ
$990/tháng của lab) sẽ chiếm ưu thế ở quy mô này. Loại **ghi theo từng
request**: file count không kham nổi. Loại **batch 15 phút**: không đạt
yêu cầu freshness 5 phút khi cộng thêm độ trễ xử lý.

**5. Materialize Gold: streaming job pre-aggregate, không scan on-demand.**
Gold là aggregate tumbling-window 5 phút (p50/p95, cost, error_rate theo
tenant × model), tính liên tục và đủ nhỏ để query rẻ. Loại **Trino scan
Silver on-demand mỗi lần dashboard refresh**: dù ở dạng chỉ-metadata, giữ
5 TB/ngày, một query aggregate live mỗi 5 phút với nhiều người xem
dashboard cùng lúc sẽ cần một compute pool lớn chạy liên tục — đây là rủi
ro ngân sách lớn nhất, và pre-aggregate biến chi phí từ O(dung lượng dữ
liệu) thành O(số khung 5 phút).

**6. Tiering storage: S3 Standard (Bronze 7 ngày) → S3 IA (Silver ngày
8–90) → S3 Glacier Instant Retrieval (Silver ngày 91–365).**
Loại **để tất cả trên Standard**: vượt cap $5K (xem mục 5). Loại
**Glacier Flexible/Deep Archive cho tầng >90 ngày**: độ trễ retrieval
(hàng giờ) không tương thích với query "best-effort" của analyst vào
aggregate dữ liệu 1 năm tuổi — vẫn cần trả kết quả trong ngày dù chậm.

---

## 4. Failure modes (4, có ít nhất 1 gắn với concept Day 18)

1. **Streaming job crash giữa chừng một micro-batch lúc 3 giờ sáng.**
   *Detection:* commit của Iceberg là atomic — một batch bị crash thì hoặc
   commit trọn vẹn hoặc không, không có state partition dở dang. Nhưng nếu
   crash-loop (restart rồi fail lặp lại) sẽ gây ra small-file storm y hệt
   những gì NB6 đo được (`numFiles` tăng trong khi data volume không đổi)
   — cần alert theo *tốc độ tăng* `numFiles` mỗi table, không chỉ số tuyệt
   đối. *Rollback:* checkpoint của Structured Streaming khiến restart
   idempotent (replay từ Kafka offset commit gần nhất); job `OPTIMIZE`
   theo lịch sẽ dọn sạch nợ small-file phát sinh trong khoảng crash-loop.
   **Liên hệ Day 18:** đây chính là phát hiện "sửa chu kỳ ghi rẻ hơn dọn
   dẹp" của NB6, áp dụng cho tình huống crash thay vì steady-state.

2. **Một team producer đẩy một breaking schema change (đổi tên/kiểu field)
   vào payload Kafka.**
   *Detection:* Bronze write dùng schema enforcement nghiêm ngặt (không
   bật `schema_mode="merge"` ở hot path) — micro-batch fail rõ ràng ngay
   trên partition đó thay vì âm thầm làm hỏng Gold aggregate. **Liên hệ
   Day 18:** đây là cơ chế schema enforcement của NB1, cố tình *không* bật
   merge mode ở đây vì một field không kiểm soát là một sự cố production,
   không phải một feature để tự động chấp nhận.
   *Rollback:* batch fail bị đưa vào Kafka topic quarantine; on-call review
   rồi hoặc patch producer, hoặc duyệt thủ công migrate sang
   `schema_mode="merge"` sau khi hiểu rõ field mới.

3. **Token vault sập trong lúc đang ingest.**
   *Detection:* bước tokenize là một circuit breaker trong đường ingest —
   nếu vault không reachable, micro-batch **không** fallback ghi text thô,
   mà đẩy vào dead-letter topic. *Rollback:* khi vault hồi phục, replay lại
   dead-letter topic qua đúng đường tokenize→Bronze. Query time-travel vào
   Bronze cho khung thời gian sự cố để xác nhận không có row nào lọt vào
   mà chưa tokenize trước khi đóng incident — đây chính là pattern
   `history()`/`versionAsOf` của NB3, dùng như công cụ verify compliance
   thay vì công cụ rollback.

4. **Lịch `OPTIMIZE`/compaction chạy quá dày, chi phí compute vọt lên.**
   *Detection:* alert FinOps riêng cho dòng chi phí job compaction (không
   phải tổng chi tiêu), vì NB6 đã cho thấy dòng này bị driven bởi file
   count/tần suất, không phải data volume — chi phí vọt lên nghĩa là lịch
   bị sai, không phải traffic tăng.
   *Rollback:* giảm về lịch `OPTIMIZE` đã biết là ổn gần nhất; vì
   compaction idempotent và không phá dữ liệu, không cần rollback dữ liệu,
   chỉ cần rollback lịch chạy.

---

## 5. Ước lượng chi phí (back-of-envelope, $/tháng)

Giả định: S3 Standard $0.023/GB-tháng, S3 IA $0.0125/GB-tháng, S3 Glacier
Instant Retrieval $0.004/GB-tháng (theo bậc giá công khai tham khảo).

**Storage:**
- Bronze (Standard, cửa sổ trượt 7 ngày): `5 TB/ngày × 7 ngày = 35 TB` →
  `35,000 GB × $0.023 = $805/tháng`
- Silver chỉ-metadata (~200 B/record so với 5 KB raw sau redact):
  `1B req/ngày × 200 B ≈ 200 GB/ngày`
  - Ngày 8–90 (IA, 82 ngày): `82 × 200 GB = 16.4 TB → 16,400 × $0.0125 =
    $205/tháng`
  - Ngày 91–365 (Glacier IR, 275 ngày): `275 × 200 GB = 55 TB → 55,000 ×
    $0.004 = $220/tháng`
- Gold (rất nhỏ — bucket tenant × model × 5 phút, ~1 năm): không đáng kể,
  < $10/tháng
- **Tổng storage: ≈ $1,240/tháng**

**Compute:**
- Streaming ingestion (Spark Structured Streaming, đủ sức cho 11.6K req/s
  trung bình / có burst, ~6–8 node cỡ vừa): **≈ $1,500/tháng**
- Job compaction/OPTIMIZE (chạy theo lịch, không always-on): **≈
  $500/tháng**
- Job streaming aggregate Gold (nhỏ — cardinality output bị chặn):
  **≈ $400/tháng**
- Tầng phục vụ Trino/DuckDB cho dashboard + query incident-replay:
  **≈ $800/tháng**
- **Tổng compute: ≈ $3,200/tháng**

**Tổng: ≈ $4,440/tháng**, dưới cap $5K, còn dư ~$560/tháng cho catalog
hosting và token vault. Con số này cho thấy tiering *metadata* (không phải
raw data — vốn đã biến mất sau 7 ngày) mới là thứ làm cho yêu cầu retention
1 năm khả thi về chi phí — cửa sổ Standard 7 ngày của Bronze là dòng
storage lớn nhất dù có retention ngắn nhất.

---

## 6. MVP tuần đầu

Không phải toàn bộ pipeline multi-tenant — chỉ slice nhỏ nhất chứng minh
hai cơ chế khó nhất của kiến trúc thật sự hoạt động:

1. Một tenant giả lập, Kafka topic → Spark Structured Streaming (trigger
   1 phút) → Bronze table **Iceberg**, với một hàm tokenize HMAC thật (dù
   đơn giản) thay thế prompt/response thô trước khi ghi.
2. Một Gold aggregate table (p50/p95 latency, cost_usd, error_rate), cập
   nhật mỗi 5 phút, query được qua DuckDB cho một panel dashboard.
3. Chứng minh 3 điều end-to-end: (a) schema enforcement nghiêm ngặt từ
   chối một record sai định dạng thay vì làm hỏng Gold, (b) tokenization
   chỉ đảo ngược được qua vault — Bronze trên disk không chứa PII thô,
   (c) freshness dashboard ≤ 5 phút kể từ lúc ingest dưới tải burst giả
   lập.

Nếu cả 3 điều trên đúng với một tenant ở quy mô nhỏ, phiên bản
multi-tenant/1B-req-ngày chỉ là bài toán scale (thêm Kafka partition,
thêm Spark executor, cluster theo tenant) chứ không phải thiết kế lại.
