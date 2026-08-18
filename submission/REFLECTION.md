# Reflection

**Top 5 Lakehouse Anti-Patterns:** (1) small-file accumulation từ writer commit
quá thường xuyên, (2) filter trên cột derived thay vì cột gốc nên mất pruning,
(3) coi `VACUUM`/`expire_snapshots` là dọn rác hoàn chỉnh, (4) không có
retention/orphan-sweep policy nên rác tích luỹ vô hạn, (5) schema evolution
không kiểm soát làm vỡ downstream reader.

Team mình dễ vướng nhất **anti-pattern #1 — small files** (kéo theo #3). NB2
dựng lại đúng tình huống: writer commit liên tục tạo ≥100 file nhỏ, query
chậm hẳn cho tới khi chạy `OPTIMIZE` + `Z-ORDER` (numFiles giảm >10 lần). NB6
lượng hoá thành tiền: trong hoá đơn managed-compaction $990/mo, phần tính
theo **số object** chiếm 24%, driven bởi tần suất commit chứ không phải data
volume. Đây là bug âm thầm: pipeline vẫn chạy đúng, dashboard vẫn xanh, chỉ
chi phí và latency âm thầm leo thang tới khi ai đó đo `numFiles`.

Bài học: theo dõi `numFiles`/table như metric observability thường trực, và
chỉnh trigger interval của writer thay vì compact bị động sau khi phát sinh
vấn đề — sửa chu kỳ ghi rẻ hơn nhiều so với thuê người dọn dẹp sau đó.
